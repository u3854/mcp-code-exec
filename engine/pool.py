import asyncio
import logging
import multiprocessing
import os
import signal
from multiprocessing.connection import Connection
import sys
from typing import Any, Callable, Optional, Tuple
import uuid


multiprocessing.set_start_method("spawn", force=True)
log = logging.getLogger(__name__)

# --- CONFIGURATION ---
BASE_DIR = os.getcwd()
SANDBOX_DIR = os.path.join(BASE_DIR, "sandbox")
ALLOWED_PATHS = [SANDBOX_DIR]
PRELOAD_LIBRARIES = ["math", "numpy", "sympy", "pandas"]
PROCESSPOOL_SIZE = 4

def audit_hook(event, args):
    """
    Intercepts low-level Python events.
    We care about 'open' and 'os.listdir' events.
    """
    # Block subprocess usage completely
    if event == "subprocess.Popen":
        raise PermissionError("Subprocess execution is restricted.")
    
    if event == "open" or event == "os.listdir":
        path = args[0]
        if isinstance(path, int): # If it's a file descriptor, it's usually safe/internal
            return
            
        # Resolve absolute path to check against allowed list
        abs_path = os.path.abspath(path)
        
        # Check if the path starts with our sandbox directory
        is_allowed = False
        for safe_path in ALLOWED_PATHS:
            # Ensure safe_path ends with a separator to prevent prefix matching (e.g. /sandbox vs /sandbox_secret)
            safe_path_guarded = os.path.join(safe_path, "") 
            
            # Check if it matches exactly OR is a subpath
            if abs_path == safe_path or abs_path.startswith(safe_path_guarded):
                is_allowed = True
                break
        
        # Special exceptions (LLM needs to import libraries)
        if abs_path.startswith("/usr/local/lib") or abs_path.startswith("/usr/lib"):
             is_allowed = True

        if not is_allowed:
            raise PermissionError(f"Access denied: {path} is outside the sandbox.")


def _initialize_worker_environment():
    """Sets up the sandbox and pre-loads libraries."""
    
    # 1. PRE-LOAD YOUR CODE (The "Friends" List)
    # We import the executor module here. Python reads the file NOW.
    # Because the hook isn't active yet, this is allowed.
    try:
        import engine.executor  # noqa: F401
    except ImportError as e:
        log.error(f"Failed to preload executor: {e}")
        raise

    # 2. Warm up libraries (Pandas, Numpy, etc.)
    for lib in PRELOAD_LIBRARIES:
        try:
            __import__(lib)
        except ImportError:
            pass

    # 3. Enforce Sandbox (Move to directory)
    if not os.path.exists(SANDBOX_DIR):
        os.makedirs(SANDBOX_DIR, exist_ok=True)
    os.chdir(SANDBOX_DIR)
    
    # 4. LOCK THE DOOR (Activate the Hook)
    # From this point on, NO new files outside SANDBOX_DIR can be opened.
    sys.addaudithook(audit_hook)
    

def worker_main(task_conn: Connection, result_conn: Connection):
    """The persistent process loop."""
    task_conn.close()
    signal.signal(signal.SIGINT, signal.SIG_IGN) # Ignore Ctrl+C

    # --- INITIALIZATION ---
    try:
        _initialize_worker_environment()
    except Exception:
        log.exception("Worker init failed during startup")
        return # Die if we can't sandbox

    while True:
        try:
            # Wait for a task
            fn, args, kwargs, task_id = result_conn.recv()
        except EOFError:
            break

        try:
            # Execute the function passed from the main process
            # This 'fn' will be our 'execute_user_code' function
            output = fn(*args, **kwargs)
            result_conn.send((task_id, "ok", output))
        except Exception as exc:
            # Capture full traceback or error string
            result_conn.send((task_id, "err", str(exc)))



class Worker:
    def __init__(self, wid: int):
        self.wid = wid
        self.lock = asyncio.Lock()
        self._setup()

    def _setup(self):
        self.parent_conn, self.child_conn = multiprocessing.Pipe()
        # Start the process
        self.process = multiprocessing.Process(
            target=worker_main,
            args=(self.parent_conn, self.child_conn),
            daemon=True
        )
        self.process.start()
        self.child_conn.close()  # Parent keeps only the parent side
        log.info(
            f"WORKER STARTED    | Parent PID={os.getpid()} | "
            f"Worker ID={self.wid} | Process PID={self.process.pid}"
        )

    def is_alive(self):
        return self.process.is_alive()

    def is_busy(self):
        """Returns True if the lock is currently held (worker is working)."""
        return self.lock.locked()

    def kill(self):
        if self.is_alive():
            pid_to_terminate = self.process.pid
            self.process.terminate()
            self.process.join(timeout=2.0)
            # If still alive after 2 seconds, force kill
            if self.process.is_alive():
                os.kill(pid_to_terminate, signal.SIGKILL) 
                self.process.join()
            self.parent_conn.close() 
            log.info(
                f"WORKER TERMINATED | Parent PID={os.getpid()} | "
                f"Worker ID={self.wid} | Process PID={pid_to_terminate}"
            )

    def restart(self):
        self.kill()
        # Re-initialize (creates new pipe, new process)
        self._setup()


class WorkerPool:
    def __init__(self, size: int = 4, max_backlog: int = 50):
        self._shutting_down = False
        self.size = size
        log.info(f"Initializing worker pool of {size} processes")

        self.bouncer = asyncio.BoundedSemaphore(max_backlog)
        self.workers = [Worker(i) for i in range(size)]
        self.available_workers = asyncio.Queue()

        for w in self.workers:
            self.available_workers.put_nowait(w)
            
        self._print_status("INITIALIZED")

    def _print_status(self, reason: str):
        active_count = sum(1 for w in self.workers if w.is_alive())
        idle_count = self.available_workers.qsize()
        log.info(
            f"POOL STATUS ({reason}): "
            f"Total: {self.size} | Active Proc: {active_count} | "
            f"Idle/Queue: {idle_count}"
        )

    async def run(self, fn: Callable, execution_timeout: float = 5.0, queue_timeout: float = 30.0, bouncer_timeout: float = 1.0, *args, **kwargs) -> Tuple[int, Any]:
        """Executes the function in a sub process.
        
        Args:
            fn (Callable): Function to execute
            execution_timeout: Max time allowed for the function to run (triggers restart if exceeded).
            queue_timeout: Max time allowed to wait for a worker (does NOT trigger restart).
            bouncer_timeout: Max time allowed to wait for backlog access token (will get server busy error if there's too much backlog).

        Returns:
            Tuple[int, Any]: worker ID, function output
        """
        try:
            # prevents task queue from growing indefinitely
            await asyncio.wait_for(self.bouncer.acquire(), timeout=bouncer_timeout)
        except asyncio.TimeoutError:
            log.error("System overloaded: Max backlog limit reached.")
            raise RuntimeError("Server overloaded: Please try again later.")
        
        try:
            return await self._execute_task(fn, execution_timeout, queue_timeout, *args, **kwargs)
        finally:
            self.bouncer.release()


    async def _execute_task(self, fn, exec_timeout, q_timeout, *args, **kwargs):
        # --- PHASE 1: ACQUIRE WORKER ---
        try:
            # log.info("WAITING FOR AVAILABLE WORKER")
            w: Worker = await asyncio.wait_for(self.available_workers.get(), timeout=q_timeout)
        except asyncio.TimeoutError:
            log.error(f"Queue Timeout: Waited {q_timeout}s but no workers became free.")
            raise RuntimeError("Server busy: Could not acquire worker in time.")

        # log.info(f"WORKER (wid={w.wid}) READY")
          

        # --- PHASE 2: EXECUTE ---
        # The 'execution_timeout' ONLY applies to this block.
        async with w.lock:
            try:
                task_id = str(uuid.uuid4())
                log.info(f"Sending '{fn.__name__}' execution task to (uvicorn_pid: {os.getpid()}, worker_id: {w.wid})")    # testing
                # Send the task
                w.parent_conn.send((fn, args, kwargs, task_id))

                # Wait for result strictly with execution_timeout
                result = await asyncio.wait_for(
                    asyncio.to_thread(w.parent_conn.recv),
                    timeout=exec_timeout,
                )
                
                got_id, status, output = result
                
                if got_id != task_id:
                    log.warning(f"ID MISMATCH on Worker {w.wid}. Restarting...")
                    w.restart() 
                    raise RuntimeError("Task ID mismatch! Restarted worker.")

                if status == "ok":
                    return w.wid, output
                else:
                    raise RuntimeError(output)

            # --- ERROR HANDLING ---
            
            except asyncio.TimeoutError:
                # 1. ACTUAL EXECUTION TIMEOUT
                # The worker took too long to calculate. It might be stuck.
                log.warning(f"EXECUTION TIMEOUT ({exec_timeout}s) on Worker {w.wid}. Restarting...")
                w.restart()
                self._print_status(f"RESTART: EXEC TIMEOUT WID {w.wid}")
                raise TimeoutError(f"Task execution timed out after {exec_timeout}s")

            except asyncio.CancelledError:
                # 2. CLIENT DISCONNECT / OUTER TIMEOUT
                # The user closed the connection, but the worker is still crunching data.
                # We MUST restart it, otherwise it stays busy with a dead task.
                log.warning(f"Task Cancelled on Worker {w.wid}. Restarting to clear state...")
                w.restart()
                raise 

            except (EOFError, BrokenPipeError, ConnectionResetError, OSError) as e:
                if self._shutting_down:
                    log.info(f"Worker {w.wid} killed during shutdown. Not restarting.")
                    raise RuntimeError("System shutting down")
                # 3. WORKER CRASH
                log.warning(f"[Worker Crash] Worker {w.wid} died. Restarting.")
                w.restart()
                self._print_status(f"RESTART: CRASH WID {w.wid}")
                raise RuntimeError(f"Worker {w.wid} died unexpectedly") from e
            
            finally:
                # --- PHASE 3: RETURN WORKER ---
                # We only put the worker back if we didn't crash explicitly before restart logic
                # (The restart methods above handle internal state, so 'w' is safe to return)
                if w.is_alive():
                    self.available_workers.put_nowait(w)
                else:
                    # edge case: OOM
                    log.critical(f"Worker {w.wid} is dead and could not be restarted. Removing from pool.")

    def shutdown(self):
        self._shutting_down = True
        log.info("\nSHUTTING DOWN POOL...")
        for w in self.workers:
            w.kill()
        self._print_status("SHUTDOWN COMPLETE")

# ----------------------------------------------------------------

worker_pool: Optional[WorkerPool] = None

def start_worker_pool(max_workers: int=4, max_backlog: int=50):
    """Initializes a pool of sub processes"""
    if max_workers < 1:
        log.warning("Worker pool size < 1. Pool will NOT start.")
        return
    global worker_pool
    if worker_pool is None:
        worker_pool = WorkerPool(size=max_workers, max_backlog=max_workers)

def stop_worker_pool():
    global worker_pool
    if worker_pool:
        log.info("Shutting down worker pool...")
        worker_pool.shutdown()
        worker_pool = None
        log.info("Worker pool shut down.")

restart_lock = asyncio.Lock()

async def safe_restart():
    global worker_pool
    async with restart_lock:
        if worker_pool:
            worker_pool.shutdown()
            worker_pool = None
        # Optional: Force GC here to clear parent memory
        import gc
        gc.collect()
        # Start new
        start_worker_pool(PROCESSPOOL_SIZE)


def get_worker_pool():
    global worker_pool
    return worker_pool