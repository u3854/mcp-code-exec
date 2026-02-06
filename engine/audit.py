import logging
import os
import sys
from typing import Mapping
from engine.config import config
from engine.context import active_session_id, pip_is_active

log = logging.getLogger(__name__)

# These are the only variables allowed to exist in the environment
ALLOWED_ENV = {
    "PATH", 
    "HOME", 
    "PYTHONUNBUFFERED", 
    "PYTHONDONTWRITEBYTECODE", 
    "LC_CTYPE"
}

# --- ENVIRONMENT SCRUBBING & PROXYING ---
# We physically delete everything else from memory
for key in list(os.environ.keys()):
    if key not in ALLOWED_ENV:
        del os.environ[key]

# proxy for os.environ
class ReadOnlyEnviron(Mapping):
    """A read-only proxy for the OS environment."""
    def __init__(self, data):
        self._data = data
    def __getitem__(self, key):
        return self._data[key]
    def __len__(self):
        return len(self._data)
    def __iter__(self):
        return iter(self._data)
    def __setitem__(self, key, value):
        raise PermissionError(f"Modification of environment variable '{key}' is forbidden.")
    def __delitem__(self, key):
        raise PermissionError(f"Deletion of environment variable '{key}' is forbidden.")
    def get(self, key, default=None):
        return self._data.get(key, default)
    def __getattr__(self, name):
        # Forward other calls (like .copy()) to the underlying dict
        return getattr(self._data, name)

# Swap the real environ with our read-only proxy
os.environ = ReadOnlyEnviron(os.environ.copy())

def audit_hook(event, args):
    # print(f"audit_hook EVENT: {event}")
    # ---------------------------------------------------------
    # 1. BLOCK ALL SHELL & PROCESS MORPHING
    # ---------------------------------------------------------
    # Blocks os.system, os.spawn, os.exec, and subprocess
    if event in ("os.system", "os.exec", "os.spawn", "os.posix_spawn", "subprocess.Popen"):
        
        # Determine where the command arguments are based on the event
        # For Popen and posix_spawn, args[1] is usually the argv list
        cmd_args = args[1] if "subprocess" in event or "posix_spawn" in event else args[0]
        
        if not cmd_args:
            raise PermissionError(f"Unauthorized execution attempt: {event}")

        # Strict Identity Check: Is this the REAL python binary?
        requested_exe = os.path.realpath(str(cmd_args[0] if isinstance(cmd_args, list) else cmd_args))
        actual_python = os.path.realpath(sys.executable)

        # Whitelist 'python -m pip' only
        is_pip = False
        if requested_exe == actual_python and len(cmd_args) >= 3:
            if cmd_args[1] == "-m" and cmd_args[2] == "pip":
                is_pip = True
        
        if not is_pip:
            raise PermissionError(f"Execution blocked. '{requested_exe}' is not authorized.")

    # ---------------------------------------------------------
    # 2. FILE SYSTEM ACCESS CONTROL
    # ---------------------------------------------------------
    if event in ("open", "os.listdir", "os.scandir"):
        path = args[0]
        # Ignore file descriptors (integers)
        if isinstance(path, int): 
            return
        
        try:
            # Resolve symlinks and '..' before checking
            target_path = os.path.realpath(path)
        except Exception:
            raise PermissionError("Could not resolve file path safely.")

        # get active session id
        sid = active_session_id.get()

        # Dynamic read and write Whitelists
        # Everyone can read system libs and their own session folder
        read_list = [os.path.realpath(d) for d in config.allowed_read_paths]
        write_list = []

        if sid:
            # Resolve the session-specific subfolder
            session_dir = os.path.realpath(os.path.join(config.sandbox_dir, sid))
            read_list.append(session_dir)
            write_list.append(session_dir)
            
        # If PIP is active, unlock the library directory
        if pip_is_active.get():
            write_list.append(config.user_lib_dir)

        # validation helper
        def is_subpath(p, base_list):
            for base in base_list:
                # Check if p is exactly the directory OR is inside it
                if p == base or p.startswith(base + os.sep):
                    return True
            return False
        
        # mode enforcement
        mode = args[1] if (event == "open" and len(args) > 1) else 'r'
        is_write = any(m in str(mode) for m in ('w', 'a', 'x', '+'))


        if is_write:
            if not is_subpath(target_path, write_list):
                # Descriptive error for debugging
                raise PermissionError(
                    f"Write denied to {target_path}. "
                    f"Session: {sid}, Pip: {pip_is_active.get()}"
                )
        else:
            if not is_subpath(target_path, read_list):
                # Check for essential system loaders (/lib, /lib64, etc)
                if not target_path.startswith(("/lib", "/usr/lib")):
                    raise PermissionError(f"Read denied: {target_path}")


def _initialize_worker_environment():
    """Sets up the sandbox and pre-loads libraries."""
    
    # We import the required modules here. Python reads the file NOW.
    # Because the hook isn't active yet, this is allowed.
    try:
        import engine.executor  # noqa: F401
        import engine.session  # noqa: F401
    except ImportError as e:
        log.error(f"Failed to preload libraries: {e}")
        raise

    # 2. Warm up libraries (Pandas, Numpy, etc.)
    for lib in config.preload_libraries:
        try:
            __import__(lib)
        except ImportError:
            pass

    # 3. Enforce Sandbox (Move to directory)
    if not os.path.exists(config.sandbox_dir):
        os.makedirs(config.sandbox_dir, exist_ok=True)
    os.chdir(config.sandbox_dir)
    
    # 4. LOCK THE DOOR (Activate the Hook)
    # From this point on, NO new files outside SANDBOX_DIR can be opened.
    sys.addaudithook(audit_hook)