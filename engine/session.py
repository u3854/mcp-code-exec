import asyncio
import logging
import os
import shutil
import time

from engine.config import config
from engine.context import active_session_id
# Use type hinting only to avoid circular imports at runtime
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from engine.pool import WorkerPool

log = logging.getLogger(__name__)

def cleanup_session_task(session_id: str, persistent_namespace: dict = None):
    """Worker-side task to wipe session data."""
    token = active_session_id.set(session_id)
    try:
        # 1. Clear the persistent Python variables
        if persistent_namespace is not None:
            persistent_namespace.clear()
            # RE-INITIALIZE: exec() needs these to function correctly for the next run
            persistent_namespace.update({
                "__name__": "__main__",
                "__builtins__": __builtins__
            })
        
        # 2. Delete the physical sandbox
        sandbox_path = os.path.join(config.sandbox_dir, session_id)
        if os.path.exists(sandbox_path):
            try:
                shutil.rmtree(sandbox_path)
            except Exception as e:
                log.error(f"Worker failed to delete {sandbox_path}: {e}")
                return f"Error: {str(e)}"
                
        return f"Session {session_id} cleaned up."
    finally:
        active_session_id.reset(token)


async def session_reaper(pool: "WorkerPool", idle_timeout: float):
    """Background task to clean up old sessions every X seconds."""
    log.info(f"Session Reaper started (Timeout: {idle_timeout}s)")
    try:
        while not pool._shutting_down:
            await asyncio.sleep(60) # Run check every minute
            now = time.time()
            to_delete = []

            # Identify expired sessions
            # Use list() to avoid "dictionary changed size during iteration" errors
            for sid, last_active in list(pool.session_activity.items()):
                if now - last_active > idle_timeout:
                    to_delete.append(sid)

            for sid in to_delete:
                log.info(f"Reaping inactive session: {sid}")
                
                # 1. Dispatch cleanup to the specific worker
                # This ensures the session's sticky worker clears its RAM
                try:
                    await pool.run(cleanup_session_task, sid, execution_timeout=10.0)
                except Exception as e:
                    log.error(f"Failed to execute cleanup for session {sid}: {e}")
                finally:
                    # 2. Always remove from Main Process registry
                    pool.session_to_wid.pop(sid, None)
                    pool.session_activity.pop(sid, None)
    except asyncio.CancelledError:
        log.info("Session Reaper task cancelled.")