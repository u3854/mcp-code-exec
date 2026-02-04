from dataclasses import dataclass
import logging
import os
import sys
from typing import Mapping

log = logging.getLogger(__name__)

# --- CONFIGURATION ---
# DEFINE PATHS (Resolved to absolute, real paths)
BASE_DIR = os.path.realpath("/app")
SANDBOX_DIR = os.path.join(BASE_DIR, "sandbox")
# Path where 'pip install --user' goes
USER_LIB_DIR = os.path.realpath(os.path.join(os.environ.get("HOME", "/home/mcp-user"), ".local"))
# Core Python Standard Libraries
SYSTEM_LIB_DIR = os.path.realpath(f"/usr/local/lib/python{sys.version_info.major}.{sys.version_info.minor}")

ALLOWED_READ_PATHS = [SANDBOX_DIR, USER_LIB_DIR, SYSTEM_LIB_DIR, "/usr/lib", "/etc/ssl"]
ALLOWED_WRITE_PATHS = [SANDBOX_DIR, USER_LIB_DIR]

# Preload libraries into the workers
PRELOAD_LIBRARIES = ["math", "numpy", "sympy", "pandas"]


# --- Internal object to store secret keys ---
# Grab your API key and any other app-specific vars before wiping the env
@dataclass(frozen=True)
class InternalConfig:
    google_api_key: str | None

    @classmethod
    def from_env(cls) -> "InternalConfig":
        return cls(
            google_api_key=os.environ.get("GOOGLE_API_KEY"),
        )
    
internal_config = InternalConfig.from_env()

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

        # Check if we are attempting to write
        mode = args[1] if event == "open" and len(args) > 1 else 'r'
        is_write = any(m in str(mode) for m in ('w', 'a', 'x', '+'))

        # Validation Logic
        def is_subpath(p, base):
            return p.startswith(base + os.sep) or p == base

        if is_write:
            if not any(is_subpath(target_path, d) for d in ALLOWED_WRITE_PATHS):
                raise PermissionError(f"Write access denied: {target_path}")
        else:
            if not any(is_subpath(target_path, d) for d in ALLOWED_READ_PATHS):
                # Allow access to essential dynamic loaders
                if not target_path.startswith(("/lib", "/usr/lib")):
                    raise PermissionError(f"Read access denied: {target_path}")
    # --- ENVIRONMENT CONTROL ---
    # Triggered by os.getenv() and accessing os.environ
    if event == "os.getenv":
        env_var_name = args[0]
        if env_var_name not in ALLOWED_ENV:
            # We return None or empty rather than crashing to avoid breaking 3rd party libs,
            # but you can raise PermissionError if you want total lockdown.
            raise PermissionError(f"Access to environment variable '{env_var_name}' is restricted.")


def _initialize_worker_environment():
    """Sets up the sandbox and pre-loads libraries."""
    
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