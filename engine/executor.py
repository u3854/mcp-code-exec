import ast
import io
from contextlib import redirect_stdout, redirect_stderr
import subprocess
import sys
import logging
import site
import importlib

log = logging.getLogger(__name__)

def execute_user_code(code: str, packages_to_install: list[str] = None) -> dict:
    
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    try:
        # --- INSTALLATION PHASE ---
        if packages_to_install:
            # print(f"[System] Installing packages: {', '.join(packages_to_install)}...", file=stderr_buf)
            
            # 1. Run Install
            log.info(f"Installing missing packages: {packages_to_install}")
            cmd = [sys.executable, "-m", "pip", "install", "--user"] + packages_to_install
            proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if proc.returncode != 0:
                return {"status": "error", "error": f"Install Failed:\n{proc.stderr}"}
            
            # print(proc.stdout, file=stdout_buf) # Log install output
            log.info("Intstallation complete.")

            # 2. DYNAMIC DISCOVERY (The Fix)
            # Instead of guessing the path, we ask pip where it put the first package.
            # This handles 'dist-packages' vs 'site-packages' and python versioning automatically.
            pkg_name = packages_to_install[0]
            show_cmd = [sys.executable, "-m", "pip", "show", pkg_name]
            show_proc = subprocess.run(show_cmd, capture_output=True, text=True)
            
            new_site_path = None
            for line in show_proc.stdout.splitlines():
                if line.startswith("Location:"):
                    # Extract the path (e.g., "Location: /home/user/.local/lib/...")
                    new_site_path = line.split(":", 1)[1].strip()
                    break
            
            # 3. Inject the path
            if new_site_path:
                # Use insert(0) to force priority over system packages
                if new_site_path not in sys.path:
                    sys.path.insert(0, new_site_path)
                    site.addsitedir(new_site_path) # Handles .pth files correctly
                    # print(f"[System] Discovered and added lib path: {new_site_path}", file=stderr_buf)
            else:
                _ = f"[System] Warning: Could not auto-detect install location for {pkg_name}"
                log.warning(_)
                print(_, file=stderr_buf)

            # 4. Invalidate caches so imports work immediately
            importlib.invalidate_caches()


        # --- EXECUTION PHASE ---
        tree = ast.parse(code)
        
        # Transform last node to print()
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            last_node = tree.body[-1]
            print_call = ast.Expr(
                value=ast.Call(
                    func=ast.Name(id='print', ctx=ast.Load()),
                    args=[last_node.value],
                    keywords=[]
                )
            )
            tree.body[-1] = print_call
            ast.fix_missing_locations(tree)

        compiled = compile(tree, filename="<string>", mode="exec")
        
        with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            exec(compiled, {"__name__": "__main__"})
            
        return {
            "stdout": stdout_buf.getvalue(),
            "stderr": stderr_buf.getvalue(),
            "status": "success"
        }
        
    except Exception as e:
        log.info(f"Code execution error: {str(e)}")
        return {
            "stdout": stdout_buf.getvalue(),
            "stderr": stderr_buf.getvalue(),
            "error": str(e),
            "status": "error"
        }