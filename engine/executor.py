import ast
import io
from contextlib import redirect_stdout, redirect_stderr
import subprocess
import sys

def execute_user_code(code: str, packages_to_install: list[str] = None) -> dict:
    """
    This function runs INSIDE the worker process.
    It parses AST, captures stdout, and returns the result.
    """ 
    # INSTALLATION PHASE
    if packages_to_install:
        try:
            # We assume --user install to /home/mcp-user/.local
            cmd = [sys.executable, "-m", "pip", "install", "--user"] + packages_to_install
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            
            # CRITICAL: Invalidate import caches so Python 'sees' the new libs
            import importlib
            importlib.invalidate_caches()
            
        except subprocess.CalledProcessError as e:
            return {"status": "error", "error": f"Install failed: {e.stderr}"}

    # AST Magic to capture last expression
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return {"error": f"Syntax Error: {e}"}

    # Transform last node to print() if it's an expression
    if tree.body and isinstance(tree.body[-1], ast.Expr):
        last_node = tree.body[-1]
        
        # Create a print() call wrapping the last expression
        print_call = ast.Expr(
            value=ast.Call(
                func=ast.Name(id='print', ctx=ast.Load()),
                args=[last_node.value],
                keywords=[]
            )
        )
        # Check if the expression is None (to avoid printing 'None')
        # (Simplified for brevity; usually we print everything)
        tree.body[-1] = print_call
        ast.fix_missing_locations(tree)

    # 2. Execution
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()
    
    try:
        # Compile and Run
        compiled = compile(tree, filename="<string>", mode="exec")
        
        with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            # We use a fresh global dict for every execution to avoid
            # polluting the worker's permanent state (unless you want persistence!)
            # IF YOU WANT PERSISTENCE: Defined 'user_globals' at module level
            exec(compiled, {"__name__": "__main__"})
            
        return {
            "stdout": stdout_buf.getvalue(),
            "stderr": stderr_buf.getvalue(),
            "status": "success"
        }
        
    except Exception as e:
        return {
            "stdout": stdout_buf.getvalue(),
            "stderr": stderr_buf.getvalue(),
            "error": str(e),
            "status": "error"
        }