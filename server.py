from engine.pool import start_worker_pool, get_worker_pool, stop_worker_pool
from engine.executor import execute_user_code
from engine.utils import get_missing_imports
from fastmcp import FastMCP
from typing import Annotated
import logging
from pydantic import Field
from contextlib import asynccontextmanager


# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("mcp-server")

MAX_WORKERS = 4
MAX_BACKLOG = 50
EXECUTION_TIMEOUT = 10
QUEUE_TIMEOUT = 50
BACKLOG_TIMEOUT = 1.0
INSTALL_TIMEOUT = 120


@asynccontextmanager
async def server_lifespan(server: FastMCP):
    # 1. Startup Logic
    logger.info("🚀 Lifespan: Starting Worker Pool...")
    start_worker_pool(max_workers=MAX_WORKERS, max_backlog=MAX_BACKLOG)
    
    yield  # Server is running here
    
    # 2. Shutdown Logic
    logger.info("🛑 Lifespan: Shutting down Worker Pool...")
    stop_worker_pool()


mcp = FastMCP("mcp-code-exec", lifespan=server_lifespan)


@mcp.tool(name="python-execute")
async def execute_python_code(
        code: Annotated[str, Field(description="Python code")]
) -> dict:
    """
    Code execution sandbox. Returns `stdout` and `stderr`.
    """
    
    # 1. Pre-Flight Check (Runs in Main Process)
    # This is fast and tells us if we need to extend the clock.
    missing_libs = get_missing_imports(code)
    
    # 2. Decide Timeout dynamically
    execution_timeout = INSTALL_TIMEOUT if missing_libs else EXECUTION_TIMEOUT
    
    try:
        # 3. Dispatch to Worker
        pool = get_worker_pool()
        if not pool:
            return {"pool": pool}
        _wid, res = await pool.run(
            fn=execute_user_code,
            execution_timeout=execution_timeout, # <--- DYNAMIC TIMEOUT
            queue_timeout=QUEUE_TIMEOUT,
            # Pass arguments to the function
            code=code,
            packages_to_install=missing_libs
        )
        return res

    except Exception as e:
        return {
            "status": "error",
            "stdout": "",
            "stderr": "",
            "error": str(e)
        }
    

if __name__ == "__main__":
    mcp.run()