from contextvars import ContextVar
from typing import Optional

# Tracks which user session is currently executing
active_session_id: ContextVar[Optional[str]] = ContextVar("active_session_id", default=None)

# A boolean flag that 'unlocks' the library directory for writes
pip_is_active: ContextVar[bool] = ContextVar("pip_is_active", default=False)