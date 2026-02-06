from dataclasses import dataclass
from typing import Final, Tuple
import sys
import os

@dataclass(frozen=True)
class EngineConfig:
    google_api_key: str | None

    # --- Paths ---
    base_dir: Final[str] = os.path.realpath("/app")
    sandbox_dir: Final[str] = os.path.join(base_dir, "sandbox")

    user_lib_dir: Final[str] = os.path.realpath(
        os.path.join(os.environ.get("HOME", "/home/mcp-user"), ".local")
    )

    system_lib_dir: Final[str] = os.path.realpath(
        f"/usr/local/lib/python{sys.version_info.major}.{sys.version_info.minor}"
    )

    # --- Security paths ---
    allowed_read_paths: Final[Tuple[str, ...]] = (
        user_lib_dir,
        system_lib_dir,
        "/usr/lib",
        "/etc/ssl",
    )

    allowed_write_paths: Final[Tuple[str, ...]] = (
        # session specific sanbox and pip install permission is granted dynamically
        # user_lib_dir,
    )

    # --- Runtime ---
    preload_libraries: Final[Tuple[str, ...]] = (
        "math",
        "numpy",
        "sympy",
        "pandas",
        "shutil"
    )

    @classmethod
    def from_env(cls) -> "EngineConfig":
        return cls(
            google_api_key=os.environ.get("GOOGLE_API_KEY"),
        )


config = EngineConfig.from_env()
