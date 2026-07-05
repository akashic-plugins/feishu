from __future__ import annotations

import os
import sys
from pathlib import Path


def _agent_root() -> Path:
    env = os.environ.get("AKASHIC_AGENT_ROOT", "").strip()
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[3] / "akasic-agent"


root = _agent_root()
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))
if str(root) not in sys.path:
    sys.path.insert(0, str(root))
