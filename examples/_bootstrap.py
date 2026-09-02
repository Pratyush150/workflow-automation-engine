"""Put ``src/`` on the path so the examples run from a checkout.

No install step, no virtualenv, no ``PYTHONPATH`` incantation: every example is
``python3 examples/<name>.py`` and nothing else.
"""

from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
