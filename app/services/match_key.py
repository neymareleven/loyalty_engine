from __future__ import annotations

import re


def normalize_match_key(value) -> str | None:
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or None
