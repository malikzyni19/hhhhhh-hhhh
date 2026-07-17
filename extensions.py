"""Shared extension instances.

Re-exports the canonical Flask-SQLAlchemy ``db`` instance defined in
``models.py``. Several Phase 11.15 learning-review code paths import ``db``
from ``extensions``; this module makes that import resolve to the single
shared instance in production. Test suites may inject a stub ``extensions``
module into ``sys.modules`` before import, which takes precedence.
"""
from models import db

__all__ = ["db"]
