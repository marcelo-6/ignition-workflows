# exchange/workflows/bootstrap.py
"""
Workflows - a DBOS-inspired durable-ish workflow executor for Ignition.

Public surface area:
- Decorators: workflow, step
- Singleton runtime accessor: getWorkflows()
- Convenience API: exchange.workflows.api.service

Notes:
- This package is designed for Ignition Gateway scope.
- All I/O should occur within steps.
"""

from exchange.workflows.engine.runtime import workflow, step  # noqa: F401
from exchange.workflows.engine.instance import getWorkflows  # noqa: F401
