# exchange/workflows/apps/__init__.py
"""
Workflow registration entrypoint.

All workflow modules must be imported here so their @workflow decorators execute
and register into the runtime registry.
"""

_loaded = False
_tests_loaded = False


def load(include_tests=True):
    """
    Call this from runtime/service to ensure modules are imported.

    Args:
        include_tests (bool): Input value for this call.

    Returns:
        bool: True/False result for this operation.
    """
    global _loaded, _tests_loaded
    if not _loaded:
        from exchange.workflows.apps.examples import workflows  # noqa

        _loaded = True
    if include_tests and not _tests_loaded:
        import exchange.workflows.tests.fixtures  # noqa

        _tests_loaded = True
    return True
