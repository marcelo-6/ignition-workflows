"""Exception helpers for bridging Jython and Java stack traces."""

import sys
import traceback

from java.lang import Exception as JavaException, StackTraceElement, Throwable


def fullClassName(cls):
    """Return dotted class hierarchy name for stack element construction."""
    try:
        if cls and cls.__bases__:
            return fullClassName(cls.__bases__[0]) + "." + cls.__name__
        if cls:
            return cls.__name__
    except:
        pass
    return "<global>"


class PythonAsJavaException(JavaException):
    """Java wrapper for Jython exceptions preserving Python traceback frames."""

    def __init__(self, pyexc, tb=None):
        super(PythonAsJavaException, self).__init__(repr(pyexc), None, True, True)
        traceElements = []
        if tb is None:
            _etype, _eobj, tb = sys.exc_info()
        while tb:
            try:
                code = tb.tb_frame.f_code
                vnames = code.co_varnames
                if vnames and vnames[0] in ("cls", "self"):
                    ref = tb.tb_frame.f_locals[vnames[0]]
                    if vnames[0] == "self":
                        className = fullClassName(ref.__class__)
                    else:
                        className = fullClassName(ref)
                else:
                    className = "<global>"
                traceElements.append(
                    StackTraceElement(
                        className, code.co_name, code.co_filename, int(tb.tb_lineno)
                    )
                )
            except:
                pass
            tb = tb.tb_next
        traceElements.reverse()
        try:
            self.setStackTrace(traceElements)
        except:
            pass


def formatCaughtException(exc):
    """Format Java or Python exception with best-effort stack details."""
    parts = []

    try:
        if isinstance(exc, Throwable):
            parts.append("%s: %s" % (exc.getClass().getName(), str(exc)))
            trace = exc.getStackTrace()
            if trace:
                lines = []
                for el in trace:
                    lines.append("  at %s" % str(el))
                parts.append("java_stack:\n%s" % "\n".join(lines))
        else:
            wrapped = PythonAsJavaException(exc, tb=sys.exc_info()[2])
            trace = wrapped.getStackTrace()
            if trace:
                lines = []
                for el in trace:
                    lines.append("  at %s" % str(el))
                parts.append("python_stack:\n%s" % "\n".join(lines))
    except:
        try:
            parts.append(str(exc))
        except:
            parts.append("<unprintable exception>")

    try:
        py = traceback.format_exc()
        if py and "NoneType: None" not in py:
            parts.append(py)
    except:
        pass

    if not parts:
        try:
            return str(exc)
        except:
            return "<unprintable exception>"
    return "\n".join(parts)


__all__ = ["JavaException", "PythonAsJavaException", "formatCaughtException"]
