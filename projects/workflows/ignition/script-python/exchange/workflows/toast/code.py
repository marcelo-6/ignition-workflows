"""Perspective toast helpers used across workflow UIs."""

def show(
    viewPath=None,
    viewParams=None,
    message=None,
    preserveNewlines=False,
    **toastOverrides
):
    """
    Display a Perspective toast (EMBR Periscope module required). If `viewPath` is set, mounts a view; otherwise shows plain text.
    
            Usage Examples:
            ----------
                # Embedded view toast (default light/info)
                show(
                    viewPath="Project/MyModule/StatusView",
                    viewParams={"status": "OK"}
                )
    
                # Plain text toast (warning, with close button)
                show(
                    message="Data upload failed!",
                    type="warning",
                    closeButton=True
                )
    
                # Embedded view with overrides (never auto-close)
                show(
                    viewPath="Project/MyModule/DetailView",
                    viewParams={"id": 42},
                    autoClose=False,
                    type="success"
                )
                Available toast options (defaults shown):
                    autoClose=5000          # milliseconds before auto-close, or False to disable
                    closeButton=True        # show the close (x) button
                    closeOnClick=True       # close when user clicks anywhere in the toast
                    draggable=False         # allow drag-to-dismiss behavior
                    hideProgressBar=False   # hide the timer progress bar
                    icon=True               # show default icon
                    pauseOnFocusLoss=False  # pause timer if window loses focus
                    pauseOnHover=True       # pause timer while hovering
                    position='top-right'    # top-left, top-right, top-center, bottom-left, bottom-right, bottom-center
                    theme='dark'            # light, dark, colored
                    type='info'             # info, success, warning, error

    Args:
        viewPath (str): Perspective view path to mount in a toast.
        viewParams (dict): Perspective view parameters for mounted views.
        message (dict): Message payload for mailbox or log usage.
        preserveNewlines (bool): Input value for this call.
        **toastOverrides (dict): Input value for this call.

    Returns:
        bool: True/False result for this operation.
    """

    if not hasattr(system.perspective, "runJavaScriptAsync"):
        return False
    # Merge default toast options
    defaults = {
        "autoClose": 5000,
        "closeButton": True,
        "closeOnClick": False,
        "draggable": False,
        "hideProgressBar": False,
        "icon": True,
        "pauseOnFocusLoss": False,
        "pauseOnHover": True,
        "position": "top-right",
        "theme": "dark",
        "type": "info",
    }
    toastOptions = defaults.copy()
    toastOptions.update(toastOverrides)

    # Ensure nested style dict exists if we need to tweak styles
    if "style" not in toastOptions or toastOptions["style"] is None:
        toastOptions["style"] = {}

    # If plain-text mode with newline preservation, make sure CSS preserves them
    if not viewPath and preserveNewlines:
        # If user didn't override, set sensible defaults
        toastOptions["style"].setdefault("whiteSpace", "pre")
        # Let user interact with content inside the toast
        toastOptions["style"].setdefault("pointerEvents", "all")

    # Determine toast mode
    if viewPath:
        if viewParams is None:
            viewParams = {}
        js = """(viewPath, viewParams, toastOptions) => {
            periscope.toast(({ toastProps }) => {
                return perspective.createView({
                    resourcePath: viewPath,
                    mountPath: `toast-${toastProps.toastId}`,
                    params: viewParams
                })
            }, toastOptions)
        }"""
        args = {
            "viewPath": viewPath,
            "viewParams": viewParams,
            "toastOptions": toastOptions,
        }
    else:
        if not message:
            message = ""  # system.util.jsonEncode(system.perspective.getSessionInfo())
        js = """(message, toastOptions) => {
            periscope.toast(message, toastOptions)
        }"""
        args = {"message": message, "toastOptions": toastOptions}

    system.perspective.runJavaScriptAsync(js, args)
    return True


def dismissAll():
    """
    Dismiss all active (and queued) toast notifications in the current Perspective session.

    Args:
        None.

    Returns:
        bool: True/False result for this operation.
    """

    if not hasattr(system.perspective, "runJavaScriptAsync"):
        return False

    js = r"""() => {
      try { periscope.toast.dismiss(); } catch(e) {}
      // If queueing is enabled in the module, clear waiting items too (no-op if not present)
      try { if (periscope.toast.clearWaitingQueue) periscope.toast.clearWaitingQueue(); } catch(e) {}
    }"""
    system.perspective.runJavaScriptAsync(js)
    return True
