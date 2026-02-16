# exchange/workflows/util/perspective.py
"""Small Perspective utility helpers used by workflows screens."""


def textToClipboard(text):
    """
    Writes text to the browser clipboard using client-side JS.
            Automatically checks if runJavaScriptAsync is available.

    Args:
        text (object): Input value for this call.

    Returns:
        None: No explicit value is returned.
    """

    if not hasattr(system.perspective, "runJavaScriptAsync"):
        return

    # JS function to write to clipboard
    js = """
    async (text) => {
        try {
            await navigator.clipboard.writeText(text);
            return "Clipboard write successful";
        } catch (err) {
            return "Clipboard write FAILED: " + err;
        }
    }
    """

    args = {"text": text}

    # Execute JS on the client
    system.perspective.runJavaScriptAsync(
        js,
        args,
    )

    return
