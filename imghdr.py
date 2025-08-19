# imghdr.py — shim for Python 3.13+
# Minimal compatibility layer so libraries that `import imghdr` keep working.

def what(file, h=None):
    """
    Return 'png' | 'jpeg' | 'gif' | 'webp' if detected, else None.
    API-compatible with the old stdlib: what(filename, h=None)
    """
    try:
        if h is None:
            with open(file, "rb") as f:
                h = f.read(32)
        # PNG
        if h[:8] == b"\x89PNG\r\n\x1a\n":
            return "png"
        # JPEG
        if h[:3] == b"\xff\xd8\xff":
            return "jpeg"
        # GIF
        if h[:6] in (b"GIF87a", b"GIF89a"):
            return "gif"
        # WEBP
        if h[:4] == b"RIFF" and h[8:12] == b"WEBP":
            return "webp"
    except Exception:
        pass
    return None
    
