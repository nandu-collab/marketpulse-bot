# Minimal imghdr shim (Python 3.13+)
# Provides imghdr.what(file, h=None) for jpg/png/gif/webp
import io

def what(file, h=None):
    if h is None:
        with open(file, "rb") as f:
            h = f.read(16)
    if h.startswith(b"\xff\xd8"):
        return "jpeg"
    if h.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if h.startswith(b"GIF87a") or h.startswith(b"GIF89a"):
        return "gif"
    if h[:4] == b"RIFF" and h[8:12] == b"WEBP":
        return "webp"
    return None
    
