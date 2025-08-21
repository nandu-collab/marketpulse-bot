# imghdr.py — compatibility shim for python-telegram-bot 13.x on Python 3.13+
def what(file, h=None):
    try:
        if h is None:
            with open(file, "rb") as f:
                h = f.read(32)
        if h[:8] == b"\x89PNG\r\n\x1a\n": return "png"
        if h[:3] == b"\xff\xd8\xff": return "jpeg"
        if h[:6] in (b"GIF87a", b"GIF89a"): return "gif"
        if h[:4] == b"RIFF" and h[8:12] == b"WEBP": return "webp"
    except:
        pass
    return None
    
