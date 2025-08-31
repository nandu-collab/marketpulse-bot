# imghdr.py - custom replacement for deprecated Python stdlib

import os

def what(file, h=None):
    if h is None:
        if not os.path.isfile(file):
            return None
        with open(file, 'rb') as f:
            h = f.read(32)

    if h.startswith(b'\211PNG\r\n\032\n'):
        return 'png'
    if h.startswith(b'\377\330'):
        return 'jpeg'
    if h[6:10] in (b'JFIF', b'Exif'):
        return 'jpeg'
    if h.startswith((b'GIF87a', b'GIF89a')):
        return 'gif'
    if h.startswith(b'BM'):
        return 'bmp'
    return None
  
