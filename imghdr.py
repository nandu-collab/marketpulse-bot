# Minimal shim for Python 3.13 where stdlib imghdr was removed
def what(file, h=None):
    # We don't need image detection for this bot (text-only posts)
    # Returning None emulates "unknown type" and is safe.
    return None
    
