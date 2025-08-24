# Minimal shim for Python 3.13 where stdlib imghdr was removed.
# python-telegram-bot 13.x imports imghdr on import, so this keeps it happy.
def what(file, h=None):
    return None
    
