"""Compatability with older Python releases"""

try:
    all # Python 2.5
except NameError:
    def all(iter):
        for v in iter:
            if not v:
                return False
        return True
