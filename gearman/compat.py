"""Compatability with older Python releases"""

try:
    all # Python 2.5
except NameError:
    def all(values):
        for v in values:
            if not v:
                return False
        return True
