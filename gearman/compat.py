"""Compatability with older Python releases"""

try:
    all # Python 2.5
except NameError:
    def all(values):
        for val in values:
            if not val:
                return False
        return True
