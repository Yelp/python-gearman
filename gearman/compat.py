"""
Gearman compatibility module
"""

# Required for python2.4 backward compatibilty
# Add a module attribute called "any" which is equivalent to "any"
try:
    any = any
except NameError:
    def any(iterable):
        """Return True if any element of the iterable is true. If the iterable is empty, return False"""
        for element in iterable:
            if element:
                return True
        return False

# Required for python2.4 backward compatibilty
# Add a module attribute called "all" which is equivalent to "all"
try:
    all = all
except NameError:
    def all(iterable):
        """Return True if all elements of the iterable are true (or if the iterable is empty)"""
        for element in iterable:
            if not element:
                return False
        return True

# Required for python2.4 backward compatibilty
# Add a class called "defaultdict" which is equivalent to "collections.defaultdict"
try:
    from collections import defaultdict
except ImportError:
    class defaultdict(dict):
        """A pure-Python version of Python 2.5's defaultdict
        taken from http://code.activestate.com/recipes/523034-emulate-collectionsdefaultdict/"""
        def __init__(self, default_factory=None, * a, ** kw):
            if (default_factory is not None and
                not hasattr(default_factory, '__call__')):
                raise TypeError('first argument must be callable')
            dict.__init__(self, * a, ** kw)
            self.default_factory = default_factory
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)
        def __missing__(self, key):
            if self.default_factory is None:
                raise KeyError(key)
            self[key] = value = self.default_factory()
            return value
        def __reduce__(self):
            if self.default_factory is None:
                args = tuple()
            else:
                args = self.default_factory,
            return type(self), args, None, None, self.items()
        def copy(self):
            return self.__copy__()
        def __copy__(self):
            return type(self)(self.default_factory, self)
        def __deepcopy__(self, memo):
            import copy
            return type(self)(self.default_factory,
                              copy.deepcopy(self.items()))
        def __repr__(self):
            return 'defaultdict(%s, %s)' % (self.default_factory,
                                            dict.__repr__(self))
