import os


class OvhPath:
    """OVH does not store objects in a directory but using a path-like filename allow a similar result.
    This class standardizes the path-like syntax (Windows path sep \ and Unix path sep /)
    OvhPath(a,b,c,file) -> a/b/c/file
    """

    def __init__(self, *args):
        self.components = [self._strip_slashes(str(e)) for e in args]
        self._str = "/".join(self.components)

    def __repr__(self) -> str:
        return f"'{self._str}'"

    def __str__(self) -> str:
        return self._str

    def __eq__(self, other):
        if isinstance(other, OvhPath):
            return repr(self) == repr(other)
        return False

    def __lt__(self, other):
        if isinstance(other, str):
            return self._str < other

        elif isinstance(other, OvhPath):
            return self._str < str(other)

    def __gt__(self, other):
        return not self.__lt__(other)

    def to_local(self):
        return os.path.join(*self.components)

    def _strip_slashes(self, _str: str) -> str:
        return _str.strip("/").strip("\\").strip("/").strip("\\")
