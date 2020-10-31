from typing import Tuple

from vinum.util.util import TREE_INDENT_SYMBOL


class RecursiveTreePrint:
    """
    Abstract class that implements recursive print statement
    with depth indentation.
    """
    @staticmethod
    def _level_indent_string(indent_level: int) -> str:
        return TREE_INDENT_SYMBOL * indent_level

    def str_lines_repr(self, indent_level: int) -> Tuple[str, ...]:
        pass

    def to_str(self, indent_level: int = 0) -> str:
        return '\n'.join(self.str_lines_repr(indent_level))

    def __str__(self):
        return self.to_str(1)
