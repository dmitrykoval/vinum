# flake8: noqa

hard_dependencies = ("pyarrow", "numpy", "moz_sql_parser")
missing_dependencies = []

for dependency in hard_dependencies:
    try:
        __import__(dependency)
    except ImportError as e:
        missing_dependencies.append(f"{dependency}: {e}")

if missing_dependencies:
    raise ImportError(
        "Unable to import required dependencies:\n" + "\n".join(
            missing_dependencies
        )
    )
del hard_dependencies, dependency, missing_dependencies

# Need to create symlinks to pyarrow shared libs
pyarrow.create_library_symlinks()

import vinum_lib
if vinum_lib.import_pyarrow() != 0:
    raise StandardError('Failed to initialize pyarrow C++ bindings.')

from vinum.core.udf import (   # noqa: F401
    register_python,
    register_numpy,
)

from vinum.io.arrow import (  # noqa: F401
    stream_csv,
    read_csv,
    read_json,
    read_parquet,
)

from vinum.api.table import Table  # noqa: F401
from vinum.api.stream_reader import StreamReader  # noqa: F401


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


__doc__ = """
**Vinum** is a SQL processor written in pure Python, designed for
data analysis workflows and in-memory analytics. 
Conceptually, Vinum's design goal is to provide deeper integration of 
Python data analysis tools such as `Numpy <https://numpy.org/>`_,
`Pandas <https://pandas.pydata.org/>`_ or in general any Python code with
the SQL language. Key features include native support of
vectorized Numpy and Python functions as UDFs in SQL queries.
"""

_batch_size = 10000


def get_batch_size():
    return _batch_size


def set_batch_size(batch_size: int):
    _batch_size = batch_size

