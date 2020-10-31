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


from vinum.core.functions import (   # noqa: F401
    register_python,
    register_numpy,
)

from vinum.io.arrow import (  # noqa: F401
    read_csv,
    read_json,
    read_parquet,
)

from vinum.core.table import Table  # noqa: F401


__version__ = '0.0.1'


__doc__ = """
vinum - a Python data analysis library with SQL interface
for in-memory analytics.
=================================================================================
"""
