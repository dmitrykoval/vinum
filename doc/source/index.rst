Vinum documentation
===================

**Vinum** is a SQL query processor for Python,
designed for data analysis workflows and in-memory analytics.

When should I use Vinum?
========================
Vinum is running inside of the host Python process and allows to execute any
functions available to the interpreter as UDFs. If you are doing data analysis
or running ETL in Python, Vinum allows to execute efficient SQL queries
with an ability to call native Python UDFs.

Key Features:
=============

* Vinum is running inside of the host Python process and has a hybrid query
  execution model - whenever possible it would prefer native compiled
  version of operators and only executes python interpreted code where
  strictly necessary (ie. for native Python UDFs).

* Allows to use functions available within the host Python interpreter
  as UDFs, including native Python, NumPy, Pandas, etc.

* Vinum's execution model doesn't require input datasets to fit into memory,
  as it operates on the stream batches. However, final result is fully
  materialized in memory.

* Written in the mix of C++ and Python and built from ground up on top of
  `Apache Arrow <https://arrow.apache.org/>`_, which provides the
  foundation for moving data and enables minimal
  overhead for transferring data to and from Numpy and Pandas.



.. toctree::
    :maxdepth: 3
    :caption: Contents:

    installation

    getting_started

    sql

    api




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
