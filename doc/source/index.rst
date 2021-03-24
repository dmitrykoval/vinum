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
  version of operators and only executes Python interpreted code where
  strictly necessary (ie. for native Python UDFs).

* Allows to use functions available within the host Python interpreter
  as UDFs, including native Python, NumPy, etc.

* Vinum's execution model doesn't require input datasets to fit into memory,
  as it operates on the stream batches. However, final result is fully
  materialized in memory.

* Written in the mix of C++ and Python and built from ground up on top of
  `Apache Arrow <https://arrow.apache.org/>`_, which provides the
  foundation for moving data and enables minimal
  overhead for transferring data to and from Numpy and Pandas.


Architecture
============
.. image:: https://github.com/dmitrykoval/vinum/raw/main/doc/source/_static/architecture.png

Vinum uses PostgresSQL parser provided by
`pglast <https://github.com/lelit/pglast>`_ project.

Query planner and executor are implemented in Python,
while all the physical operators are either implemented in C++ or use
compiled vectorized kernels from Arrow or NumPy. The only exception to this is
native python UDFs, which are running within interpreted Python.

Query execution model is based on the vectorized model described in the prolific
paper by
`P. A. Boncz, M. Zukowski, and N. Nes. Monetdb/x100: Hyper-pipelining query
execution. In CIDR, 2005. <https://ir.cwi.nl/pub/16497/16497B.pdf>`_

Example of a query plan:

.. image:: https://github.com/dmitrykoval/vinum/raw/main/doc/source/_static/query.png



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
