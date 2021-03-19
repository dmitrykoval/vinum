*****
Vinum
*****

|PyPi|_ |CI|_ |Grade_Python|_ |Codecov|_


.. |PyPi| image:: https://img.shields.io/pypi/v/vinum.svg
.. _PyPi: https://pypi.org/project/vinum/

.. |CI| image:: https://github.com/dmitrykoval/vinum/actions/workflows/ci.yml/badge.svg
.. _CI: https://github.com/dmitrykoval/vinum/actions/workflows/ci.yml

.. |Grade_Python| image:: https://img.shields.io/lgtm/grade/python/g/dmitrykoval/vinum.svg?logo=lgtm&logoWidth=18
.. _Grade_Python: https://lgtm.com/projects/g/dmitrykoval/vinum/context:python

.. |Codecov| image:: https://codecov.io/gh/dmitrykoval/vinum/branch/main/graphs/badge.svg?branch=main&service=github
.. _Codecov: https://codecov.io/gh/dmitrykoval/vinum?branch=main



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


Architecture
============
.. image:: https://github.com/dmitrykoval/vinum/raw/main/doc/source/_static/architecture.png

Vinum uses PostgresSQL parser provided by
`pglast <https://github.com/lelit/pglast>`_ project.

Query planning and execution phases are implemented in Python,
while all physical operators are either implemented in C++ or use
native kernels from Arrow or NumPy. The only exception to this is
native python UDFs, which are running within interpreted Python.

Query execution model is based on the vectorized model described in the prolific
paper by
`P. A. Boncz, M. Zukowski, and N. Nes. Monetdb/x100: Hyper-pipelining query
execution. In CIDR, 2005. <https://ir.cwi.nl/pub/16497/16497B.pdf>`_

Example of a query plan:

.. image:: https://github.com/dmitrykoval/vinum/raw/main/doc/source/_static/query.png


Install
=======

``pip install vinum``


Examples
========

Query python dict
-----------------

Create a Table from a python dict and return result of the query
as a Pandas DataFrame.

    >>> import vinum as vn
    >>> data = {'value': [300.1, 2.8, 880], 'mode': ['air', 'bus', 'air']}
    >>> tbl = vn.Table.from_pydict(data)
    >>> tbl.sql_pd("SELECT value, np.log(value) FROM t WHERE mode='air'")
       value    np.log
    0  300.1  5.704116
    1  880.0  6.779922


Query pandas dataframe
----------------------

    >>> import pandas as pd
    >>> import vinum as vn
    >>> data = {'col1': [1, 2, 3], 'col2': [7, 13, 17]}
    >>> pdf = pd.DataFrame(data=data)
    >>> tbl = vn.Table.from_pandas(pdf)
    >>> tbl.sql_pd('SELECT * FROM t WHERE col2 > 10 ORDER BY col1 DESC')
       col1  col2
    0     3    17
    1     2    13


Run query on a csv stream
-------------------------

For larger datasets or datasets that won't fit into memory -
`stream_csv() <https://vinum.readthedocs.io/en/latest/io.html#stream-csv>`_ is
the recommended way to execute a query. Compressed files are also supported
and can be streamed without prior extraction.

    >>> import vinum as vn
    >>> query = 'select passenger_count pc, count(*) from t group by pc'
    >>> vn.stream_csv('taxi.csv.bz2').sql(query).to_pandas()
       pc  count
    0   0    165
    1   5   3453
    ...

Read and query csv
------------------
    >>> import vinum as vn
    >>> tbl = vn.read_csv('taxi.csv')
    >>> res_tbl = tbl.sql('SELECT key, fare_amount, passenger_count FROM t '
    ...                   'WHERE fare_amount > 5 LIMIT 3')
    >>> res_tbl.to_pandas()
                                key  fare_amount  passenger_count
    0   2010-01-05 16:52:16.0000002         16.9                1
    1  2011-08-18 00:35:00.00000049          5.7                2
    2   2012-04-21 04:30:42.0000001          7.7                1

Compute Euclidean distance with numpy functions
-----------------------------------------------

Use any numpy functions via the 'np.*' namespace.

    >>> import vinum as vn
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('SELECT *, np.sqrt(np.square(x) + np.square(y)) dist '
    ...            'FROM t ORDER BY dist DESC')
       x   y       dist
    0  3  17  17.262677
    1  2  13  13.152946
    2  1   7   7.071068


Compute Euclidean distance with vectorized UDF
----------------------------------------------

Register UDF performing vectorized operations on Numpy arrays.

    >>> import vinum as vn
    >>> vn.register_numpy('distance',
    ...                   lambda x, y: np.sqrt(np.square(x) + np.square(y)))
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('SELECT *, distance(x, y) AS dist '
    ...            'FROM t ORDER BY dist DESC')
       x   y       dist
    0  3  17  17.262677
    1  2  13  13.152946
    2  1   7   7.071068


Compute Euclidean distance with python UDF
------------------------------------------

Register Python lambda function as UDF.

    >>> import math
    >>> import vinum as vn
    >>> vn.register_python('distance', lambda x, y: math.sqrt(x**2 + y**2))
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('SELECT x, y, distance(x, y) AS dist FROM t')
       x   y       dist
    0  1   7   7.071068
    1  2  13  13.152946
    2  3  17  17.262677


Group by z-score
----------------

    >>> import numpy as np
    >>> import vinum as vn
    >>> def z_score(x: np.ndarray):
    ...     "Compute Standard Score"
    ...     mean = np.mean(x)
    ...     std = np.std(x)
    ...     return (x - mean) / std
    ...
    >>> vn.register_numpy('score', z_score)
    >>> tbl = vn.read_csv('taxi.csv')
    >>> tbl.sql_pd('select to_int(score(fare_amount)) AS bucket, avg(fare_amount), count(*) '
    ...            'FROM t GROUP BY bucket ORDER BY bucket limit 3')
       bucket        avg  count_star
    0      -1  -1.839000          10
    1       0   8.817733       45158
    2       1  25.155522        2376



Documentation
=============
* `Vinum documentation <https://vinum.readthedocs.io/en/latest/>`_
* `Getting started <https://vinum.readthedocs.io/en/latest/getting_started.html>`_


What Vinum is not
=================
Vinum is not a Database Management System, there are no plans to support
DML and transactions.
If you need a DBMS designed for data analytics and OLAP,
or don't need Python UDFs,
consider using excellent `DuckDB <https://duckdb.org/>`_ - it is based on
a solid scientific foundation and is extremely fast.

Dependencies
============
* `Pyarrow <https://arrow.apache.org/docs/python/>`_
* `NumPy <https://numpy.org/>`_
* `pglast <https://github.com/lelit/pglast>`_

Inspiration
===========
* `Intro to Database Systems <https://www.youtube.com/playlist?list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi>`_ and
  `Advanced Database Systems <https://www.youtube.com/playlist?list=PLSE8ODhjZXjasmrEd2_Yi1deeE360zv5O>`_
  by `Andy Pavlo <https://twitter.com/andy_pavlo>`_
* `P. A. Boncz, M. Zukowski, and N. Nes. Monetdb/x100: Hyper-pipelining query
  execution. In CIDR, 2005. <https://ir.cwi.nl/pub/16497/16497B.pdf>`_
* `DuckDB <https://duckdb.org/>`_

Future plans
============
* Support joins and sub-queries.
* Consider `Gandiva <https://github.com/dremio/gandiva>`_
  for expression evaluation.
* Parallel execution.
