*****
Vinum
*****

|PyPi|_ |Travis|_ |Codecov|_ |GitTutorial|_


.. |PyPi| image:: https://img.shields.io/pypi/v/vinum.svg
.. _PyPi: https://pypi.org/project/vinum/

.. |Travis| image:: https://travis-ci.com/dmitrykoval/vinum.svg?branch=main
.. _Travis: https://travis-ci.com/dmitrykoval/vinum

.. |Codecov| image:: https://codecov.io/gh/dmitrykoval/vinum/branch/main/graphs/badge.svg?branch=main&service=github
.. _Codecov: https://codecov.io/gh/dmitrykoval/vinum?branch=main

.. |GitTutorial| image:: https://img.shields.io/badge/PR-Welcome-%23FF8300.svg?
.. _GitTutorial: https://git-scm.com/book/en/v2/GitHub-Contributing-to-a-Project


**Vinum** is a SQL processor written in pure Python, designed for
data analysis workflows and in-memory analytics. 
Conceptually, Vinum's design goal is to provide deeper integration of 
Python data analysis tools such as `Numpy <https://numpy.org/>`_,
`Pandas <https://pandas.pydata.org/>`_ or in general any Python code with
the SQL language. Key features include native support of
vectorized Numpy and Python functions as UDFs in SQL queries.


Key Features:
=============

* Natively supports vectorized Numpy and Python functions inside of
  SELECT, WHERE, GROUP BY, HAVING and ORDER BY clauses.
  All the numpy functions are available by default via the 'np.*' namespace.

* Written in pure Python and built from ground up on top of
  `Apache Arrow <https://arrow.apache.org/>`_ and
  `Numpy <https://numpy.org/>`_.

* Apache Arrow provides the foundation for "moving" data and enables minimal
  overhead for transferring data to and from Numpy and Pandas.

* Designed for in-memory analytics workflows, based on columnar memory layout.


Design
======
Vinum's query planner compiles SQL SELECT statement into a DAG of
vectorized Arrow and Numpy operators and therefore integration with Numpy,
Arrow or native Python functions comes naturally.
In Vinum, all Numpy functions are first class citizens and can be used inside 
of SELECT, WHERE, GROUP BY, HAVING and ORDER BY clauses.

Below is an example of a possible simplified query plan.

.. image:: https://github.com/dmitrykoval/vinum/raw/main/doc/source/_static/dag_ex.png


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


Query csv
---------
    >>> import vinum as vn
    >>> tbl = vn.read_csv('test.csv')
    >>> res_tbl = tbl.sql('SELECT * FROM t WHERE fare > 5 LIMIT 3')
    >>> res_tbl.to_pandas()
       id                            ts        lat        lng  fare
    0   1   2010-01-05 16:52:16.0000002  40.711303 -74.016048  16.9
    1   2  2011-08-18 00:35:00.00000049  40.761270 -73.982738   5.7
    2   3   2012-04-21 04:30:42.0000001  40.733143 -73.987130   7.7


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
    ...     """Compute Standard Score"""
    ...     mean = np.mean(x)
    ...     std = np.std(x)
    ...     return (x - mean) / std
    ...
    >>> vn.register_numpy('score', z_score)
    >>> tbl = vn.read_csv('test.csv')
    >>> tbl.sql_pd('select int(score(fare)) AS bucket, avg(fare), count(*) '
    ...            'FROM t GROUP BY bucket ORDER BY bucket')
       bucket        avg  count
    0       0   8.111630     92
    1       1  19.380000      3
    2       2  27.433333      3
    3       3  34.670000      1
    4       6  58.000000      1



Documentation
=============
* `Vinum documentation <https://vinum.readthedocs.io/en/latest/>`_
* `Getting started <https://vinum.readthedocs.io/en/latest/getting_started.html>`_


What Vinum is not
=================
Vinum is not a Database Management System, there are no plans to support
INSERT or UPDATE statements, as well as MVCC.

Dependencies
============
* `Pyarrow <https://arrow.apache.org/docs/python/>`_
* `NumPy <https://numpy.org/>`_
* `Moz SQL Parser <https://github.com/mozilla/moz-sql-parser>`_

Future plans
============
* Performance improvements.
* Support sub-queries and JOINs.
* Parallel execution.
