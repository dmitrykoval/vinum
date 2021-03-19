import pyarrow as pa
from typing import Dict, TYPE_CHECKING

from vinum.arrow.arrow_table import ArrowTable
from vinum.executor.executor import RecursiveExecutor
from vinum.parser.parser import parser_factory
from vinum.parser.query import Query
from vinum.planner.planner import QueryPlanner

if TYPE_CHECKING:
    import pandas as pd
    from vinum.core.base import Operator


class Table:
    """
    `Table` represents a tabular dataset and provides SQL SELECT interface
    for data manipulation. Consists of a set of named columns of equal length.
    Essentially, is the same abstraction as a table in the
    relational databases world.

    Provides minimum overhead transfer to and from Pandas DataFrame
    as well as Arrow Table, powered by
    `Apache Arrow <https://arrow.apache.org/>`_ framework.

    There are two major ways to instantiate `Table`:
        1. By invoking ``Table.from_*`` factory methods.
        2. By using convenice functions, such as:
            :func:`vinum.read_csv`, :func:`vinum.read_parquet`,
            :func:`vinum.read_json`.

    By default, all the Numpy functions are available via 'np.*'
    namespace.

    User Defined Function can be registered via
    :func:`vinum.register_python` or :func:`vinum.register_numpy`.

    Parameters
    ----------
    arrow_table : pyarrow.Table
        Arrow Table containing the dataset

    Examples
    --------
    >>> import pyarrow as pa
    >>> import vinum as vn
    >>> data = {'col1': [1, 2, 3], 'col2': [7, 13, 17]}
    >>> arrow_table = pa.Table.from_pydict(data)
    >>> tbl = vn.Table(arrow_table)
    >>> tbl.sql_pd('select * from t')
       col1  col2
    0     1     7
    1     2    13
    2     3    17


    >>> import pandas as pd
    >>> import vinum as vn
    >>> pdf = pd.DataFrame(data={'col1': [1, 2, 3], 'col2': [7, 13, 17]})
    >>> tbl = vn.Table.from_pandas(pdf)
    >>> tbl.sql('select * from t')
    <vinum.core.table.Table object at 0x114cff7f0>

    Notice that :func:`vinum.Table.sql` returns
    :class:`vinum.Table` object type.

    >>> tbl.sql_pd('select * from t')
       col1  col2
    0     1     7
    1     2    13
    2     3    17

    Notice that :func:`vinum.Table.sql_pd` returns
    :class:`pandas.DataFrame`.

    To register a Numpy UDF:

    >>> import vinum as vn
    >>> vn.register_numpy('product', lambda x, y: x*y)
    >>> tbl.sql_pd('select product(col1, col2) from t')
       product
    0        7
    1       26
    2       51

    'product' UDF defined above, would perform vectorized multiplication
    on arrays, represented by columns 'col1' and 'col2'.

    """
    def __init__(self, arrow_table: pa.Table, reader=None):
        super().__init__()
        self._arrow_table: ArrowTable = ArrowTable(arrow_table)
        self._reader = reader

    @classmethod
    def from_pydict(cls, pydict: Dict):
        """
        Constructs a `Table` from :class:`dict`

        Parameters
        ----------
        pydict : Python dictionary

        Returns
        -------
        :class:`vinum.Table`
            Vinum Table instance.

        Examples
        --------
        >>> import vinum as vn
        >>> data = {'col1': [1, 2, 3], 'col2': [7, 13, 17]}
        >>> tbl = vn.Table.from_pydict(data)
        >>> tbl.sql_pd('select * from t')
           col1  col2
        0     1     7
        1     2    13
        2     3    17
        """
        arrow_table = pa.Table.from_pydict(pydict)
        return cls(arrow_table)

    @classmethod
    def from_arrow(cls, arrow_table: pa.Table):
        """
        Constructs a `Table` from :class:`pyarrow.Table`

        Parameters
        ----------
        arrow_table : `pyarrow.Table` object

        Returns
        -------
        :class:`vinum.Table`
            Vinum Table instance.

        Examples
        --------
        >>> import pyarrow as pa
        >>> import vinum as vn
        >>> data = {'col1': [1, 2, 3], 'col2': [7, 13, 17]}
        >>> arrow_table = pa.Table.from_pydict(data)
        >>> tbl = vn.Table.from_arrow(arrow_table)
        >>> tbl.sql_pd('select * from t')
           col1  col2
        0     1     7
        1     2    13
        2     3    17
        """
        return cls(arrow_table)

    @classmethod
    def from_pandas(cls, data_frame):
        """
        Constructs a `Table` from :class:`pandas.DataFrame`

        Parameters
        ----------
        data_frame : `pandas.DataFrame` object

        Returns
        -------
        :class:`vinum.Table`
            Vinum Table instance.

        Examples
        --------
        >>> import pandas as pd
        >>> import vinum as vn
        >>> data = {'col1': [1, 2, 3], 'col2': [7, 13, 17]}
        >>> pdf = pd.DataFrame(data=data)
        >>> tbl = vn.Table.from_pandas(pdf)
        >>> tbl.sql_pd('select * from t')
           col1  col2
        0     1     7
        1     2    13
        2     3    17
        """
        table = pa.Table.from_pandas(data_frame)
        return cls(table)

    @staticmethod
    def _create_query_tree(query, schema: pa.Schema) -> Query:
        return parser_factory(query, schema).parse()

    @staticmethod
    def _create_plan_dag(query_tree: Query,
                         query_table: ArrowTable) -> 'Operator':
        return QueryPlanner(query_tree, table=query_table).plan_query()

    def sql(self, query: str):
        """
        Executes SQL SELECT query on a Table and returns
        the result of the query.

        Parameters
        ----------
        query : str
            SQL SELECT query.

        Returns
        -------
        :class:`vinum.Table`
            Vinum Table instance.

        See also
        --------
        sql_pd : Executes SQL SELECT query on a Table and returns
            the result of the query as a Pandas DataFrame.

        Notes
        -----
        Only SELECT statements are supported.
        For SELECT statements, JOINs and subqueries are currently
        not supported.
        However, optimizations aside, one can run a subsequent query on the
        result of a query, to model the behaviour of subqueries.

        Table name in 'select * from table' clause is ignored.
        The table of the underlying DataFrame is used to run a query.

        By default, all the Numpy functions are available via 'np.*'
        namespace.

        User Defined Function can be registered via
        :func:`vinum.register_python` or :func:`vinum.register_numpy`

        Examples
        --------
        Using pandas dataframe:

        >>> import pandas as pd
        >>> import vinum as vn
        >>> data = {'col1': [1, 2, 3], 'col2': [7, 13, 17]}
        >>> pdf = pd.DataFrame(data=data)
        >>> tbl = vn.Table.from_pandas(pdf)
        >>> res_tbl = tbl.sql('select * from t')
        >>> res_tbl.to_pandas()
           col1  col2
        0     1     7
        1     2    13
        2     3    17


        Running queries on a csv file:

        >>> import vinum as vn
        >>> tbl = vn.read_csv('taxi.csv')
        >>> res_tbl = tbl.sql('select key, fare_amount from t limit 3')
        >>> res_tbl.to_pandas()
                                    key  fare_amount
        0   2009-06-15 17:26:21.0000001          4.5
        1   2010-01-05 16:52:16.0000002         16.9
        2  2011-08-18 00:35:00.00000049          5.7

        >>> import vinum as vn
        >>> tbl = vn.read_csv('taxi.csv')
        >>> res_tbl = tbl.sql('select to_int(fare_amount) fare, count(*) from t '
        ...                   'group by fare order by fare limit 3')
        >>> res_tbl.to_pandas()
           fare  count_star
        0    -5           1
        1    -3           1
        2    -2           4
        """
        query_table = self._arrow_table

        query_tree = self._create_query_tree(query, query_table.get_schema())
        query_dag = self._create_plan_dag(query_tree, query_table)

        executor = RecursiveExecutor()
        result_table = executor.execute(query_dag)

        return Table(result_table.get_table())

    def sql_pd(self, query: str):
        """
        Executes SQL SELECT query on a Table and returns
        the result of the query as a Pandas DataFrame.

        This is a convience method which runs :method:`vinum.Table.sql` and
        then calls :method:`vinum.Table.to_pandas` on the result.
        Equivalent to:

        >>> res_tbl = tbl.sql('select * from t')
        >>> res_tbl.to_pandas()
           col1  col2
        0     1     7
        1     2    13
        2     3    17


        Parameters
        ----------
        query : str
            SQL SELECT query.

        Returns
        -------
        :class:`pandas.DataFrame`
            Pandas DataFrame.

        See also
        --------
        sql : Executes SQL SELECT query on a Table and returns
            the result of the query.

        Notes
        -----
        Only SELECT statements are supported.
        For SELECT statements, JOINs and subqueries are currently
        not supported.
        However, optimizations aside, one can run a subsequent query on the
        result of the query, to model the behaviour of subqueries.

        Table name in ``select * from table`` clause is ignored.
        The table of the underlying Table object is used to run a query.

        Examples
        --------
        Using pandas dataframe:

        >>> import pandas as pd
        >>> import vinum as vn
        >>> data = {'col1': [1, 2, 3], 'col2': [7, 13, 17]}
        >>> pdf = pd.DataFrame(data=data)
        >>> tbl = vn.Table.from_pandas(pdf)
        >>> tbl.sql_pd('select * from t')
           col1  col2
        0     1     7
        1     2    13
        2     3    17


        Running queries on a csv file:

        >>> import vinum as vn
        >>> tbl = vn.read_csv('taxi.csv')
        >>> tbl.sql_pd('select key, passenger_count from t limit 3')
                                    key  passenger_count
        0   2009-06-15 17:26:21.0000001                1
        1   2010-01-05 16:52:16.0000002                1
        2  2011-08-18 00:35:00.00000049                2

        >>> import vinum as vn
        >>> tbl = vn.read_csv('taxi.csv')
        >>> res_tbl = tbl.sql('select to_int(fare_amount) fare, count(*) from t '
        ...                   'group by fare order by fare limit 3')
        >>> res_tbl.to_pandas()
           fare  count_star
        0    -5           1
        1    -3           1
        2    -2           4
        """

        return self.sql(query).to_pandas()

    def explain(self, query: str, print_query_tree=False):
        """
        Returns a query plan as a string.

        Parameters
        ----------
        query : str
            SQL SELECT query.
        print_query_tree : bool, optional
            Set to True to also return an AST of the query.

        Returns
        -------
        str : Query Plan.

        See also
        --------
        sql : Executes SQL SELECT query on a Table and returns
            the result of the query.
        sql_pd : Executes SQL SELECT query on a Table and returns
            the result of the query as a Pandas Table.


        Examples
        --------
        >>> import vinum as vn
        >>> tbl = vn.read_csv('taxi.csv')
        >>> print(tbl.explain('select to_int(fare_amount) fare, count(*) from t '
        ...                   'group by fare order by fare limit 3'))

        Query plan:
           Operator: MaterializeTableOperator
            Operator: SliceOperator
              Operator: ProjectOperator
                  Column: to_int_4304514592
                  Column: count_star_4556372144
                Operator: SortOperator
                    Column: to_int_4304514592
                  Operator: AggregateOperator
                    Operator: ProjectOperator
                      VectorizedExpression: IntCastFunction
                          Column: fare_amount
                      Operator: ProjectOperator
                          Column: fare_amount
                        Operator: TableReaderOperator
        """
        query_tree = self._create_query_tree(query,
                                             self._arrow_table.get_schema())
        plan_str = ''
        if print_query_tree:
            plan_str += str(query_tree)
        query_dag = self._create_plan_dag(query_tree, self._arrow_table)
        return f'{plan_str}\nQuery plan:\n {query_dag}'

    def head(self, n: int) -> 'pd.DataFrame':
        """
        Return first n rows of a table as Pandas DataFrame

        Parameters
        ----------
        n : int
            Number of first rows to return.

        Returns
        -------
        :class:`pandas.DataFrame`
        """
        assert n >= 0
        return self._arrow_table.slice(n).get_table().to_pandas()

    @property
    def schema(self):
        """
        Return schema of the table.

        Returns
        -------
        :class:`pyarrow.Schema`
        """
        return self._arrow_table.get_schema()

    def to_arrow(self) -> pa.Table:
        """
        Convert `Table` to 'pyarrow.Table`.

        Returns
        -------
        :class:`pyarrow.Table`
        """
        return self._arrow_table.get_table()

    def to_pandas(self):
        """
        Convert `Table` to 'pandas.DataFrame`.

        Returns
        -------
        :class:`pandas.DataFrame`
        """
        return self._arrow_table.to_pandas()

    def to_string(self) -> str:
        """
        Return string representation of a `Table`.

        Returns
        -------
        str
        """
        return self._arrow_table.get_table().to_string()

    def __str__(self) -> str:
        return self.to_string()
