import pyarrow.csv
import pyarrow.json
import pyarrow.parquet

from vinum.api.stream_reader import StreamReader
from vinum.api.table import Table


def stream_csv(input_file, read_options=None, parse_options=None,
               convert_options=None):
    """
    stream_csv(input_file, read_options=None, parse_options=None, convert_options=None)

        Open a streaming reader of CSV data.

        Reading using this function is always single-threaded.

        This function is a thin convenience wrapper around
        ``pyarrow.csv.open_csv``, which returns ``StreamReader``.


        Parameters
        ----------
        input_file: string, path or file-like object
            The location of CSV data.  If a string or path, and if it ends
            with a recognized compressed file extension (e.g. ".gz" or ".bz2"),
            the data is automatically decompressed when reading.
        read_options: pyarrow.csv.ReadOptions, optional
            Options for the CSV reader (see pyarrow.csv.ReadOptions constructor
            for defaults)
        parse_options: pyarrow.csv.ParseOptions, optional
            Options for the CSV parser
            (see pyarrow.csv.ParseOptions constructor for defaults)
        convert_options: pyarrow.csv.ConvertOptions, optional
            Options for converting CSV data
            (see pyarrow.csv.ConvertOptions constructor for defaults)

        Returns
        -------
        :class:`StreamReader`

        Examples
        --------
        Run aggregation query on a csv stream:

        >>> import vinum as vn
        >>> query = 'select passenger_count pc, count(*) from t group by pc'
        >>> vn.stream_csv('taxi.csv').sql(query).to_pandas()
           pc  count
        0   0    165
        1   5   3453
        2   6    989
        3   1  34808
        4   2   7386
        5   3   2183
        6   4   1016
    """
    return StreamReader(
        pyarrow.csv.open_csv(input_file, read_options,
                             parse_options, convert_options)
    )


def read_csv(input_file, read_options=None, parse_options=None,
             convert_options=None, memory_pool=None) -> Table:
    """
    Read a `Table` from a stream of CSV data.

    This function is a thin convenience wrapper around
    ``pyarrow.csv.read_csv``, which returns ``Table``.

    Parameters
    ----------
    input_file: string, path or file-like object
        The location of CSV data.  If a string or path, and if it ends
        with a recognized compressed file extension (e.g. ".gz" or ".bz2"),
        the data is automatically decompressed when reading.
    read_options: pyarrow.csv.ReadOptions, optional
        Options for the CSV reader (see pyarrow.csv.ReadOptions constructor
        for defaults)
    parse_options: pyarrow.csv.ParseOptions, optional
        Options for the CSV parser
        (see pyarrow.csv.ParseOptions constructor for defaults)
    convert_options: pyarrow.csv.ConvertOptions, optional
        Options for converting CSV data
        (see pyarrow.csv.ConvertOptions constructor for defaults)
    memory_pool: MemoryPool, optional
        Pool to allocate Table memory from

    Returns
    -------
    :class:`vinum.Table`
        Vinum Table instance.

    Examples
    --------
    >>> import vinum as vn
    >>> tbl = vn.read_csv('taxi.csv')
    >>> res_tbl = tbl.sql('select key, fare_amount from t limit 3')
    >>> res_tbl.to_pandas()
                                key  fare_amount
    0   2009-06-15 17:26:21.0000001          4.5
    1   2010-01-05 16:52:16.0000002         16.9
    2  2011-08-18 00:35:00.00000049          5.7
    """
    table = pyarrow.csv.read_csv(input_file, read_options, parse_options,
                                 convert_options, memory_pool)
    return Table(table)


def read_json(input_file, read_options=None, parse_options=None,
              memory_pool=None) -> Table:
    """
    Read a `Table` from a stream of JSON data.
    This function is a thin convenience wrapper
    around ``pyarrow.csv.read_json`` which returns ``Table``.

    Parameters
    ----------
    input_file: string, path or file-like object
        The location of JSON data. Currently only the line-delimited JSON
        format is supported.
    read_options: pyarrow.json.ReadOptions, optional
        Options for the JSON reader (see ReadOptions constructor for defaults)
    parse_options: pyarrow.json.ParseOptions, optional
        Options for the JSON parser
        (see ParseOptions constructor for defaults)
    memory_pool: MemoryPool, optional
        Pool to allocate Table memory from

    Returns
    -------
    :class:`vinum.Table`
        Vinum Table instance.

    Examples
    --------
    >>> import vinum as vn
    >>> tbl = vn.read_json('test_data.json')
    >>> tbl.sql_pd('select * from t limit 3')
       id  origin    destination    fare
    0   1  London  San Francisco  1348.1
    1   2  Berlin         London   256.3
    2   3  Munich         Malaga   421.7
    """
    table = pyarrow.json.read_json(input_file, read_options, parse_options,
                                   memory_pool)
    return Table(table)


def read_parquet(source, columns=None, use_threads=True, metadata=None,
                 use_pandas_metadata=False, memory_map=False,
                 read_dictionary=None, filesystem=None, filters=None,
                 buffer_size=0, partitioning="hive",
                 use_legacy_dataset=True) -> Table:
    """
    Read a `Table` from Parquet format.
    This function is a thin convenience wrapper around
    ``pyarrow.parquet.read_table``, which returns ``Table``.

    Parameters
    ----------
    source: str, pyarrow.NativeFile, or file-like object
        If a string passed, can be a single file name or directory name. For
        file-like objects, only read a single file. Use pyarrow.BufferReader to
        read a file contained in a bytes or buffer-like object.
    columns: list
        If not None, only these columns will be read from the file. A column
        name may be a prefix of a nested field, e.g. 'a' will select 'a.b',
        'a.c', and 'a.d.e'.
    use_threads : bool, default True
        Perform multi-threaded column reads.
    metadata : FileMetaData
        If separately computed
    read_dictionary : list, default None
        List of names or column paths (for nested types) to read directly
        as DictionaryArray. Only supported for BYTE_ARRAY storage. To read
        a flat column as dictionary-encoded pass the column name. For
        nested types, you must pass the full column "path", which could be
        something like level1.level2.list.item. Refer to the Parquet
        file's schema to obtain the paths.
    memory_map : bool, default False
        If the source is a file path, use a memory map to read file, which can
        improve performance in some environments.
    buffer_size : int, default 0
        If positive, perform read buffering when deserializing individual
        column chunks. Otherwise IO calls are unbuffered.
    partitioning : Partitioning or str or list of str, default "hive"
        The partitioning scheme for a partitioned dataset. The default of
        "hive" assumes directory names with key=value pairs like
        "/year=2009/month=11".
        In addition, a scheme like "/2009/11" is also supported, in which case
        you need to specify the field names or a full schema. See the
        ``pyarrow.dataset.partitioning()`` function for more details.
    use_pandas_metadata : bool, default False
        If True and file has custom pandas schema metadata, ensure that
        index columns are also loaded
    use_legacy_dataset : bool, default True
        Set to False to enable the new code path (experimental, using the
        new Arrow Dataset API). Among other things, this allows to pass
        `filters` for all columns and not only the partition keys, enables
        different partitioning schemes, etc.
    filesystem : FileSystem, default None
        If nothing passed, paths assumed to be found in the local on-disk
        filesystem.
    filters : List[Tuple] or List[List[Tuple]] or None (default)
        Rows which do not match the filter predicate will be removed from
        scanned data. Partition keys embedded in a nested directory structure
        will be exploited to avoid loading files at all if they contain no
        matching rows.
        If `use_legacy_dataset` is True, filters can only reference partition
        keys and only a hive-style directory structure is supported. When
        setting `use_legacy_dataset` to False, also within-file level filtering
        and different partitioning schemes are supported.
        Predicates are expressed in disjunctive normal form (DNF), like
        ``[[('x', '=', 0), ...], ...]``. DNF allows arbitrary boolean logical
        combinations of single column predicates. The innermost tuples each
        describe a single column predicate. The list of inner predicates is
        interpreted as a conjunction (AND), forming a more selective and
        multiple column predicate. Finally, the most outer list combines these
        filters as a disjunction (OR).
        Predicates may also be passed as List[Tuple]. This form is interpreted
        as a single conjunction. To express OR in predicates, one must
        use the (preferred) List[List[Tuple]] notation.

    Returns
    -------
    :class:`vinum.Table`
        Vinum Table instance.

    Examples
    --------
    >>> import vinum as vn
    >>> tbl = vn.read_parquet('users.parquet')
    >>> tbl.sql_pd('select * from t limit 3')
        registration_dttm  id first_name  ...      usage                title
    0 2016-02-03 07:55:29   1     Amanda  ...   49756.53     Internal Auditor
    1 2016-02-03 17:04:03   2     Albert  ...  150280.17        Accountant IV
    2 2016-02-03 01:09:31   3     Evelyn  ...  144972.51  Structural Engineer

    [3 rows x 13 columns]
    """
    table = pyarrow.parquet.read_table(source, columns, use_threads, metadata,
                                       use_pandas_metadata, memory_map,
                                       read_dictionary, filesystem, filters,
                                       buffer_size, partitioning,
                                       use_legacy_dataset)
    return Table(table)
