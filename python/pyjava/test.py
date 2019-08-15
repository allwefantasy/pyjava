import pandas as pd
import pyarrow as pa
xrange = range



def _create_batch(series):
    """
    Create an Arrow record batch from the given pandas.Series or list of Series,
    with optional type.

    :param series: A single pandas.Series, list of Series, or list of (series, arrow_type)
    :return: Arrow RecordBatch
    """
    _assign_cols_by_name = True
    _safecheck= True
    from pyjava.datatype.types import _check_series_convert_timestamps_internal
    # Make input conform to [(series1, type1), (series2, type2), ...]
    if not isinstance(series, (list, tuple)) or \
            (len(series) == 2 and isinstance(series[1], pa.DataType)):
        series = [series]
    series = ((s, None) if not isinstance(s, (list, tuple)) else s for s in series)
    print(series)

    def create_array(s, t):
        mask = s.isnull()
        # Ensure timestamp series are in expected form for Spark internal representation
        if t is not None and pa.types.is_timestamp(t):
            s = _check_series_convert_timestamps_internal(s, None)
        try:
            array = pa.Array.from_pandas(s, mask=mask, type=t, safe=_safecheck)
        except pa.ArrowException as e:
            error_msg = "Exception thrown when converting pandas.Series (%s) to Arrow " + \
                        "Array (%s). It can be caused by overflows or other unsafe " + \
                        "conversions warned by Arrow. Arrow safe type check can be " + \
                        "disabled by using SQL config " + \
                        "`spark.sql.execution.pandas.arrowSafeTypeConversion`."
            raise RuntimeError(error_msg % (s.dtype, t), e)
        return array

    arrs = []
    for s, t in series:
        if t is not None and pa.types.is_struct(t):
            if not isinstance(s, pd.DataFrame):
                raise ValueError("A field of type StructType expects a pandas.DataFrame, "
                                 "but got: %s" % str(type(s)))

            # Input partition and result pandas.DataFrame empty, make empty Arrays with struct
            if len(s) == 0 and len(s.columns) == 0:
                arrs_names = [(pa.array([], type=field.type), field.name) for field in t]
            # Assign result columns by schema name if user labeled with strings
            elif _assign_cols_by_name and any(isinstance(name, basestring)
                                                   for name in s.columns):
                arrs_names = [(create_array(s[field.name], field.type), field.name)
                              for field in t]
            # Assign result columns by  position
            else:
                arrs_names = [(create_array(s[s.columns[i]], field.type), field.name)
                              for i, field in enumerate(t)]

            struct_arrs, struct_names = zip(*arrs_names)
            arrs.append(pa.StructArray.from_arrays(struct_arrs, struct_names))
        else:
            arrs.append(create_array(s, t))
    return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in xrange(len(arrs))])


df = pd.DataFrame({'AAA': [4, 5, 6, 7], 'BBB': [10, 20, 30, 40], 'CCC': [100, 50, -30, -50]})
iterator = [[df['AAA'], df['BBB']]]
batches = (_create_batch(series) for series in iterator)
for item in batches:
    print(item[0])
