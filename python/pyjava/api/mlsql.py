import warnings

from pyjava.utils import _exception_message


class Data(object):

    def __init__(self, iterator):
        self.input_data = iterator
        self.output_data = None
        self.schema = None

    def output(self, value, schema):
        self.output_data = value
        self.schema = schema

    def _output(self):
        if self.output_data is None:
            raise Exception("You should call input_data.output(..) before finally return")
        return self.output_data

    def fetch_once(self):
        for item in self.input_data:
            yield item.to_pydict()

    def to_pandas(self, timezone=None):
        try:
            from pyjava.datatype.types import _check_dataframe_localize_timestamps
            import pyarrow
            import pandas as pd
            batches = self.fetch_once()
            if len(batches) > 0:
                table = pyarrow.Table.from_batches(batches)
                # Pandas DataFrame created from PyArrow uses datetime64[ns] for date type
                # values, but we should use datetime.date to match the behavior with when
                # Arrow optimization is disabled.
                pdf = table.to_pandas(date_as_object=True)
                return _check_dataframe_localize_timestamps(pdf, timezone)
            else:
                return pd.DataFrame.from_records([], columns=[])
        except Exception as e:
            # We might have to allow fallback here as well but multiple Spark jobs can
            # be executed. So, simply fail in this case for now.
            msg = ("toPandas attempted fail\n  %s" % _exception_message(e))
            warnings.warn(msg)
            raise
