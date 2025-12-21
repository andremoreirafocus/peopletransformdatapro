import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO


def flattened_records_to_parquet_buffer(flattened_records):
    """
    Converts a list of flattened records (dicts) to a Parquet file in a BytesIO buffer.
    Returns the buffer.
    """
    df = pd.DataFrame(flattened_records)
    print(df)
    table = pa.Table.from_pandas(df)
    out_buffer = BytesIO()
    pq.write_table(table, out_buffer)
    out_buffer.seek(0)
    return out_buffer
