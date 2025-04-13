import argparse
import pandas as pd
import pyarrow
import pyarrow.parquet
import tqdm

def count_rows(csv_path: str) -> int:
    """
    Count rows from a given CSV.

    Parameters
    ----------
    csv_path : str
        Path to CSV.

    Returns
    -------
    int
        Numbers of rows inside CSV.
    """
    with open(csv_path, 'r') as f:
        return sum(1 for _ in f) - 1

if __name__ == "__main__":
    # Defining script argument parsing
    parser = argparse.ArgumentParser(description="Script to convert CSV to Apache Parquet format with stream data")
    parser.add_argument(
        '--input',
        required=True,
        help='Path to import CSV',
        type=str
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Path to save parquet',
        type=str
    )
    opts, pipeline_args = parser.parse_known_args()

    # Defining metadata for operation
    chunksize = 100_000 * 6 # n rows / interation
    total_chunks = count_rows(opts.input) // chunksize + 1
    writer = None

    # Iterating rows from CSV
    for row in tqdm.tqdm(
        total=total_chunks,
        iterable=pd.read_csv(
            filepath_or_buffer=opts.input,
            chunksize=chunksize,
            iterator=True,
            encoding='iso8859-1'
        )
    ):
        data = pyarrow.Table.from_pandas(df=row, preserve_index=True, safe=True)
        if writer == None:
            # Create obj to handle stream data
            writer = pyarrow.parquet.ParquetWriter(
                where=opts.output,
                schema=data.schema,
                compression='snappy'
            )
        # Appending data to parquet file
        writer.write_table(table=data)
    writer.close()
