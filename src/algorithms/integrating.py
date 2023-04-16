import pandas as pd
from pandas import DataFrame


def get_dataframe(file_path: str, columns: [] = None) -> DataFrame:
    """
    Read the dataframe and add columns if missing.

    Args:
        file_path: str,
            The path of the dataframe object.
        columns: list,
            The list of column names.

    Returns:
        The dataframe with column names.

    """

    df = pd.read_csv(file_path, sep=',')

    # add column names if missing
    try:
        value_type = int(df.columns[0])
        if isinstance(value_type, int):
            df = pd.read_csv(file_path, sep=',', header=None)
            df.columns = columns
    except ValueError:
        pass

    return df
