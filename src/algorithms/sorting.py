from src.classes.records import Records
from typing import Any


def sort_records(records: Records,
                 sort_by: str or [str],
                 ascending: bool or [bool] = True,
                 inplace: Any = True) -> Records:
    """
    Sort values based on the given fields.

    Args:
        records: object of the class Records
        sort_by: dataframe field or list of dataframe field to sort by
        ascending: sorting can be done any way: ascending or descending. List specifications for various sorting
        inplace: perform the sorting in-place if True

    Returns:
        Sorted object of the class Records
    """

    # get the dataframe
    df = records.get_df().copy()

    # sort values
    df.sort_values(by=sort_by, ascending=ascending, inplace=inplace)
    # reset index
    df = df.reset_index(drop=True)

    records = Records(df)

    return records
