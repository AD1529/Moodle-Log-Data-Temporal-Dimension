from datetime import datetime
import numpy as np


def convert_timestamp_to_time(date: int or np.int64) -> str:
    """
    Convert a Unix timestamp to the corresponding string date.

    Args:
        date: int,
            Unix time value (e.g. 1670599154).

    Returns:
        The string format of the date.
    """

    # convert timestamp to a date and time string format ("%Y/%m/%d, %H:%M:%S, %Z")
    timestamp_to_date = datetime.fromtimestamp(date).strftime("%d/%m/%Y, %X")

    return timestamp_to_date
