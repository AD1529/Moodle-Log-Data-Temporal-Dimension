from src.classes.records import Records
import numpy as np


def get_basic_duration(records: Records,
                       duration_field: str = 'basic_duration') -> Records:
    """
    Calculate the duration as the simple difference between two consecutive timestamps and add the values as a new
    column to the dataframe.

    Args:
        records: the class of records to analyse
        duration_field: name of the dataframe duration column

    Returns:
        Records which include the duration values
    """

    # get the dataframe
    df = records.get_df()

    # calculate the difference
    duration = list(abs(df['Unix_Time'].diff()))

    # moves the first value (= nan) to the end
    duration += [duration.pop(0)]

    # add the duration values to the specified field in the dataframe
    df[duration_field] = duration

    # remove the last record for each user since the last duration is always nonexistent
    for user in records.get_usernames():
        idx = df.loc[df.Username == user].index[-1]
        df.drop(idx, inplace=True)

    # covert the values to int in the df
    df[duration_field] = [np.int64(d) for d in df[duration_field]]

    return records


def get_categorical_duration(records: Records):

    # compute the basic duration
    records = get_basic_duration(records)

    # TODO get_category_duration

    return records
