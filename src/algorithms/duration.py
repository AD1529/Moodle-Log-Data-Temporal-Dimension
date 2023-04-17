import math

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

    records = Records(df)

    return records


def get_categorical_duration(records: Records) -> Records:

    # compute the basic duration
    records = get_basic_duration(records)

    # get the dataframe
    df = records.get_df()

    starting = df.Category == 'Starting'
    df.loc[starting, 'categorical_duration'] = df.loc[starting]['basic_duration']

    ending = list((df.loc[df.Category == 'Ending']).index)
    for item in ending:
        search_before = item - 1
        duration = df.loc[search_before, 'basic_duration']
        df.loc[item, 'categorical_duration'] = math.ceil(duration/2)
        df.loc[search_before, 'categorical_duration'] = math.floor(duration/2)

    simultaneous = df.Category == 'Simultaneous'
    df.loc[simultaneous, 'categorical_duration'] = 0

    # TODO trovare soluzione a eventi che durano 1 secondo

    closing = list((df.loc[df.Category == 'Closing']).index)
    for item in closing:
        search_before = item - 1
        df.loc[item, 'categorical_duration'] = df.loc[search_before, 'basic_duration']
        df.loc[search_before, 'categorical_duration'] = 0

    instantaneous = list((df.loc[df.Category == 'Instantaneous']).index)
    for item in instantaneous:
        search_before = item - 1
        df.loc[search_before, 'categorical_duration'] = df.loc[search_before, 'basic_duration'] + df.loc[item, 'basic_duration']
        df.loc[item, 'categorical_duration'] = 0

    # set data type
    df['categorical_duration'] = df['categorical_duration'].astype('Int64')

    records = Records(df)

    return records
