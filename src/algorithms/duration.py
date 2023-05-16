import math
from src.classes.records import Records


def get_basic_duration(records: Records,
                       duration_field: str = 'basic_duration') -> Records:
    """
    Calculate the duration as the simple difference between two consecutive timestamps and add the values as a new
    column to the dataframe.

    Args:
        records: the object of the classe records
        duration_field: name of the dataframe duration column

    Returns:
        Records with the basic duration
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

    # reset index
    df = df.reset_index(drop=True)

    # set data type
    df[duration_field] = df[duration_field].astype('int64')

    records = Records(df)

    return records


def get_categorical_duration(records: Records,
                             basic_duration_field: str = 'basic_duration',
                             categorical_duration_field: str = 'categorical_duration') -> Records:
    """
    Calculate the duration according to the different categories. Please be aware that sometimes simultaneous events
    can last 1 seconds rather than 0. This happens because we are utilising a one-second granularity Unix timestamp
    value. Looking at the timestamps with a granularity per millisecond, they are not truly simultaneous. As a result,
    it is possible for one action to be recorded in the millisecond of one second and another in the next second. To
    preserve this information, the duration value is assigned to the first event that is not simultaneous. Please be
    aware that the order of computation should be as follows: starting, simultaneous, instantaneous, closing, ending.

    Args:
        records: the object of the classe records
        basic_duration_field: name of the basic duration column
        categorical_duration_field: name of the categorical duration column

    Returns:
        Records with the categorical duration
    """

    # get the dataframe
    df = records.get_df()

    # ensure the index is correctly sorted
    df = df.reset_index(drop=True)

    # starting
    starting = df.Category == 'Starting'
    # keep the basic duration
    df.loc[starting, categorical_duration_field] = df.loc[starting][basic_duration_field]

    # simultaneous
    simultaneous = list((df.loc[df.Category == 'Simultaneous']).index)
    # in case a simultaneous events last more than 0 seconds (for millisecond recording)
    for item in simultaneous:
        search_next = item + 1
        if search_next < len(df):
            # if the following record is simultaneous
            if df.iloc[search_next]['Category'] == 'Simultaneous':
                df.loc[search_next, basic_duration_field] = df.loc[item, basic_duration_field]
            else:
                # assign the duration value to the following record
                df.loc[search_next, categorical_duration_field] = \
                    df.loc[search_next, basic_duration_field] + df.loc[item, basic_duration_field]

    # takes the duration of the simultaneous record to 0
    df.loc[df.Category == 'Simultaneous', categorical_duration_field] = 0

    # instantaneous
    instantaneous = list((df.loc[df.Category == 'Instantaneous']).index)
    for item in instantaneous:
        search_before = item - 1
        # assign the duration value of the instantaneous record to the previous record
        df.loc[search_before, categorical_duration_field] = \
            df.loc[search_before, basic_duration_field] + df.loc[item, basic_duration_field]
        # take the duration of the instantaneous record to 0
        df.loc[item, categorical_duration_field] = 0

    # closing
    closing = list((df.loc[df.Category == 'Closing']).index)
    for item in closing:
        search_before = item - 1
        # assign the duration value of the opening record to the closing record
        df.loc[item, categorical_duration_field] = df.loc[search_before, basic_duration_field]
        # take the duration of the opening record to 0
        df.loc[search_before, categorical_duration_field] = 0

    # ending
    ending = list((df.loc[df.Category == 'Ending']).index)
    for item in ending:
        search_before = item - 1
        # find the duration value
        duration = df.loc[search_before, basic_duration_field]
        # assign half of the duration to the starting event
        df.loc[item, categorical_duration_field] = math.ceil(duration / 2)
        # assign half of the duration to the ending event
        df.loc[search_before, categorical_duration_field] = math.floor(duration / 2)

    
    # set data type
    df['categorical_duration'] = df['categorical_duration'].astype('Int64')
    
    records = Records(df)

    return records
