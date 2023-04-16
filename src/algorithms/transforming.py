from pandas import DataFrame
import src.algorithms.timing as tm


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename the colum names.

    Args:
        df: The dataframe object.

    Returns:
        The dataframe with renamed columns.

    """

    # rename columns
    df.rename(columns={'User full name': 'Username',
                       'Affected user': 'Affected_user',
                       'Event context': 'Event_context',
                       'Event name': 'Event_name',
                       'IP address': 'IP_address',
                       'id': 'ID',
                       'timecreated': 'Unix_Time'},
              inplace=True)

    return df


def set_data_types(df: DataFrame) -> DataFrame:
    """
    Set the data types.

    Args:
        df: The dataframe object.

    Returns:
        The dataframe with the data types set.

    """

    # set data types
    df['Time'] = df['Time'].astype('str')
    df['Username'] = df['Username'].astype('str')
    df['Affected_user'] = df['Affected_user'].astype('str')
    df['Event_context'] = df['Event_context'].astype('str')
    df['Component'] = df['Component'].astype('str')
    df['Event_name'] = df['Event_name'].astype('str')
    df['Description'] = df['Description'].astype('str')
    df['Origin'] = df['Origin'].astype('str')
    df['IP_address'] = df['IP_address'].astype('str')
    df['ID'] = df['ID'].astype('Int64')
    df['userid'] = df['userid'].astype('Int64')
    df['courseid'] = df['courseid'].astype('Int64')
    df['relateduserid'] = df['relateduserid'].astype('str')
    df['Unix_Time'] = df['Unix_Time'].astype('Int64')

    return df


def make_timestamp_readable(df: DataFrame) -> DataFrame:
    """
    Transform the timestamps in standard format.

    Args:
        df: The dataframe object.

    Returns:
        The dataframe with a new column 'Time'.

    """

    df['Time'] = df.loc[:, 'Unix_Time'].map(lambda x: tm.convert_timestamp_to_time(x))

    return df
