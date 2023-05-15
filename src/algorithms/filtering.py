from pandas import DataFrame


def safely_remove_records(df: DataFrame) -> DataFrame:
    """
    The 'Course activity completion updated' events can be simultaneous, as they are automatic and their duration is 0,
    or instantaneous, when students mark an activity as completed from the course homepage. This means that removing
    them will only affect the amount of time spent on the course's homepage, so they can be safely removed.
    In addition, we do not consider these events because they contain bugs that need to be resolved
    (https://moodle.org/mod/forum/discuss.php?d=391272) and because they are not informative for temporal analysis.
    We also have no interest in actions performed by guests or administrators, therefore we remove all their logs.
   """

    # 'Course activity completion updated' events
    activity_completion = list((df.loc[df['Event_name'] == 'Course activity completion updated']).index)

    # events generated before the user accesses the platform
    failed_login = list((df.loc[df['Event_name'] == 'User login failed']).index)

    # admin, managers, and guest users
    admin = list((df.loc[df['Role'] == 'Admin']).index)
    manager = list((df.loc[df['Role'] == 'Manager']).index)
    guest = list((df.loc[df['Role'] == 'Guest']).index)

    to_remove = admin + manager + guest + failed_login + activity_completion

    df.drop(to_remove, axis=0, inplace=True)
    df = df.reset_index(drop=True)

    return df


def remove_dataset_records(df: DataFrame) -> DataFrame:
    """
    Remove unnecessary data. Here are listed logs that usually are not related to
    learning activities. Moreover, some user logs may be gathered for deleted courses or for courses not listed in the
    course_shortnames file (because you do not want to analyse them). When attempting to match the course id and
    shortname, they are not matched and the course/area field will be empty. This function is customisable according
    to specific needs.

    Please be aware that if you deal with time, and you calculate the duration as the interval between two
    consecutive events, the dataset cleaning should be performed after the duration calculation to avoid biased results.
    """

    # logs not related to learning activities
    logs = list((df.loc[df['Component'] == 'Logs']).index)
    recycle_bin = list((df.loc[df['Component'] == 'Recycle bin']).index)
    report = list((df.loc[df['Component'] == 'Report']).index)
    insights = list((df.loc[df['Event_name'] == 'Insights viewed']).index)

    # actions performed on deleted modules, activities, or courses
    other = list((df.loc[df['Status'] == 'DELETED']).index)

    # actions performed via mobile
    mobile = list((df.loc[df['Component'] == 'Mobile']).index)

    # data whose course is not listed in the course_shortnames file and whose area is absent
    records_left = list((df.loc[df.Course_Area.isnull()]).index)

    # remove remaining admin/manager-related actions that are not specified in the integrating functions
    system = list((df.loc[df['Component'] == 'System']).index)
    login_as = list(df.loc[df['Username'].str.contains(' as ')].index)

    to_remove = logs + recycle_bin + report + insights + other + mobile + records_left + system + login_as
    df.drop(to_remove, axis=0, inplace=True)
    df = df.reset_index(drop=True)

    return df
