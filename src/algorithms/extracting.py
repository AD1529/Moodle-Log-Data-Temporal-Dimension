from src.classes.records import Records
import src.algorithms.integrating as it
from pandas import DataFrame


def get_startdate_enddate(df: DataFrame, course_dates: str, courses: [str], years: [int]):
    """
    Remove values that don't fall within the start and end dates of the course.

    Query to extract database data:
        SELECT shortname, startdate, enddate
        FROM mdl_course
        where id <> 1

    Args:
        df: The dataframe object.
        course_dates: str,
            The path of the data extracted from the database.
        courses: list of str,
            The courses whose data are removed.
        years: list of int,
            The years of the courses.

    Returns:
        The courses' values that fall within the start and end dates.

    """

    course_dates = it.get_dataframe(course_dates, columns=['id', 'shortname', 'startdate', 'enddate'])

    for course in courses:
        for year in years:
            shortname = course + '_' + str(year)
            startdate = course_dates.loc[course_dates['shortname'] == shortname, 'startdate'].values[0]
            enddate = course_dates.loc[course_dates['shortname'] == shortname, 'enddate'].values[0]
            to_remove = (df.loc[(df.Course_Area == course) & (df.Year == year) &
                                (df.Unix_Time < startdate) | (df.Unix_Time > enddate)]).index
            df.drop(to_remove, axis=0, inplace=True)

    return df


def extract_records(records: Records,
                    year: [int] = None,
                    course_area: [str] = None,
                    role: [str] = None,
                    username: [str] = None,
                    course_dates: str = "") -> Records:

    """
    Return the filtered records by year, course_area, role and/or username and sorted by the specified field.

    Args:
        records: Records,
            The object of the class Records.
        year: list of int,
            Year of the course/area.
        course_area: list of str,
            Course(s) or area(s) of the platform.
        role: list of str,
            'Student', 'Teacher', 'Manager', etc.
        username: list of str,
            The username of the user.
        course_dates: str,
            The path of the data extracted from the database.

    Returns:
        The object of the class Records.
    """

    # attributes to filter
    filters = dict([('Year', year),
                    ('Course_Area', course_area),
                    ('Role', role),
                    ('Username', username)
                    ])

    # columns in the dataframe
    columns = [i for i in filters if filters[i] is not None]

    # get the df
    df = records.get_df()

    for column in columns:
        # for each column filter the values
        df = df.loc[df[column].isin(filters.get(column))]

    # get only the values between start_date and end_date
    if course_dates != "" and course_area is not None:
        df = get_startdate_enddate(df, course_dates, course_area, year)

    # create a Records object for the extracted values
    records = Records(df)

    return records
