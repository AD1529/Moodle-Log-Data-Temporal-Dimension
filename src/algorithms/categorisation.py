from pandas import DataFrame
from src.classes.records import Records


def insert_temporal_category(records: Records) -> Records:
    """
    Insert the temporal category. This list comprises all the Moodle standard student role-related 'participating'
    and 'other' events. You can add other events according to your needs.
    """

    # get the dataframe
    df = records.get_df()

    # assignment
    assignment = df['Component'] == 'Assignment'
    df.loc[assignment &
           (df['Event_name'] == 'A file has been uploaded.'), 'Category'] = 'Simultaneous'
    df.loc[assignment &
           (df['Event_name'] == 'A submission has been submitted.'), 'Category'] = 'Simultaneous'
    df.loc[assignment &
           (df['Event_name'] == 'An online text has been uploaded.'), 'Category'] = 'Simultaneous'
    df.loc[assignment &
           (df['Event_name'] == 'Comment deleted'), 'Category'] = 'Instantaneous'
    df.loc[assignment &
           (df['Event_name'] == 'Course module instance list viewed'), 'Category'] = 'Starting'
    df.loc[assignment &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Simultaneous'
    df.loc[assignment &
           (df['Event_name'] == 'Feedback viewed'), 'Category'] = 'Simultaneous'
    df.loc[assignment &
           (df['Event_name'] == 'Remove submission confirmation viewed.'), 'Category'] = 'Starting'
    df.loc[assignment &
           (df['Event_name'] == 'Submission confirmation form viewed.'), 'Category'] = 'Starting'

    df.loc[assignment &
           (df['Event_name'] == 'Submission created.'), 'Category'] = 'Ending'
    assignment_sc = list((df.loc[assignment].loc[df['Event_name'] == 'Submission created.']).index)
    for item in assignment_sc:
        search_before = item - 2
        if df.iloc[search_before]['Event_name'] == 'Course module viewed':
            # switch the events to calculate the duration properly
            df.iloc[search_before], df.iloc[item] = df.iloc[item], df.iloc[search_before]

    df.loc[assignment &
           (df['Event_name'] == 'Submission form viewed.'), 'Category'] = 'Starting'

    df.loc[assignment &
           (df['Event_name'] == 'Submission updated.'), 'Category'] = 'Ending'
    assignment_up = list((df.loc[assignment].loc[df['Event_name'] == 'Submission updated.']).index)
    for item in assignment_up:
        search_before = item - 2
        if df.iloc[search_before]['Event_name'] == 'Course module viewed':
            # switch the events to calculate the duration properly
            df.iloc[search_before], df.iloc[item] = df.iloc[item], df.iloc[search_before]

    df.loc[assignment &
           (df['Event_name'] == 'The status of the submission has been updated.'), 'Category'] = 'Simultaneous'

    df.loc[assignment &
           (df['Event_name'] == 'The status of the submission has been viewed.'), 'Category'] = 'Starting'

    df.loc[assignment &
           (df['Event_name'] == 'The user duplicated their submission.'), 'Category'] = 'Ending'
    assignment_uds = list((df.loc[assignment].loc[df['Event_name'] == 'The user duplicated their submission.']).index)
    for item in assignment_uds:
        search_before = item - 1
        if df.iloc[search_before]['Event_name'] == 'Course module viewed':
            # switch the events to calculate the duration properly
            df.iloc[search_before], df.iloc[item] = df.iloc[item], df.iloc[search_before]

    df.loc[(df['Component'] == 'Assignment') &
           (df['Event_name'] == 'Comment created'), 'Category'] = 'Ending'
    assignment_cm = list((df.loc[assignment].loc[df['Event_name'] == 'Comment created']).index)
    for item in assignment_cm:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the ending
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'The status of the submission has been viewed.'
    df = df.sort_index().reset_index(drop=True)

    # attendance
    attendance = df['Component'] == 'Attendance'
    df.loc[attendance &
           (df['Event_name'] == 'Attendance taken by student'), 'Category'] = 'Instantaneous'
    df.loc[attendance &
           (df['Event_name'] == 'Course module instance list viewed'), 'Category'] = 'Starting'
    df.loc[attendance &
           (df['Event_name'] == 'Session report viewed'), 'Category'] = 'Starting'

    # badge
    badge = df['Component'] == 'Badge'
    df.loc[badge &
           (df['Event_name'] == 'Badge listing viewed'), 'Category'] = 'Starting'
    df.loc[badge &
           (df['Event_name'] == 'Badge viewed'), 'Category'] = 'Starting'

    # blog
    df.loc[(df['Component'] == 'Blog') &
           (df['Event_name'] == 'Blog entries viewed'), 'Category'] = 'Starting'

    # book
    book = df['Component'] == 'Book'
    df.loc[book &
           (df['Event_name'] == 'Book printed'), 'Category'] = 'Instantaneous'
    df.loc[book &
           (df['Event_name'] == 'Chapter printed'), 'Category'] = 'Instantaneous'
    df.loc[book &
           (df['Event_name'] == 'Chapter viewed'), 'Category'] = 'Starting'
    df.loc[book &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Simultaneous'

    # calendar
    df.loc[(df['Component'] == 'Calendar') &
           (df['Event_name'] == 'Calendar event created'), 'Category'] = 'Simultaneous'
    calendar_ec = list((df.loc[df['Component'] == 'Calendar'].loc[df['Event_name'] == 'Calendar event created']).index)
    df = convert_calendar_simultaneous_ending(df, calendar_ec)

    df.loc[(df['Component'] == 'Calendar') &
           (df['Event_name'] == 'Calendar event deleted'), 'Category'] = 'Simultaneous'
    calendar_ed = list((df.loc[df['Component'] == 'Calendar'].loc[df['Event_name'] == 'Calendar event deleted']).index)
    df = convert_calendar_simultaneous_ending(df, calendar_ed)

    df.loc[(df['Component'] == 'Calendar') &
           (df['Event_name'] == 'Calendar event updated'), 'Category'] = 'Simultaneous'
    calendar_eu = list((df.loc[df['Component'] == 'Calendar'].loc[df['Event_name'] == 'Calendar event updated']).index)
    df = convert_calendar_simultaneous_ending(df, calendar_eu)

    # chat
    df.loc[(df['Component'] == 'Chat') &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'

    df.loc[(df['Component'] == 'Chat') &
           (df['Event_name'] == 'Message sent'), 'Category'] = 'Ending'
    chat_ms = list((df.loc[df['Component'] == 'Chat'].loc[df['Event_name'] == 'Message sent']).index)
    for item in chat_ms:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the ending
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Course module viewed'
    df = df.sort_index().reset_index(drop=True)

    df.loc[(df['Component'] == 'Chat') &
           (df['Event_name'] == 'Sessions viewed'), 'Category'] = 'Starting'

    # choice
    df.loc[(df['Component'] == 'Choice') &
           (df['Event_name'] == 'Choice answer added'), 'Category'] = 'Closing'
    choice_caa = list((df.loc[df['Component'] == 'Choice'].loc[df['Event_name'] == 'Choice answer added']).index)
    for item in choice_caa:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the closing
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Choice summary viewed'
    df = df.sort_index().reset_index(drop=True)

    df.loc[(df['Component'] == 'Choice') &
           (df['Event_name'] == 'Choice answer deleted'), 'Category'] = 'Closing'
    choice_cad = list((df.loc[df['Component'] == 'Choice'].loc[df['Event_name'] == 'Choice answer deleted']).index)
    for item in choice_cad:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the closing
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Course module viewed'
    df = df.sort_index().reset_index(drop=True)

    df.loc[(df['Component'] == 'Choice') &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    choice_cmv = list((df.loc[df['Component'] == 'Choice'].loc[df['Event_name'] == 'Course module viewed']).index)
    for item in choice_cmv:
        search_next = item + 1
        if search_next < len(df):
            if df.iloc[search_next]['Category'] == 'Closing':
                # indicate the correct category if the next event is closing
                df.loc[item, 'Category'] = 'Opening'

    # contact request
    df.loc[(df['Component'] == 'Contact request') &
           (df['Event_name'] == 'Notification sent'), 'Category'] = 'Instantaneous'

    # course home
    df.loc[(df['Component'] == 'Course home') &
           (df['Event_name'] == 'Course viewed'), 'Category'] = 'Starting'

    # courses list
    courses_list = df['Component'] == 'Courses list'
    df.loc[courses_list &
           (df['Event_name'] == 'Category viewed'), 'Category'] = 'Starting'
    df.loc[courses_list &
           (df['Event_name'] == 'Courses searched'), 'Category'] = 'Starting'

    # dashboard
    dashboard = df['Component'] == 'Dashboard'
    df.loc[dashboard &
           (df['Event_name'] == 'Dashboard reset'), 'Category'] = 'Instantaneous'
    df.loc[dashboard &
           (df['Event_name'] == 'Dashboard viewed'), 'Category'] = 'Starting'

    # database
    df.loc[(df['Component'] == 'Database') &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'

    # feedback
    df.loc[(df['Component'] == 'Feedback') &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    feedback_cmv = list((df.loc[df['Component'] == 'Feedback'].loc[df['Event_name'] == 'Course module viewed']).index)
    for item in feedback_cmv:
        search_next = item + 1
        if search_next < len(df):
            if df.iloc[search_next]['Event_name'] == 'Response submitted':
                # indicate the correct category
                df.loc[item, 'Category'] = 'Opening'

    df.loc[(df['Component'] == 'Feedback') &
           (df['Event_name'] == 'Response submitted'), 'Category'] = 'Closing'
    feedback_rs = list((df.loc[df['Component'] == 'Feedback'].loc[df['Event_name'] == 'Response submitted']).index)
    for item in feedback_rs:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the closing
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Feedback summary viewed'
    df = df.sort_index().reset_index(drop=True)

    # file
    df.loc[(df['Component'] == 'File') &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'

    # folder
    folder = df['Component'] == 'Folder'
    df.loc[folder &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    df.loc[folder &
           (df['Event_name'] == 'Zip archive of folder downloaded'), 'Category'] = 'Instantaneous'

    # forum
    forum = df['Component'] == 'Forum'
    df.loc[forum &
           (df['Event_name'] == 'Course module instance list viewed'), 'Category'] = 'Starting'
    df.loc[forum &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    df.loc[forum &
           (df['Event_name'] == 'Course searched'), 'Category'] = 'Starting'
    df.loc[forum &
           (df['Event_name'] == 'Discussion created'), 'Category'] = 'Simultaneous'
    df.loc[forum &
           (df['Event_name'] == 'Discussion subscription created'), 'Category'] = 'Simultaneous'
    df.loc[forum &
           (df['Event_name'] == 'Discussion subscription deleted'), 'Category'] = 'Simultaneous'
    df.loc[forum &
           (df['Event_name'] == 'Discussion viewed'), 'Category'] = 'Starting'
    df.loc[forum &
           (df['Event_name'] == 'Post deleted'), 'Category'] = 'Instantaneous'
    df.loc[forum &
           (df['Event_name'] == 'Post updated'), 'Category'] = 'Simultaneous'
    df.loc[forum &
           (df['Event_name'] == 'Some content has been posted.'), 'Category'] = 'Ending'
    df.loc[forum &
           (df['Event_name'] == 'Subscription created'), 'Category'] = 'Simultaneous'
    df.loc[forum &
           (df['Event_name'] == 'Subscription deleted'), 'Category'] = 'Simultaneous'
    df.loc[forum &
           (df['Event_name'] == 'User report viewed'), 'Category'] = 'Starting'

    df.loc[(df['Component'] == 'Forum') &
           (df['Event_name'] == 'Post created'), 'Category'] = 'Simultaneous'
    forum_pc = list((df.loc[df['Component'] == 'Forum'].loc[df['Event_name'] == 'Post created']).index)
    for item in forum_pc:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the simultaneous
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Discussion viewed'
    df = df.sort_index().reset_index(drop=True)

    # glossary
    glossary = df['Component'] == 'Glossary'
    df.loc[glossary &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    df.loc[glossary &
           (df['Event_name'] == 'Entry has been viewed'), 'Category'] = 'Starting'

    # grades
    grades = df['Component'] == 'Grades'
    df.loc[grades &
           (df['Event_name'] == 'Course user report viewed'), 'Category'] = 'Starting'
    df.loc[grades &
           (df['Event_name'] == 'Grade overview report viewed'), 'Category'] = 'Starting'
    df.loc[grades &
           (df['Event_name'] == 'Grade user report viewed'), 'Category'] = 'Starting'

    # h5p
    df.loc[(df['Component'] == 'H5P') &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Simultaneous'

    df.loc[(df['Component'] == 'H5P') &
           (df['Event_name'] == 'H5P content viewed'), 'Category'] = 'Starting'
    h5p_cv = list((df.loc[df['Component'] == 'H5P'].loc[df['Event_name'] == 'H5P content viewed']).index)
    for item in h5p_cv:
        search_next = item + 1
        if search_next < len(df):
            if df.iloc[search_next]['Event_name'] == 'xAPI statement received':
                df.loc[item, 'Category'] = 'Opening'

    df.loc[(df['Component'] == 'H5P') &
           (df['Event_name'] == 'Report viewed'), 'Category'] = 'Starting'

    df.loc[(df['Component'] == 'H5P') &
           (df['Event_name'] == 'xAPI statement received'), 'Category'] = 'Closing'
    h5p_sr = list((df.loc[df['Component'] == 'H5P'].loc[df['Event_name'] == 'xAPI statement received']).index)
    for item in h5p_sr:
        search_next = item + 1
        add_next = item + 0.5
        # if after a statement received there is another statement received
        if df.iloc[search_next]['Event_name'] == 'xAPI statement received':
            df.loc[add_next] = df.loc[item]
            # add a new event to represent the starting action
            df.loc[add_next, 'Category'] = 'Opening'
            df.loc[add_next, 'Event_name'] = 'H5P content viewed'
        else:
            df.loc[add_next] = df.loc[item]
            # add a new event to represent the starting action
            df.loc[add_next, 'Category'] = 'Starting'
            df.loc[add_next, 'Event_name'] = 'H5P summary viewed'
    df = df.sort_index().reset_index(drop=True)

    # insights
    df.loc[(df['Component'] == 'System') &
           (df['Event_name'] == 'Insights viewed'), 'Category'] = 'Starting'

    # jitsi
    jitsi = df['Component'] == 'Jitsi'
    df.loc[jitsi &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    df.loc[jitsi &
           (df['Event_name'] == 'Enter to session'), 'Category'] = 'Starting'

    # level up
    df.loc[df['Component'] == 'Level Up XP', 'Category'] = 'Instantaneous'

    # lesson
    lesson = df['Component'] == 'Lesson'
    df.loc[lesson &
           (df['Event_name'] == 'Content page viewed'), 'Category'] = 'Starting'
    df.loc[lesson &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Simultaneous'
    df.loc[lesson &
           (df['Event_name'] == 'Lesson ended'), 'Category'] = 'Starting'
    df.loc[lesson &
           (df['Event_name'] == 'Lesson restarted'), 'Category'] = 'Simultaneous'
    df.loc[lesson &
           (df['Event_name'] == 'Lesson resumed'), 'Category'] = 'Simultaneous'
    df.loc[lesson &
           (df['Event_name'] == 'Lesson started'), 'Category'] = 'Simultaneous'

    df.loc[lesson &
           (df['Event_name'] == 'Question viewed'), 'Category'] = 'Starting'
    lesson_qv = list((df.loc[lesson].loc[df['Event_name'] == 'Question viewed']).index)
    for item in lesson_qv:
        search_next = item + 1
        if search_next < len(df):
            if df.iloc[search_next]['Event_name'] == 'Question answered':
                df.loc[item, 'Category'] = 'Opening'

    df.loc[lesson &
           (df['Event_name'] == 'Question answered'), 'Category'] = 'Closing'
    lesson_qa = list((df.loc[lesson].loc[df['Event_name'] == 'Question answered']).index)
    for item in lesson_qa:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the closing
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Question summary viewed'
    df = df.sort_index().reset_index(drop=True)

    # login
    df.loc[(df['Component'] == 'Login') &
           (df['Event_name'] == 'User has logged in'), 'Category'] = 'Simultaneous'
    login = list((df.loc[df['Component'] == 'Login'].loc[df['Event_name'] == 'User has logged in']).index)
    for item in login:
        search_next = item + 1
        if search_next < len(df):
            # if the users access for the first time, they are requested to update their password
            if df.iloc[search_next]['Event_name'] == 'User password updated':
                df.loc[item, 'Category'] = 'Starting'

    # logout
    df.loc[(df['Component'] == 'Logout') &
           (df['Event_name'] == 'User logged out'), 'Category'] = 'Starting'

    # messaging
    messaging = df['Component'] == 'Messaging'
    df.loc[messaging &
           (df['Event_name'] == 'Message contact added'), 'Category'] = 'Instantaneous'
    df.loc[messaging &
           (df['Event_name'] == 'Message deleted'), 'Category'] = 'Instantaneous'
    df.loc[messaging &
           (df['Event_name'] == 'Message viewed'), 'Category'] = 'Starting'

    df.loc[(df['Component'] == 'Messaging') &
           (df['Event_name'] == 'Group message sent'), 'Category'] = 'Ending'
    messaging_gms = list((df.loc[df['Component'] == 'Messaging'].loc[df['Event_name'] == 'Group message sent']).index)
    for item in messaging_gms:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the ending
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Message viewed'
    df = df.sort_index().reset_index(drop=True)

    df.loc[(df['Component'] == 'Messaging') &
           (df['Event_name'] == 'Message sent'), 'Category'] = 'Ending'
    messaging_ms = list((df.loc[df['Component'] == 'Messaging'].loc[df['Event_name'] == 'Message sent']).index)
    for item in messaging_ms:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the ending
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Message viewed'
    df = df.sort_index().reset_index(drop=True)

    # notification
    df.loc[(df['Component'] == 'Notification') &
           (df['Event_name'] == 'Notification viewed'), 'Category'] = 'Starting'
    # page
    df.loc[(df['Component'] == 'Page') &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'

    # participant profile
    participant_profile = df['Component'] == 'Participant profile'
    df.loc[participant_profile &
           (df['Event_name'] == 'User list viewed'), 'Category'] = 'Starting'
    df.loc[participant_profile &
           (df['Event_name'] == 'User profile viewed'), 'Category'] = 'Starting'

    # quiz
    quiz = df['Component'] == 'Quiz'
    df.loc[quiz &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    df.loc[quiz &
           (df['Event_name'] == 'Quiz attempt reviewed'), 'Category'] = 'Starting'
    df.loc[quiz &
           (df['Event_name'] == 'Quiz attempt started'), 'Category'] = 'Simultaneous'
    df.loc[quiz &
           (df['Event_name'] == 'Quiz attempt submitted'), 'Category'] = 'Simultaneous'
    df.loc[quiz &
           (df['Event_name'] == 'Quiz attempt summary viewed'), 'Category'] = 'Starting'
    df.loc[quiz &
           (df['Event_name'] == 'Quiz attempt viewed'), 'Category'] = 'Starting'

    # reservation
    reservation = df['Component'] == 'Reservation'
    df.loc[reservation &
           (df['Event_name'] == 'Course module instance list viewed'), 'Category'] = 'Starting'
    df.loc[reservation &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'

    df.loc[reservation &
           (df['Event_name'] == 'Reservation request added'), 'Category'] = 'Ending'
    reservation_rra = list((df.loc[reservation].loc[df['Event_name'] == 'Reservation request added']).index)
    for item in reservation_rra:
        search_before = item - 1
        if df.iloc[search_before]['Event_name'] == 'Course module viewed':
            # switch the events to calculate the duration properly
            df.iloc[search_before], df.iloc[item] = df.iloc[item], df.iloc[search_before]

    df.loc[reservation &
           (df['Event_name'] == 'Reservation request cancelled'), 'Category'] = 'Ending'
    reservation_rrc = list((df.loc[reservation].loc[df['Event_name'] == 'Reservation request cancelled']).index)
    for item in reservation_rrc:
        search_before = item - 1
        if df.iloc[search_before]['Event_name'] == 'Course module viewed':
            # switch the events to calculate the duration properly
            df.iloc[search_before], df.iloc[item] = df.iloc[item], df.iloc[search_before]

    # site home
    df.loc[(df['Component'] == 'Site home') &
           (df['Event_name'] == 'Course viewed'), 'Category'] = 'Starting'

    # survey
    survey = df['Component'] == 'Survey'
    df.loc[survey &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    survey_cmv = list((df.loc[survey].loc[df['Event_name'] == 'Course module viewed']).index)
    for item in survey_cmv:
        search_next = item + 1
        if search_next < len(df):
            if df.iloc[search_next]['Event_name'] == 'Survey response submitted':
                df.loc[item, 'Category'] = 'Opening'

    df.loc[survey &
           (df['Event_name'] == 'Survey response submitted'), 'Category'] = 'Closing'
    survey_rs = list((df.loc[survey].loc[df['Event_name'] == 'Survey response submitted']).index)
    for item in survey_rs:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the closing
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'Survey summary viewed'
    df = df.sort_index().reset_index(drop=True)

    # tag
    tag = df['Component'] == 'Tag'
    df.loc[tag &
           (df['Event_name'] == 'Tag added to an item'), 'Category'] = 'Instantaneous'
    df.loc[tag &
           (df['Event_name'] == 'Tag created'), 'Category'] = 'Simultaneous'
    df.loc[tag &
           (df['Event_name'] == 'Tag deleted'), 'Category'] = 'Simultaneous'
    df.loc[tag &
           (df['Event_name'] == 'Tag removed from an item'), 'Category'] = 'Instantaneous'

    # url
    df.loc[(df['Component'] == 'URL') &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'

    # user profile
    df.loc[(df['Component'] == 'User profile') &
           (df['Event_name'] == 'User password updated'), 'Category'] = 'Ending'
    profile_upu = list((df.loc[df['Component'] == 'User profile'].loc[df['Event_name'] == 'User password updated']).index)
    for item in profile_upu:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the ending
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'User profile viewed'
    df = df.sort_index().reset_index(drop=True)

    df.loc[(df['Component'] == 'User profile') &
           (df['Event_name'] == 'User profile viewed'), 'Category'] = 'Starting'

    df.loc[(df['Component'] == 'User profile') &
           (df['Event_name'] == 'User updated'), 'Category'] = 'Ending'
    profile_uu = list(
        (df.loc[df['Component'] == 'User profile'].loc[df['Event_name'] == 'User updated']).index)
    for item in profile_uu:
        add_next = item + 0.5
        df.loc[add_next] = df.loc[item]
        # add a new event to represent the starting action after the ending
        df.loc[add_next, 'Category'] = 'Starting'
        df.loc[add_next, 'Event_name'] = 'User profile viewed'
    df = df.sort_index().reset_index(drop=True)

    # Wiki
    wiki = df['Component'] == 'Wiki'
    df.loc[wiki &
           (df['Event_name'] == 'Comments viewed'), 'Category'] = 'Starting'
    df.loc[wiki &
           (df['Event_name'] == 'Course module viewed'), 'Category'] = 'Starting'
    df.loc[wiki &
           (df['Event_name'] == 'Wiki diff viewed'), 'Category'] = 'Starting'
    df.loc[wiki &
           (df['Event_name'] == 'Wiki history viewed'), 'Category'] = 'Starting'

    df.loc[wiki &
           (df['Event_name'] == 'Wiki page created'), 'Category'] = 'Starting'
    wiki_wpc = list((df.loc[wiki].loc[df['Event_name'] == 'Wiki page created']).index)
    for item in wiki_wpc:
        search_next = item + 1
        if search_next < len(df):
            if df.iloc[search_next]['Event_name'] == 'Wiki page updated':
                # indicate the correct category
                df.loc[item, 'Category'] = 'Opening'

    df.loc[wiki &
           (df['Event_name'] == 'Wiki page locks deleted'), 'Category'] = 'Simultaneous'
    df.loc[wiki &
           (df['Event_name'] == 'Wiki page map viewed'), 'Category'] = 'Starting'

    df.loc[wiki &
           (df['Event_name'] == 'Wiki page updated'), 'Category'] = 'Ending'
    wiki_wpu = list((df.loc[wiki].loc[df['Event_name'] == 'Wiki page updated']).index)
    for item in wiki_wpu:
        search_before = item - 1
        if df.iloc[search_before]['Event_name'] == 'Wiki page created':
            # indicate the correct category
            df.loc[item, 'Category'] = 'Closing'

    df.loc[wiki &
           (df['Event_name'] == 'Wiki page version viewed'), 'Category'] = 'Starting'
    df.loc[wiki &
           (df['Event_name'] == 'Wiki page viewed'), 'Category'] = 'Starting'

    df = df.reset_index(drop=True)

    records = Records(df)

    return records


def convert_calendar_simultaneous_ending(df: DataFrame, events_index) -> DataFrame:
    """
    Convert 'calendar' simultaneous events to ending events.
    """

    for item in events_index:
        search_next = item + 1
        if search_next < len(df):
            if df.iloc[search_next]['Unix_Time'] != df.iloc[item]['Unix_Time'] and \
                    df.iloc[search_next]['Unix_Time'] != (df.iloc[item]['Unix_Time'] + 1) and \
                    df.iloc[search_next]['Event_name'] != df.iloc[item]['Event_name']:

                df.loc[item, 'Category'] = 'Ending'
                add_next = item + 0.5
                df.loc[add_next] = df.loc[item]
                # add a new event to represent the starting action after the ending
                df.loc[add_next, 'Category'] = 'Starting'
                df.loc[add_next, 'Event_name'] = 'Calendar viewed'

    df = df.sort_index().reset_index(drop=True)

    return df
