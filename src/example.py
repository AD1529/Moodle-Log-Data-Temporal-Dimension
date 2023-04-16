from src.classes.records import Records
import src.algorithms.cleaning as cl
import src.algorithms.extracting as ex
import pandas as pd
from src.paths import course_dates_path

# ------------
# GET DATA
# ------------
# get the consolidated dataframe
df_path = 'datasets/df_consolidated.csv'
df = pd.read_csv(df_path)

# create a Records object to use its methods
records = Records(df)

# ----------------------
# GET COURSES TO ANALYSE
# ----------------------
# select specific attributes to get the desired values
course_A = ex.extract_records(records, course_area=['Course_A'], role=['Student'], course_dates=course_dates_path)
course_B = ex.extract_records(records, year=[2021], username=['User 43'])

# -----------------
# CLEAN THE DATASET
# -----------------
# you can either clean the entire dataset or each course individually
# records = cl.clean_specific_records(records)
course_A = cl.clean_specific_records(course_A)
