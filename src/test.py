import pandas as pd
from src.classes.records import Records
import src.algorithms.cleaning as cl
import src.algorithms.categorisation as ct
import src.algorithms.duration as dt
import src.algorithms.extracting as ex
import src.algorithms.sorting as st

# get the dataframe
file_path = 'datasets/df_consolidated.csv'
df = pd.read_csv(file_path, sep=',', index_col=0)

# create a Records object to use its methods
records = Records(df)
# to calculate the duration, values must be sorted by username and ID
records = st.sort_records(records, sort_by=['Username', 'ID'])
# insert the temporal category
records = ct.insert_temporal_category(records)
# compute the categorical duration before the extraction
records = dt.get_categorical_duration(records)

# extract student records
student_records = ex.extract_records(records, role=['Student'])
# remove useless data from the entire dataset
df = cl.clean_dataset_records(student_records.get_df())

# compute the estimated duration
# student_records = dt.get_estimated_duration(student_records) #TO BE ADDED
