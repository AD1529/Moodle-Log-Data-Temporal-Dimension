from src.classes.records import Records
import src.algorithms.integrating as it
import src.algorithms.filtering as fl
import src.algorithms.categorisation as ct
import src.algorithms.duration as dt
import src.algorithms.sorting as st


def get_categorical_duration(dataframe_path: str) -> Records:

    # get the dataframe
    dataframe = it.get_dataframe(dataframe_path)
    # before calculating the duration, safely remove events that have no impact on the duration calculation.
    dataframe = fl.safely_remove_records(dataframe)
    # create a Records object to use its methods
    records = Records(dataframe)
    # to calculate the duration, values must be sorted by username and ID
    records = st.sort_records(records, sort_by=['Username', 'ID'])
    # compute the basic duration
    records = dt.get_basic_duration(records)
    # insert the temporal category
    records = ct.insert_temporal_category(records)
    # compute the categorical duration
    records = dt.get_categorical_duration(records)
    # remove unnecessary data from the entire dataset
    records = Records(fl.remove_dataset_records(records.get_df()))

    return records


if __name__ == '__main__':

    FILE_PATH = 'src/datasets/consolidated_df.csv'

    logs = get_categorical_duration(FILE_PATH)

    # you can save the dataset for further analysis
    logs.get_df().to_csv('src/datasets/categorical_df.csv')
