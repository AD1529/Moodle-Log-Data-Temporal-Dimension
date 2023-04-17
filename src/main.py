import src.algorithms.integrating as it
import src.algorithms.cleaning as cl
from pandas import DataFrame

if __name__ == '__main__':

    from src.paths import *

    # remove useless data from the entire dataset
    # df = cl.clean_dataset_records(df)

    # you can save the dataset for further analysis
    # df.to_csv('datasets/df_consolidated.csv')
