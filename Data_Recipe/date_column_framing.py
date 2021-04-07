"""
This recipe creates cross join (catesian product) of data column and key column to populate missing rows.
This works for time interval of day, week, month.

This recipe is for regression probelm use case; hence, the permutation method for target column is filling 0.

Specify date column, key column, target column, and time interval.
"""
from typing import Union, List
from h2oaicore.data import CustomData
import datatable as dt
import numpy as np
import pandas as pd

# User input for setting date column and key column
date_col_input = '日付'
key_col_input = '商品グループ'
target_col_input = '合計 / 補正販売数量'
# User input for date interval specification: Day = 'D', Week = 'W', Begining of Month = 'MS'
date_interval ='D'

class DateColumnFraming(CustomData):

    @staticmethod
    def create_data(X: dt.Frame = None) -> Union[str, List[str],
                                                 dt.Frame, List[dt.Frame],
                                                 np.ndarray, List[np.ndarray],
                                                 pd.DataFrame, List[pd.DataFrame]]:
        # exit gracefully if method is called as a data upload rather than data modify

        if X is None:
            return []

        X = dt.Frame(X).to_pandas()

        # Convert date column into date format
        X[date_col_input] = pd.to_datetime(X[date_col_input])

        # Extract start date and end date
        start_date = X[date_col_input].min()
        end_date = X[date_col_input].max()

         # Create full date list
        full_date_list = pd.date_range(start_date, end_date, freq = date_interval)
        full_date_list  = pd.DataFrame(full_date_list).rename(columns={0: date_col_input})

        # Create unique key value list
        key_column = X[key_col_input]
        unique_key_column = pd.DataFrame(key_column.unique()).rename(columns={0: key_col_input})

        # Take cross join of full date list and key values
        full_date_list['join_key'] =1
        unique_key_column['join_key'] =1
        joined_table = pd.merge(unique_key_column, full_date_list, on = 'join_key', how = 'outer')
        joined_table = joined_table.drop(['join_key'], axis = 1)

        # Left join of Original data on cross joined table
        X = pd.merge(joined_table, X, on = [key_col_input, date_col_input], how = 'left')

        # Fill 0 to target column
        X[target_col_input] =  X[target_col_input].fillna(0)
        return dt.Frame(X)
