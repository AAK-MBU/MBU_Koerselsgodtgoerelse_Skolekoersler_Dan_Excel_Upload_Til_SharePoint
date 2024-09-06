"""
This module provides functionalities to export pandas DataFrames to an Excel file. 
It uses the openpyxl library to either append data to an existing sheet or create 
a new sheet if the file does not exist.
"""

import os
import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows


def export_to_excel(filepath, sheetname, dataframe_data, add_columns=None, remove_columns=None, move_columns_to_last=None):
    """
    Exports a pandas DataFrame to an Excel file. If the file exists, it appends the data to the specified sheet.
    If the file does not exist, it creates a new Excel file with the data.

    Args:
        filepath (str): The path to the Excel file.
        sheetname (str): The name of the sheet to append the data to.
        dataframe_data (pd.DataFrame): The pandas DataFrame containing the data to export.
        add_columns (dict, optional): Dictionary of columns to add, where keys are column names and values are the data for the columns.
        remove_columns (list, optional): List of column names to remove from the DataFrame.
        move_column_to_last (str, optional): Column name to move to the last position.

    Raises:
        ValueError: If the sheet name does not exist in the existing workbook or if the lengths of add_columns values do not match the DataFrame length.
    """
    if add_columns:
        for col_name, col_data in add_columns.items():
            if len(col_data) == 0:
                col_data = [None] * len(dataframe_data)
            if len(col_data) != len(dataframe_data):
                raise ValueError(f"Length of values for column '{col_name}' ({len(col_data)}) does not match length of DataFrame ({len(dataframe_data)}).")
            dataframe_data[col_name] = col_data

    if remove_columns:
        dataframe_data.drop(columns=remove_columns, inplace=True)

    if move_columns_to_last:
        for col in move_columns_to_last:
            if col in dataframe_data.columns:
                cols = list(dataframe_data.columns)
                cols.append(cols.pop(cols.index(col)))
                dataframe_data = dataframe_data[cols]
            else:
                raise ValueError(f"The column '{col}' does not exist in the DataFrame.")

    if os.path.isfile(filepath):
        workbook = openpyxl.load_workbook(filepath)
        if sheetname not in workbook.sheetnames:
            raise ValueError(f"The sheet name '{sheetname}' does not exist in the workbook.")
        sheet = workbook[sheetname]
        for row in dataframe_to_rows(dataframe_data, header=False, index=False):
            row = [str(cell) if cell is not None else "" for cell in row]
            sheet.append(row)
        workbook.save(filepath)
        workbook.close()
    else:
        with pd.ExcelWriter(path=filepath, engine='openpyxl') as writer:
            dataframe_data.to_excel(writer, index=False, sheet_name=sheetname)
