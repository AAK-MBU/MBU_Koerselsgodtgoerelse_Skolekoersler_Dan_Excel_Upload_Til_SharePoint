"""This module contains the main process of the robot."""
import os
import json
import shutil
from datetime import datetime, timedelta
import locale
import pandas as pd
import pyodbc
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint
from robot_framework.sub_processes.excel import export_to_excel
from robot_framework import config


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    oc_args_json = json.loads(orchestrator_connection.process_arguments)
    creds = orchestrator_connection.get_credential(config.USERNAME)

    temp_path = oc_args_json['tempPath']
    conn_str = orchestrator_connection.get_constant('DbConnectionString').value
    
    orchestrator_connection.log_trace("Create tmp-folder.")
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    orchestrator_connection.log_trace("Export data from hub in SQL database.")
    file = export_egenbefordring_from_hub(conn_str, temp_path)

    orchestrator_connection.log_trace(f"Upload file to sharepoint: {file}")
    upload_file_to_sharepoint(config.FOLDER_NAME, file, creds)

    orchestrator_connection.log_trace("Remove tmp-folder.")
    shutil.rmtree(temp_path)


def get_week_dates(number_of_weeks: int = None):
    """
    Returns the start and end dates of the current week.

    The week is considered to start on Monday at 00:00:00 and end on Sunday at 23:59:59.
    If number_of_weeks is provided, it adjusts the current date by subtracting the specified number of weeks.

    Args:
        number_of_weeks (int, optional): Number of weeks to subtract from the current date.

    Returns:
        tuple: A tuple containing two datetime objects:
               - start_of_week: the start of the current week (Monday)
               - end_of_week: the end of the current week (Sunday)
    """
    locale.setlocale(locale.LC_TIME, 'da_DK.UTF-8')
    today = datetime.now() - timedelta(weeks=number_of_weeks) if number_of_weeks else datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(days=6, seconds=86399)

    return start_of_week, end_of_week


def export_egenbefordring_from_hub(connection_string: str, temp_path: str, number_of_weeks: int = None):
    """
    Retrieves 'Egenbefordring' data for the current week from the database and exports it to an Excel file.

    Args:
        connection_string (str): The database connection string.
        temp_path (str): The path where the Excel file will be saved.

    The function performs the following steps:
        - Retrieves the start and end dates for the current week.
        - Queries the database for records that fall within the week.
        - Normalizes and formats the JSON data retrieved.
        - Exports the normalized data to an Excel file with the current week's details.
    """
    current_week_start, current_week_end = get_week_dates(number_of_weeks=number_of_weeks)
    start_date = current_week_start.strftime('%Y-%m-%d %H:%M:%S')
    end_date = current_week_end.strftime('%Y-%m-%d %H:%M:%S')
    current_week_number = datetime.date(datetime.now() - timedelta(weeks=number_of_weeks) if number_of_weeks else datetime.now()).isocalendar()[1]
    date_filename = f"{current_week_number}_{current_week_start.strftime('%d%m%Y')}_{current_week_end.strftime('%d%m%Y')}"
    xl_sheetname = f"{current_week_number}_{datetime.now().year}"

    add_columns = {
        'aendret_beloeb_i_alt': [],
        'godkendt': [],
        'godkendt_af': [],
        'behandlet_ok': [],
        'behandlet_fejl': []
    }

    remove_columns = ['koerselsliste_tomme_felter_tjek_']
    move_columns_to_last = ['test', 'attachments', 'uuid']

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    query = f"""
    SELECT  reference,
            CASE
                WHEN JSON_VALUE(data, '$.completed') IS NOT NULL THEN JSON_VALUE(data, '$.completed')
                ELSE JSON_VALUE(data, '$.entity.completed[0].value')
            END as [modtagelsesdato],
            data
    FROM    rpa.Hub_GO_Egenbefordring_ifm_til_skolekoer
    WHERE   (JSON_Value(data, '$.completed') >= '{start_date}' AND JSON_Value(data, '$.completed') <= '{end_date}')
            OR (JSON_Value(data, '$.entity.completed[0].value') >= '{start_date}' AND JSON_Value(data, '$.entity.completed[0].value') <= '{end_date}')
    """

    cursor.execute(query)
    result = cursor.fetchall()

    file_name = rf"{temp_path}\Egenbefordring_{date_filename}.xlsx"

    for row in result:
        uuid = row.reference
        received_date = row.modtagelsesdato
        datetime_obj = datetime.fromisoformat(received_date)
        formatted_datetime_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
        json_data = json.loads(row.data)
        json_data_normalized = pd.json_normalize(json_data['data'], sep='_', max_level=0)
        json_data_normalized['modtagelsesdato'] = formatted_datetime_str
        json_data_normalized['uuid'] = uuid
        export_to_excel(file_name, f"{xl_sheetname}", json_data_normalized, add_columns, remove_columns, move_columns_to_last)

    cursor.close()
    conn.close()

    return file_name


def upload_file_to_sharepoint(folder_name: str, file: str, credentials):
    """
    Uploads a file to a specified folder within a SharePoint site.

    Args:
        folder_name (str): The name of the folder within the SharePoint
                            document library where the file will be uploaded.
        file (str): The local path to the file that needs to be uploaded.
        credentials: An object containing 'username' and 'password' attributes
                        for SharePoint authentication.

    Returns:
        None
    """
    sharepoint_details = {
        "username": f"{credentials.username}",
        "password": f"{credentials.password}",
        "site_url": "https://aarhuskommune.sharepoint.com",
        "site_name": f"{config.SITE_NAME}",
        "document_library": "Delte dokumenter"
    }
    sp = Sharepoint(**sharepoint_details)

    sp.upload_file(folder_name, file)


if __name__ == "__main__":
    oc = OrchestratorConnection.create_connection_from_args()
    process(oc)
