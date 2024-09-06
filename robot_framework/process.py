"""This module contains the main process of the robot."""
import os
import json
from datetime import datetime, timedelta
import locale
import pandas as pd
from sqlalchemy import create_engine, text
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from robot_framework.sub_processes.excel import export_to_excel


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    oc_args_json = json.loads(orchestrator_connection.process_arguments)

    temp_path = oc_args_json['tempPath']
    conn_str = orchestrator_connection.get_constant('DbConnectionString').value

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    export_egenbefordring_from_hub(conn_str, temp_path)


def get_current_week_dates():
    """
    Returns the start and end dates of the current week.

    The week is considered to start on Monday at 00:00:00 and end on Sunday at 23:59:59.

    Returns:
        tuple: A tuple containing two datetime objects:
               - start_of_week: the start of the current week (Monday)
               - end_of_week: the end of the current week (Sunday)
    """
    locale.setlocale(locale.LC_TIME, 'da_DK.UTF-8')
    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(days=6, seconds=86399)  # 23:59:59 on Sunday
    return start_of_week, end_of_week


def export_egenbefordring_from_hub(connection_string: str, temp_path: str):
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
    current_week_start, current_week_end = get_current_week_dates()
    start_date = current_week_start.strftime('%Y-%m-%d %H:%M:%S')
    end_date = current_week_end.strftime('%Y-%m-%d %H:%M:%S')
    current_week_number = datetime.date(datetime.now()).isocalendar()[1]
    date_filename = f"{current_week_number}_{current_week_start.strftime('%d%m%Y')}_{current_week_end.strftime('%d%m%Y')}"
    xl_sheetname = f"{current_week_number}_{datetime.now().year}"

    engine = create_engine(connection_string)
    add_columns = {
        'aendret_beloeb_i_alt': [],
        'godkendt': [],
        'godkendt_af': [],
        'behandlet_ok': [],
        'behandlet_fejl': []
    }

    remove_columns = ['koerselsliste_tomme_felter_tjek_']
    move_columns_to_last = ['test', 'attachments', 'uuid']

    with engine.begin() as conn:
        query = text(f"""SELECT  reference,
                                CASE
                                    WHEN JSON_VALUE(data, '$.completed') IS NOT NULL THEN JSON_VALUE(data, '$.completed')
                                    ELSE JSON_VALUE(data, '$.entity.completed[0].value')
                                END as [modtagelsesdato],
                                data
                        FROM    rpa.Hub_GO_Egenbefordring_ifm_til_skolekoer
                        WHERE   (JSON_Value(data, '$.completed') >= '{start_date}' AND JSON_Value(data, '$.completed') <= '{end_date}')
                                OR (JSON_Value(data, '$.entity.completed[0].value') >= '{start_date}' AND JSON_Value(data, '$.entity.completed[0].value') <= '{end_date}')
                     """)
        result = conn.execute(query)
        result_as_dict = result.mappings().all()

        for obj in result_as_dict:
            uuid = obj['reference']
            received_date = obj['modtagelsesdato']
            datetime_obj = datetime.fromisoformat(received_date)
            formatted_datetime_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
            json_data = json.loads(obj['data'])
            json_data_normalized = pd.json_normalize(json_data['data'], sep='_', max_level=0)
            json_data_normalized['modtagelsesdato'] = formatted_datetime_str
            json_data_normalized['uuid'] = uuid
            export_to_excel(rf"{temp_path}\Egenbefordring_{date_filename}.xlsx", f"{xl_sheetname}", json_data_normalized, add_columns, remove_columns, move_columns_to_last)
