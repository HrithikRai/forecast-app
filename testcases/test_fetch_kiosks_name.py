from dataProcessing.load_data import BigQueryManager
import pandas as pd
import pytest
from google.oauth2.service_account import Credentials
from dataProcessing.data_imputation import *  
from dataProcessing.load_data import *
import warnings 

warnings.filterwarnings('ignore')

@pytest.fixture
def bq_manager(mocker):
    bq_manager = BigQueryManager(config_data['project_id'], Credentials.from_service_account_file(config_file_path))
    return bq_manager

def test_fetch_kiosk_names_output_list_of_strings(bq_manager, mocker):
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/forecast_table_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_kiosk_names()
    assert not result.empty

def test_fetch_kiosk_names_output_list_of_strings1(bq_manager, mocker):
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/forecast_table_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_kiosk_names()
    expected_columns = ["kioskid"] 
    assert all(col in result.columns for col in expected_columns)
    assert sample_result.columns.to_list() == result.columns.to_list()