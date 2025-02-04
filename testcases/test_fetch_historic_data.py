from dataProcessing.load_data import BigQueryManager
import pandas as pd
import pytest
from google.oauth2.service_account import Credentials
from dataProcessing.data_imputation import *  
from dataProcessing.load_data import *
from google.cloud import bigquery
import numpy as np
import warnings 

warnings.filterwarnings('ignore')

@pytest.fixture
def bq_manager(mocker):
    bq_manager = BigQueryManager(config_data['project_id'], Credentials.from_service_account_file(config_file_path))
    return bq_manager

def test_fetch_historic_data_success(bq_manager, mocker):
    sample_kioskids = ["5d4c2c98e826180031d1feec", "5d4c2829dd6b51002deb29e9"]
    sample_start_date = "2022-01-01"
    sample_end_date = "2022-01-31"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/sales_data_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_historic_data(sample_kioskids, sample_start_date, sample_end_date)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert result.equals(sample_result)

def test_fetch_historic_data_check_columns_names(bq_manager, mocker):
    sample_kioskids = ["5d4c2c98e826180031d1feec", "5d4c2829dd6b51002deb29e9"]
    sample_start_date = "2022-01-01"
    sample_end_date = "2022-01-31"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/sales_data_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_historic_data(sample_kioskids, sample_start_date, sample_end_date)
    expected_columns = ["Date"] + sample_kioskids 
    assert all(col in result.columns for col in expected_columns)
    assert sample_result.columns.to_list() == result.columns.to_list()

def test_fetch_historic_notempty_result(bq_manager, mocker):
    sample_kioskids = ["5d4c2c98e826180031d1feec", "5d4c2829dd6b51002deb29e9"]
    sample_start_date = "2022-01-01"
    sample_end_date = "2022-01-31"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/sales_data_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_historic_data(sample_kioskids, sample_start_date, sample_end_date)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty

def test_fetch_historic_sales_empty_result(bq_manager, mocker):
    sample_kioskids = ["invalid_kiosk_id"]
    sample_start_date = "2022-01-01"
    sample_end_date = "2022-01-31"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/sales_data_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_historic_data(sample_kioskids, sample_start_date, sample_end_date)
    assert not result.empty      