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

def test_fetch_final_forecasts(bq_manager, mocker):
    mocker.patch('pandas_gbq.read_gbq')
    sample_kioskids = ["5d4c2c98e826180031d1feec", "5d26f6fbc112e8002eb64948"]
    bq_manager.fetch_final_forecasts(sample_kioskids)
    expected_sql = "SELECT * FROM `forecast-app.all_forecasts_merged.forecast_table` where kioskid IN UNNEST(['5d4c2c98e826180031d1feec', '5d26f6fbc112e8002eb64948'])"
    pandas_gbq.read_gbq.assert_called_once_with(expected_sql, project_id=bq_manager.project_id, credentials=bq_manager.credential)

def test_fetch_finalf_data_nonempty_result(bq_manager, mocker):
    sample_kiosk_ids = ["5d4c2c98e826180031d1feec", "5d26f6fbc112e8002eb64948"]
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/forecast_table_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_final_forecasts(sample_kiosk_ids)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty

def test_fetch_final_forecasts_success(bq_manager, mocker):
    sample_kiosk_ids = ["5d4c2c98e826180031d1feec", "5d26f6fbc112e8002eb64948"]
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/forecast_table_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_final_forecasts(sample_kiosk_ids)
    expected_columns = ["Date", "SalesForecast", "CostsForecast", "kioskid", "profit"]
    assert all(col in result.columns for col in expected_columns)
    assert result.equals(sample_result)

def test_fetch_final_forecasts_nonexistent_ids(bq_manager, mocker):
    nonexistent_ids = ["nonexistent_id_1", "nonexistent_id_2"]
    result = bq_manager.fetch_final_forecasts(nonexistent_ids)
    assert result is None or result.empty


def test_fetch_final_forecasts_empty_input(bq_manager, mocker):
    result = bq_manager.fetch_final_forecasts([])
    assert isinstance(result, pd.DataFrame)
    assert result.empty

def test_fetch_final_forecasts_sql_injection(bq_manager, mocker):
    malicious_ids = ["5d4c2c98e826180031d1feec'; DROP TABLE `forecast-app.all_forecasts_merged.forecast_table`; --"]
    result = bq_manager.fetch_final_forecasts(malicious_ids)
    assert result is None or result.empty
