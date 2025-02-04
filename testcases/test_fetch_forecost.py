from dataProcessing.load_data import BigQueryManager
import pandas as pd
import pytest
from google.oauth2.service_account import Credentials
from dataProcessing.data_imputation import *  
from dataProcessing.load_data import *
from datetime import datetime, timedelta
import warnings 

warnings.filterwarnings('ignore')

@pytest.fixture
def bq_manager(mocker):

    bq_manager = BigQueryManager(config_data['project_id'], Credentials.from_service_account_file(config_file_path))
    return bq_manager

def test_fetch_forecast_sales(bq_manager, mocker):
    sample_kiosk_id = ["5d4c2c98e826180031d1feec","5d26f6fbc112e8002eb64948"]
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/precalcualted_forecast_testing_data_5d26f6fbc112e8002eb64948.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_forecast("sales", sample_kiosk_id)
    assert isinstance(result, pd.DataFrame)
    assert "Date" in result.columns
    assert "Forecast" in result.columns
    assert result["Date"].tolist() == sample_result["Date"].tolist()
    assert result["Forecast"].tolist() == sample_result["Forecast"].tolist()

def test_fetch_forecast_costs(bq_manager, mocker):
    sample_kiosk_id = ["5d4c2c98e826180031d1feec","5d26f6fbc112e8002eb64948"]
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/precalcualted_forecast_testing_data_5d26f6fbc112e8002eb64948.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_forecast("costs", sample_kiosk_id)
    assert isinstance(result, pd.DataFrame)
    assert "Date" in result.columns
    assert "Forecast" in result.columns
    assert result["Date"].tolist() == sample_result["Date"].tolist()
    assert result["Forecast"].tolist() == sample_result["Forecast"].tolist()

def test_fetch_forecast_notempty_result(bq_manager, mocker):
    sample_kiosk_id = ["5d4c2c98e826180031d1feec","5d26f6fbc112e8002eb64948"]
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/precalcualted_forecast_testing_data_5d26f6fbc112e8002eb64948.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result_sales = bq_manager.fetch_forecast("sales", sample_kiosk_id)
    result_costs = bq_manager.fetch_forecast("costs", sample_kiosk_id)
    assert isinstance(result_sales, pd.DataFrame)
    assert not result_sales.empty
    assert isinstance(result_costs, pd.DataFrame)
    assert not result_costs.empty

def test_fetch_forecast_sales_empty_result(bq_manager, mocker):
    sample_kiosk_id = ["invalid_kiosk_id"]
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/precalcualted_forecast_testing_data_5d26f6fbc112e8002eb64948.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_forecast("sales", sample_kiosk_id)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty      

def test_fetch_forecast_costs_empty_result(bq_manager, mocker):
    sample_kiosk_id = ["invalid_kiosk_id"]
    sample_result = pd.DataFrame()
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_forecast("costs", sample_kiosk_id)
    assert isinstance(result, pd.DataFrame)
    assert result.empty    

def test_fetch_forecast_empty_kiosk_id(bq_manager, mocker):
    with pytest.raises(ValueError):
        bq_manager.fetch_forecast("sales", [])    

