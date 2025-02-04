
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

def test_fetch_product_demand_forecasts_success(bq_manager, mocker):
    sample_kioskid = "5d26f6fbc112e8002eb64948"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/precalculated_product_demand_forecost_5d26f6fbc112e8002eb64948.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_product_demand_forecasts(sample_kioskid)
    assert result.equals(sample_result)

def test_fetch_product_demand_forecasts_data_nonempty_result(bq_manager, mocker):
    sample_kioskid = "5d26f6fbc112e8002eb64948"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/precalculated_product_demand_forecost_5d26f6fbc112e8002eb64948.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_product_demand_forecasts(sample_kioskid)
    assert not result.empty

def test_fetch_product_demand_forecasts_columns(bq_manager, mocker):
    sample_kiosk_ids = "5d26f6fbc112e8002eb64948"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/precalculated_product_demand_forecost_5d26f6fbc112e8002eb64948.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_product_demand_forecasts(sample_kiosk_ids)
    assert result.equals(sample_result)
    assert sample_result.columns.to_list() == result.columns.to_list()


