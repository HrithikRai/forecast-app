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

def test_fetch_table_returns_dataframe(bq_manager, mocker):
    sample_table_name = "forecast-app.DataScience.sales_data_"
    sample_kiosk_id = "5d4c2c98e826180031d1feec"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/sales_data_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_table(sample_table_name, sample_kiosk_id)
    assert isinstance(result, pd.DataFrame)
    assert "Date" in result.columns
    assert "5d4c2c98e826180031d1feec" in result.columns
    assert result["Date"].tolist() == sample_result["Date"].tolist()
    assert result["5d4c2c98e826180031d1feec"].tolist() == sample_result["5d4c2c98e826180031d1feec"].tolist()

def test_fetch_table_notempty_result(bq_manager, mocker):
    sample_table_name = "forecast-app.DataScience.sales_data_"
    sample_kiosk_id = "5d4c2c98e826180031d1feec"
    sample_result = pd.read_csv(r'testcases/testing_test_cases_data/sales_data_testing_data.csv')
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.fetch_table(sample_table_name, sample_kiosk_id)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
