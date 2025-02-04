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

def test_count_num_transactions_success(bq_manager, mocker):
    # Define sample inputs
    start_date = "2022-01-01"
    end_date = "2022-01-31"
    kioskids = ["5d26f6fbc112e8002eb64948", "5d4c2829dd6b51002deb29e9"]
    sample_result = pd.DataFrame({"Trans_count": [100]})
    mocker.patch('pandas_gbq.read_gbq', return_value=sample_result)
    result = bq_manager.count_num_transactions(start_date, end_date, kioskids)
    assert result == int(result)

