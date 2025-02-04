import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import subprocess
import logging

import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import subprocess
import logging

logging.basicConfig(filename='forecast_fails.log', level=logging.DEBUG)
from dataPipeline.data_imputation import perform_imputation

current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
config_dir = os.path.join(current_dir, "config_files")
config_file_path = os.path.join(config_dir, "config_bigQuery.json")
credentials = service_account.Credentials.from_service_account_file(config_file_path)

project_id = 'forecast-app'
client = bigquery.Client(credentials=credentials, project=project_id)

current_dir = os.path.dirname(os.path.abspath(__file__))
model_exog_path = os.path.join(current_dir, 'sarimax.r')
model_path = os.path.join(current_dir, 'sarima.r')

def run_r_script(r_script_path, dataframe_as_string, horizon, range_value):

    try:
        logging.debug("Executing R script...")
        result = subprocess.run(
            ['Rscript', r_script_path, dataframe_as_string, str(horizon), range_value],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            logging.debug("R script executed successfully.")
            logging.debug("R script output: %s", result.stdout)
        else:
            logging.error("R script failed with exit code: %d", result.returncode)
            logging.error("Error message from R script: %s", result.stderr)
            return None

        output = result.stdout.strip().split('\n')
        data = []

        for line in output[1:]:
            parts = line.split()
            data.append({
                'Date': parts[1],
                'Forecast': parts[2],
            })

        return data

    except Exception as e:
        logging.exception("An error occurred while running the R script.")
        return None

def calculate_forecasts(KioskId, horizon):

    query = """
    SELECT Date, revenue, national_holiday, regional_holiday
    FROM time_series.{}
    ORDER BY Date
    """.format(KioskId)

    query_job = client.query(query)
    df = query_job.result().to_dataframe()

    try:
        dataframe_as_string = df.to_string()
        parsed_data = run_r_script(model_exog_path, dataframe_as_string, horizon, 'd')
        if parsed_data is None:
            df.drop(columns=['national_holiday', 'regional_holiday'], inplace=True)
            parsed_data = run_r_script(model_path, dataframe_as_string, horizon, 'd')

        forecast_data = pd.DataFrame(parsed_data)
        return forecast_data

    except Exception as e:
        logging.error("Error occurred for KioskId %s: %s", KioskId, str(e))
        return None

def calculate_product_demand_forecasts(KioskId, horizon):

    query_job = client.query("""
    SELECT *
    FROM time_series.{}
    ORDER BY Date""".format(KioskId))
    
    results = query_job.result()
    df = results.to_dataframe()
    df.drop(['revenue','national_holiday','regional_holiday'],axis=1,inplace=True)

    products = df.columns.to_list()[1:]
    product_forecasts = {}
    i=0

    for product in products:

        ts = df[['Date',product]]
        ts = ts.rename(columns={'{}'.format(product): 'revenue'})
        print(ts)
        dataframe_as_string = ts.to_string()
        try:
            parsed_data = run_r_script(model_path, dataframe_as_string, horizon, 'd')
            forecast_data = pd.DataFrame(parsed_data)
        
            if i == 0:
                product_forecasts['Date'] = forecast_data['Date']
            else:
                forecast_data.drop(['Date'],axis=1,inplace=True)
            i += 1
                      
        except Exception as e:
            logging.info("error occured - {} ,for kioskid - {}".format(e, KioskId))
            pass

        product_forecasts[product] = forecast_data['Forecast']

    forecast_table = pd.DataFrame(product_forecasts)
    forecast_table["KioskId"] = KioskId
    # since its product demand we only need integer values
    forecast_table[products] = forecast_table[products].apply(pd.to_numeric, errors='coerce').clip(lower=0).astype(int)
    return forecast_table
    

def calculate_costs_forecasts(kioskid,training_data_start_date, horizon):
    
    query_job = client.query("""
    SELECT Date, `{}`
    FROM `forecast-app.DataScience.costs_data_`
    where Date >= '{}'                        
    ORDER BY Date""".format(kioskid,training_data_start_date))
    results = query_job.result()
    costs_df = results.to_dataframe()
    costs_df.rename(columns={"{}".format(kioskid):"revenue"},inplace=True)
    costs_df = perform_imputation(costs_df)
    dataframe_as_string_cp = costs_df.to_string()

    try:
        # computing logical forecast horizon for each kiosk, it shouldnt be more than the available time series data
        parsed_data = run_r_script(model_path, dataframe_as_string_cp, horizon, 'd')
        forecast_data = pd.DataFrame(parsed_data)
        return(forecast_data)
    
    except Exception as e:
        logging.info("error occured - {} ,for kioskid - {}".format(e, costs_df.columns[1]))
        pass
