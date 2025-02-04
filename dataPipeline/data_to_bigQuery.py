from google.cloud import bigquery
import pandas as pd
from dataPipeline.agg_ops import GenerateSalesTables
from dataPipeline.data_imputation import *
from modelling.create_forecast_table import *
import json
from google.oauth2.service_account import Credentials

class DataToBigQuery:
    
    """
    A class for managing BigQuery connections and filtering data.
    """

    def __init__(self):
        """
        Initializes the bigQuery connections
        """
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_dir = os.path.join(current_dir, "livello_forecast_system/config_files")
        config_file = os.path.join(config_dir, "config_bigQuery.json")
        with open(config_file, 'r') as f:
            config_data = json.load(f)

        self.project_id = config_data['project_id']
        self.target_table = config_data['target_table']
        self.credential = Credentials.from_service_account_file(config_file)
        self.client = bigquery.Client(project=self.project_id, credentials=self.credential)  # Initialize BigQuery client
        self.national_holidays = holidays.AT(years=range(2024, 2025))
        self.national_holidays = {
            'AT': holidays.AT(years=range(2024, 2025)),
            'DE': holidays.DE(years=range(2024, 2025)),
            'CH': holidays.CH(years=range(2024, 2025))
        }

    def fetch_latest_transaction_date_preprocess_data(self):
        sql_get_date = "SELECT MAX(Date) AS max_date FROM `livello-erp.DataScience.preprocessed_data`"
        query_job = self.client.query(sql_get_date)
        df = query_job.to_dataframe()
        max_date_str = df['max_date'].iloc[0]
        return max_date_str
    
    def apply_date_filter(self, df):
        
        # Fetch the latest date from the BigQuery table
        latest_date = self.fetch_latest_transaction_date_preprocess_data()
        # New df_filtered data frame created with new updated rows added
        df_filtered = df[df['Date'] > latest_date]
        if not df_filtered.empty:
            df_final = df_filtered.reset_index()
            df_final.drop_duplicates(subset=['Date', 'ProductId', 'KioskId'], keep='first', inplace=True)

        return df_final

    def processed_data_to_bigQuery(self, df):
        """
        Store the final cleaned data into BigQuery.
        """
        df_final = df.reset_index()
        df_final.to_gbq(self.target_table, project_id=self.project_id, credentials=self.credential, if_exists='replace')
        print("DoneBQ")    

    def merged_precalculated_forecasts_to_bigquery(self, df):
        """
        After calculating the precalculated forecasts for each kiosk, we merge them and push it into BigQuery.
        """
        df.to_gbq("all_kiosks_forecasts.all_forecasts", project_id=self.project_id, credentials=self.credential, if_exists='replace')
        print("All the precalculated forecasts are merged and pushed to BigQuery")

    def generate_sales_data(self, df):
        report_generator = GenerateSalesTables(df)
        sales_data = report_generator.generate_sales_data()
        costs_data = report_generator.generate_cost_data()
        return [sales_data, costs_data]

    def upload_sales_data(self, sales_data, costs_data):
        sales_data.to_gbq("DataScience.sales_data_", project_id=self.project_id, credentials=self.credential, if_exists='replace')
        costs_data.to_gbq("DataScience.costs_data_", project_id=self.project_id, credentials=self.credential, if_exists='replace')

    def upload_company_data(self, df):
        """
        Uploading and Updating the company data in BigQuery.
        """
        df = df.astype(str)
        df.to_gbq("DataScience.company_data", project_id=self.project_id, credentials=self.credential, if_exists='replace')
        print("Company data updated")

    def create_time_series(self, df, KioskId, kiosk_location, national_holidays):
        data = df.reset_index()
        time_series = transform_into_ts(data, KioskId, kiosk_location, national_holidays)
        return time_series

    def upload_time_series(self, time_series, KioskId):
        time_series.to_gbq("time_series.{}".format(KioskId), project_id=self.project_id, credentials=self.credential, if_exists='replace')
        print("Time series data updated for KioskId - {}".format(KioskId))

    def delete_old_forecasts(self, KioskId, max_date_str):
        sql_delete_revenue = "DELETE FROM livello-erp.precalculated_forecasts.{} WHERE Date > '{}'".format(KioskId, max_date_str)
        sql_delete_costs = "DELETE FROM livello-erp.precalculated_costs_forecasts.{} WHERE Date > '{}'".format(KioskId, max_date_str)

        try:
            query_job_delete_revenue = self.client.query(sql_delete_revenue)
            query_job_delete_costs = self.client.query(sql_delete_costs)
            query_job_delete_revenue.result()
            query_job_delete_costs.result()

        except: pass

    def fetch_latest_transaction_date(self, KioskId):
        sql_get_date = "SELECT MAX(date) AS max_date FROM livello-erp.time_series.{}".format(KioskId)
        query_job = self.client.query(sql_get_date)
        df = query_job.to_dataframe()
        df['max_date'] = pd.to_datetime(df['max_date']).dt.date
        max_date_str = str(df['max_date'].iloc[0])
        return max_date_str
    
    def fetch_train_data_start_date(self, KioskId):
        sql_get_date = "SELECT MIN(date) AS min_date FROM livello-erp.time_series.{}".format(KioskId)
        query_job = self.client.query(sql_get_date)
        df = query_job.to_dataframe()
        df['min_date'] = pd.to_datetime(df['min_date']).dt.date
        min_date_str = str(df['min_date'].iloc[0])
        return min_date_str    

    def create_forecasts(self, KioskId, horizon):
        forecasts = calculate_forecasts(KioskId, horizon)
        return forecasts
    
    def upload_forecasts(self, forecasts, KioskId):
        try:
            forecasts.to_gbq("precalculated_forecasts.{}".format(KioskId), project_id=self.project_id, credentials=self.credential, if_exists='replace')
        except:
            forecasts.to_gbq("precalculated_forecasts.{}".format(KioskId), project_id=self.project_id, credentials=self.credential, if_exists='replace')
            
    def upload_cost_forecast(self, costs_forecasts, KioskId):
        costs_forecasts.to_gbq("precalculated_costs_forecasts.{}".format(KioskId), project_id=self.project_id, credentials=self.credential, if_exists='replace')
        print("Costs_forecasts updated for KioskId = {}".format(KioskId))

    def upload_complete_forecast_data(self, df):
        df.to_gbq("all_forecasts_merged.forecast_table", project_id=self.project_id, credentials=self.credential, if_exists='replace')

    def create_product_demand_forecasts(self, KioskId, horizon):
        forecasts = calculate_product_demand_forecasts(KioskId, horizon)
        return forecasts

    def upload_product_demand_forecasts(self, forecasts, KioskId):
        forecasts.to_gbq("precalculated_product_demand_forecasts.{}".format(KioskId), project_id=self.project_id, credentials=self.credential, if_exists='replace')
            
    def upload_complete_product_demand_forecast_data(self, df):
        df.to_gbq("product_demand_forecasts_merged.forecast_table", project_id=self.project_id, credentials=self.credential, if_exists='replace')

    def upload_product_mapping_data(self, df):
        df.to_gbq("DataScience.product_data", project_id=self.project_id, credentials=self.credential, if_exists='replace')

    def upload_kiosk_mapping_data(self, df):
        df.to_gbq("DataScience.kiosk_data", project_id=self.project_id, credentials=self.credential, if_exists='replace')
