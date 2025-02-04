from google.oauth2.service_account import Credentials
import pandas_gbq
import pandas as pd
import numpy as np
import os
import json
from dataPipeline.data_imputation import *
from google.cloud import bigquery

class BigQueryManager:
    """
    A class for managing BigQuery connections and filtering data.
    """
    def __init__(self, project_id, credential):
        """
        Initializes the BigQuery connections
        """
        self.client = bigquery.Client(project=project_id, credentials=credential)
        self.project_id = project_id
        self.credential = credential
        #self.target_table = target_table
    
    def filter_data(self, kioskId, product_id=None):
        """
        Filters data from BigQuery based on kiosk ID and also product ID.
        Returns a pandas DataFrame.
        """
        sql = self.generate_sql_query(kioskId, product_id)
        filtered_data = pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
        filtered_data = filtered_data.drop(columns=["index"])
        filtered_data.index = pd.to_datetime(filtered_data["Date"], format='%Y-%m-%d')
        return filtered_data
        
    def fetch_kiosk_names(self):
        sql = "SELECT distinct(kioskid) FROM `forecast-app.all_forecasts_merged.forecast_table`"
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
    
    def fetch_product_names(self):
        sql = "SELECT COLUMN_NAME from product_demand_forecasts_merged.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'forecast_table'"
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)[1:]
  
    def fetch_company_data(self, CompanyIds):
        sql = "SELECT KioskId FROM DataScience.company_data where CompanyId = {}".format(CompanyIds)
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)['KioskId'].tolist()

    def fetch_forecasted_kiosk_names(self):
        forecasted_kiosks_list = []
        with open('support_files/forecasted_kiosks_test.txt', 'r') as file:
            for line in file:
                forecasted_kiosks_list.append(line.strip())
        return forecasted_kiosks_list
    
    def fetch_table(self,table_name, kioskId):

        sql = "SELECT date,Forecast FROM {}.{}".format(table_name,kioskId)
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)  
    
    def fetch_forecast(self,type,kioskId):

        if type == "sales":
            
            sql = "SELECT Date, Forecast FROM `forecast-app.precalculated_forecasts.{}`".format(kioskId)
            return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
        
        elif type == "costs":
            
            sql = "SELECT Date, Forecast FROM `forecast-app.precalculated_costs_forecasts.{}`".format(kioskId)
            return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)          

    def fetch_location_data(self,kioskid):

        try:
            sql = """SELECT
                    JSON_EXTRACT_SCALAR(location, '$.address.state') AS state,
                    JSON_EXTRACT_SCALAR(location, '$.address.city') AS city,
                    JSON_EXTRACT_SCALAR(location, '$.address.country') AS country
                    FROM
                    `forecast-app.DataScience.company_data`
                    WHERE KioskId = '{}';""".format(kioskid)
            location_data = pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
            state = str(location_data.state.iloc[0])
            city = str(location_data.city.iloc[0])
            country = str(location_data.country.iloc[0])
            return state,city,country
        
        except :
            state = 'North Rhine-Westphalia' 
            city = "Düsseldorf"
            country = 'Germany'
            return state,city,country

    def fetch_final_forecasts(self,kioskids):

        """Function to extract data to form aggregated revenue and revenue comparision"""
        sql = "SELECT * FROM `forecast-app.all_forecasts_merged.forecast_table` where kioskid IN UNNEST({})".format(kioskids)
        return  pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
    
    def fetch_final_forecasts_agg_revenue(self, kioskids, start_date, end_date):
        """
        Function to extract data to form aggregated revenue and revenue comparison
        """
        sql = f"""
        SELECT 
            Date,
            SUM(SalesForecast) AS SalesForecast,
            SUM(CostsForecast) AS CostsForecast,
            SUM(profit) AS profit
        FROM `forecast-app.all_forecasts_merged.forecast_table`
        WHERE kioskid IN UNNEST({kioskids})
        AND DATE(Date) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY Date
        ORDER BY Date ASC
        """
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)

    def fetch_final_forecasts_comp_revenue1(self, kioskids, start_date, end_date):
        formatted_kioskids = ', '.join([f"'{item}'" for item in kioskids])
        kiosk_columns = ', '.join([f"SUM(IF(kioskid = '{kiosk}', SalesForecast, 0)) AS `{kiosk}`" for kiosk in kioskids])
        
        sql = f"""
        SELECT
            DATE(Date) AS Date,
            {kiosk_columns}
        FROM `forecast-app.all_forecasts_merged.forecast_table`
        WHERE
            kioskid IN ({formatted_kioskids}) AND
            DATE(Date) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY Date
        ORDER BY Date
        """
        
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)


    def fetch_final_forecasts_info_card(self, kioskids, start_date, end_date):
        sql = f"""
        SELECT 
            SUM(SalesForecast) AS TotalSalesForecast
        FROM 
            `forecast-app.all_forecasts_merged.forecast_table`
        WHERE 
            kioskid IN UNNEST(@kioskids) AND
            DATE(Date) BETWEEN '{start_date}' AND '{end_date}'
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("kioskids", "STRING", kioskids)
            ]
        )
        
        query_job = self.client.query(sql, job_config=job_config)
        results = query_job.result().to_dataframe()
        return results["TotalSalesForecast"].iloc[0]


    def fetch_forecasts_product_demand(self,kioskids,productids):

        """Function to fetch data for product_demand_forecasts"""
        formatted_string = ','.join([f'`{item}`' for item in productids])
        sql = "SELECT Date,{} FROM `forecast-app.product_demand_forecasts_merged.forecast_table` where kioskid IN UNNEST({})".format(formatted_string,kioskids)
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
    
    def fetch_forecasts_product_demand_pdf(self, kioskids, productids, date_list):
        """Function to fetch data for product_demand_forecasts with filtering and computation in SQL."""
        formatted_product_ids = ', '.join([f'`{item}`' for item in productids])
        formatted_date_list = ', '.join([f"'{date}'" for date in date_list])

        sql = f"""
        SELECT 
            Date, {formatted_product_ids}
        FROM 
            `forecast-app.product_demand_forecasts_merged.forecast_table`
        WHERE 
            kioskid IN UNNEST(@kioskids) AND 
            Date IN ({formatted_date_list})
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("kioskids", "STRING", kioskids)
            ]
        )
    
        query_job = self.client.query(sql, job_config=job_config)
        return query_job.result().to_dataframe()
    
    
    def fetch_forecasts_product_demand_top_prod(self, kioskids, productids, date_list):
        """Function to fetch data for product_demand_forecasts with filtering and computation in SQL."""
        formatted_product_ids = ', '.join([f'`{item}`' for item in productids])

        sql = f"""
        SELECT 
            CAST(Date AS DATE) AS Date, {formatted_product_ids}
        FROM 
            `forecast-app.product_demand_forecasts_merged.forecast_table`
        WHERE 
            kioskid IN UNNEST(@kioskids) AND 
            CAST(Date AS DATE) IN UNNEST(@date_list)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("kioskids", "STRING", kioskids),
                bigquery.ArrayQueryParameter("date_list", "DATE", date_list)
            ]
        )

        query_job = self.client.query(sql, job_config=job_config)
        return query_job.result().to_dataframe()


    
    def fetch_product_demand_forecasts(self,kioskid):

        """Function to fetch product demand forecasts fo all the products that belong to a kioskid"""
        sql = "SELECT * FROM precalculated_product_demand_forecasts.{}".format(kioskid)
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)  

         
    def fetch_historic_data(self,kioskids,start_date,end_date):
        
        """Function to fetch historic data for the aggregated revenue forecasts - idea is to fetch forecasts in 1:3 ratio"""
        formatted_string = ','.join([f'`{item}`' for item in kioskids])
        sql = "SELECT Date,{} from `forecast-app.DataScience.sales_data_` WHERE DATE(Date) >= DATE('{}') AND DATE(Date) <= DATE('{}') order by Date asc".format(formatted_string,start_date,end_date)
        historic_data = pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
        return historic_data
            
    def fetch_sales_data(self):

        """Function to fetch complete sales_data"""
        sql = "SELECT * from `forecast-app.DataScience.sales_data_`"
        sales = pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
        return sales  
    
    def fetch_costs_data(self):

        """Function to fetch complete costs_data"""
        sql = "SELECT * from `forecast-app.DataScience.costs_data_`"
        costs = pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
        return costs

    def count_num_transactions(self, start_date, end_date, kioskids):
        """Function to select the number of transactions that happened between a certain date range"""
        sql = "SELECT COUNT(*) AS Trans_count FROM `forecast-app.DataScience.preprocessed_data` WHERE DATE(Date) BETWEEN DATE('{}') AND DATE('{}') AND KioskId IN UNNEST({})".format(start_date, end_date, kioskids)
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential).Trans_count[0]

    def fetch_transactions_data(self):
        """Function to select the number of transactions that happened between a certain date range"""
        sql = "SELECT * FROM `forecast-app.DataScience.preprocessed_data`"
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
    

    def fetch_forecasts_product_demand_info1(self, kioskids, productids, date_list):
        formatted_product_ids = ', '.join([f'COALESCE(SUM(`{item}`), 0) as `{item}`' for item in productids])
        formatted_date_list = ', '.join([f"'{date}'" for date in date_list])
        
        sql = f"""
        SELECT 
            {formatted_product_ids}
        FROM 
            `forecast-app.product_demand_forecasts_merged.forecast_table`
        WHERE 
            kioskid IN UNNEST(@kioskids) AND
            Date IN ({formatted_date_list})
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("kioskids", "STRING", kioskids)
            ]
        )
        
        query_job = self.client.query(sql, job_config=job_config)
        results = query_job.result().to_dataframe()
        total_demand = results.sum(axis=1).sum()
        return total_demand


    def fetch_forecasts_product_demand_info_topsp(self, kioskids, productids, date_list, product_batch_size=400):
        """
        Function to fetch data for product_demand_forecasts with batching for productids and filtering dates.
        """
        all_results = []
        
        for i in range(0, len(productids), product_batch_size):
            batch_productids = productids[i:i + product_batch_size]
            formatted_product_ids = ', '.join([f'SUM(`{item}`) as `{item}`' for item in batch_productids])
            formatted_date_list = ', '.join([f"'{date}'" for date in date_list])
            
            sql = f"""
            SELECT 
                Date, 
                {formatted_product_ids}
            FROM 
                `forecast-app.product_demand_forecasts_merged.forecast_table`
            WHERE 
                kioskid IN UNNEST(@kioskids) AND
                Date IN ({formatted_date_list})
            GROUP BY Date
            """
            
            job_config = bigquery.QueryJobConfig(
                priority=bigquery.QueryPriority.BATCH,
                query_parameters=[
                    bigquery.ArrayQueryParameter("kioskids", "STRING", kioskids)
                ]
            )

            query_job = self.client.query(sql, job_config=job_config)  # Make an API request to run the query as a batch job
            results = query_job.result().to_dataframe()  # Wait for the job to complete and get the results
            all_results.append(results)
        
        final_results = pd.concat(all_results, ignore_index=True)
        final_results.fillna(0, inplace=True)
        return final_results

    def fetch_forecasts_product_demand_info_PPDDFF(self, kioskids, productids, date_list, product_batch_size=400):
        """
        Function to fetch and process data for product_demand_forecasts with batching for productids and filtering dates.
        """
        all_results = []
        
        for i in range(0, len(productids), product_batch_size):
            batch_productids = productids[i:i + product_batch_size]
            formatted_product_ids = ', '.join([f'COALESCE(SUM(`{item}`), 0) as `{item}`' for item in batch_productids])
            formatted_date_list = ', '.join([f"'{date}'" for date in date_list])
            
            sql = f"""
            SELECT 
                Date, 
                {formatted_product_ids}
            FROM 
                `forecast-app.product_demand_forecasts_merged.forecast_table`
            WHERE 
                kioskid IN UNNEST(@kioskids) AND
                Date IN ({formatted_date_list})
            GROUP BY 
                Date
            """
            
            job_config = bigquery.QueryJobConfig(
                priority=bigquery.QueryPriority.BATCH,
                query_parameters=[
                    bigquery.ArrayQueryParameter("kioskids", "STRING", kioskids)
                ]
            )

            query_job = self.client.query(sql, job_config=job_config)  # Make an API request to run the query as a batch job
            results = query_job.result().to_dataframe()  # Wait for the job to complete and get the results
            all_results.append(results)
        
        final_results = pd.concat(all_results, ignore_index=True)
        final_results.fillna(0, inplace=True)
        return final_results

    def count_num_kiosks(self, kioskids):
        """
        Function to count the number of transactions for each KioskId and return 
        a list of KioskIds with less than 180 transactions.
        """
        if not kioskids:
            return []

        # Convert kioskids list to a string that can be used in SQL IN clause
        kioskids_str = ','.join([f"'{str(kiosk)}'" for kiosk in kioskids])
        
        # SQL query to count transactions per KioskId
        sql = f"""
        SELECT KioskId, COUNT(*) AS Trans_count 
        FROM `forecast-app.DataScience.preprocessed_data` 
        WHERE KioskId IN ({kioskids_str}) 
        GROUP BY KioskId
        """
        
        # Execute the query and get the results as a DataFrame
        df = pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credential)
        
        # Filter the DataFrame to get KioskIds with less than 180 transactions
        low_count_kiosks = df[df['Trans_count'] < 180]['KioskId'].tolist()
        
        return low_count_kiosks

    
# Load config_data
current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
config_dir = os.path.join(current_dir, "config_files")
config_file_path = os.path.join(config_dir, "config_bigQuery.json")
with open(config_file_path, 'r') as f:
    config_data = json.load(f)


