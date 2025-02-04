from dataPipeline.data_from_mongoDB import *
from dataPipeline.data_merge_and_clean import DataMerge
from dataPipeline.data_to_bigQuery import DataToBigQuery
from dataProcessing.load_data import *
from modelling.create_forecast_table import *
import logging
import time
from datetime import datetime, timedelta
import holidays
date_format = '%Y-%m-%d'

# Change this to True, if you want to initiate logging for Data Migration component of the forecasting service
log_status = False

if log_status:
    logging.basicConfig(filename='C1:DataMigrationTest.log', encoding='utf-8', level=logging.DEBUG)

def main():

    # Record the start time
    start_time = time.time()

    # Data Migration from MongoDb to BigQuery

    connection = DataProcessFromMongoDB()
    df_transaction = connection.transaction_data_from_mongodb()
    df_product = connection.product_data_from_mongodb()
    df_organization_data = connection.organization_data_from_mongodb()

    if log_status:
        logging.info("Connection to MongoDB established")
    else: print("Connection to MongoDB established")

    # Uploader Object
    bigQ = DataToBigQuery()
    # Fetcher Object
    data_from_bigquery = BigQueryManager(config_data['project_id'], Credentials.from_service_account_file(config_file_path))

    if log_status:
        logging.info("Big Query Uploader object created")
    else: print("Big Query Uploader object created")

    merge = DataMerge(df_transaction, df_product)
    df_merge = merge.data_merge_clean()
    df_merge.drop_duplicates(subset=['Date','ProductId','KioskId'], inplace=True, keep='first')

    bigQ.processed_data_to_bigQuery(df_merge)
    
    if log_status:
        logging.info("Transactions table created from MongoDB and uploaded into BigQuery")
    else: print("Transactions table created from MongoDB and uploaded into BigQuery")
    
    # Mapping table to keep track of available kiosks and products
    product_table = df_merge[['ProductId', 'ProductName']].drop_duplicates().reset_index(drop=True)
    bigQ.upload_product_mapping_data(product_table)
    kiosk_table = df_merge[['KioskId', 'KioskName']].drop_duplicates().reset_index(drop=True)
    bigQ.upload_kiosk_mapping_data(kiosk_table)
    print("kiosks and products tables updated in bigQ")
    
    if log_status:
        logging.info("Organization Data collected from MongoDB, Uploaded into BigQuery")
    else: print("Organization Data collected from MongoDB, Uploaded into BigQuery")

    # updating the sales data in bigquery(daily net cost,sales,profit) --> To be further used for forecasting
    # unstring the below line of code only if the integrity of test DB is verified otherwise, fake data will be erased with real data
    try:
        sales_report = bigQ.generate_sales_data(df_merge)
        bigQ.upload_sales_data(sales_report[0],sales_report[1])
    except Exception as e:
        if log_status:
            logging.error("Error in generating sales table - ",e)
        else:
            print(e)

    # print('sales tables generated --------- ')
    complete_data = df_merge.drop(['ProductName','KioskName'],axis=1)
    complete_data = complete_data.dropna(subset=['KioskId'])
    complete_data["Date"] = pd.to_datetime(complete_data["Date"]).dt.date
    grouped_data_by_kiosk = complete_data.groupby('KioskId')

    d1 = pd.DataFrame(columns=complete_data.columns)
    for kioskids, group in grouped_data_by_kiosk:
        min_date = group["Date"].min()
        max_date = group["Date"].max()
        if (max_date - min_date).days <= 180:  # Check if the difference between max and min dates is at least 180 days (6 months)
            group['Infomessage'] = "Not Sufficient Data To Forecast"  # Add 'Forecast' column with the specified string
            d1 = pd.concat([d1, group])  # Add group to d1 if data available within the last 6 months

        elif (max_date - min_date).days >= 180:  # Check if the difference between max and min dates is at least 180 days (6 months)
            group['Infomessage'] = "Sufficient Data To Forecast"  # Add 'Forecast' column with the specified string
            d1 = pd.concat([d1, group])  # Add group to d1 if data available within the last 6 months  

    # Kiosk Forecast status
    info_status = d1[['KioskId','Infomessage']].drop_duplicates()
    info_status['status'] = info_status['Infomessage'] == 'Sufficient Data To Forecast'
    info_status.to_csv('support_files/kiosk_status.csv') 

    
    # List of kioskids
    kioskids = list(grouped_data_by_kiosk.groups.keys())
    print("LENGETH OF KIOSKS BEFORE is :- ",len(kioskids))

    # Remove each kiosk ID from kioskids if it exists
    for kiosk_id in kiosks_to_remove:
        if kiosk_id in kioskids:
            kioskids.remove(kiosk_id)

    # Check if the IDs were removed
    print("LENGETH OF KIOSKS AFTER REMOVING  is :- ",len(kioskids))
    
    if log_status:
        logging.info("kioskid wise grouping done")
    else: print("kioskid wise grouping done")

    national_holidays = []  

    national_holidays = {
        'AT': holidays.AT(years=range(2024, 2025)),
        'DE': holidays.DE(years=range(2024, 2025)),
        'CH': holidays.CH(years=range(2024, 2025))
    }    

    for kioskid in kioskids:

        try:
            state,city,country = data_from_bigquery.fetch_location_data(kioskid)
            kiosk_location = {'state':state,'city':city,'country':country}
            time_series = bigQ.create_time_series(df_merge,kioskid,kiosk_location,national_holidays)
            bigQ.upload_time_series(time_series, kioskid)
        except Exception as e:
            kioskids.remove(kioskid)
            if log_status:
                logging.debug('time series was not generated for the kiosk due to - {}'.format(e))
            else: print('time series was not generated for the kiosk due to - {}'.format(e))

    if log_status:
        logging.debug("Data imputed Time series created for available kiosks and uploaded into Big Query")
    else: print("Data imputed Time series created for available kiosks and uploaded into Big Query")

    # # saving the list of forecasted kioskids - to be used by modelling component

    os.remove('support_files/forecasted_kiosks_test.txt')
    with open('support_files/forecasted_kiosks_test.txt', 'w') as file:
        for item in kioskids:
            file.write('%s\n' % item) 

    # # Forecasting
            
    for kioskid in kioskids:
        
        try:
            # prepare individual forecast horizon and delete the old forecasts
            train_data_end_date = bigQ.fetch_latest_transaction_date(kioskid)
            a = datetime.strptime(train_data_end_date, date_format)
            b = datetime.now() + timedelta(days=365)
            delta = b - a
            forecast_horizon = delta.days
            training_data_start_date = bigQ.fetch_train_data_start_date(kioskid)
            bigQ.delete_old_forecasts(kioskid, train_data_end_date)
            print('forecasts deleted for = {} from date -{}'.format(kioskid, a))
           
            # Revenue forecasting
            forecast = bigQ.create_forecasts(kioskid,forecast_horizon)
            bigQ.upload_forecasts(forecast, kioskid)

            # Cost forecasting
            costs_forecast = calculate_costs_forecasts(kioskid,training_data_start_date, forecast_horizon)
            print('cost forecasting done for kioskid = {}'.format(kioskid))
            bigQ.upload_cost_forecast(costs_forecast,kioskid)

        except Exception as e:
            
            if log_status:
                logging.error("Something went wrong during forecasting for kioskid = {}, error = {}".format(kioskid,e))
            else:
                print("Something went wrong during forecasting for kioskid = {}, error = {}".format(kioskid,e))
            
            pass

    if log_status:
        logging.info("Kiosk wise revenue and cost forecasts created")
    else: print("Kiosk wise revenue and cost forecasts created")    

    # Calculating product demand

    for kioskid in kioskids:
            
        try:
            a = datetime.strptime(bigQ.fetch_latest_transaction_date(kioskid), date_format)
            b = datetime.now() + timedelta(days=365)
            delta = b - a
            forecast_horizon = delta.days
            forecast = bigQ.create_product_demand_forecasts(kioskid,forecast_horizon)
            bigQ.upload_product_demand_forecasts(forecast, kioskid)
        except Exception as e:
            print("error in product demand forecasting for the kioskid - {} and error = {}".format(kioskid,e))

    # merging all the precalculated forecasts

    product_tables_to_concat = []
    revenue_tables_to_concat = []

    for kioskid in kioskids:
        
        try:
            sales_data = data_from_bigquery.fetch_forecast("sales", kioskid)
            costs_data = data_from_bigquery.fetch_forecast("costs", kioskid)
            sales_data.rename(columns={"Forecast": "SalesForecast"}, inplace=True)
            costs_data.rename(columns={"Forecast": "CostsForecast"}, inplace=True)
            merged_data = sales_data.merge(costs_data, on="Date", how = "left")

            merged_data["SalesForecast"] = merged_data["SalesForecast"].astype(float)
            merged_data["CostsForecast"] = merged_data["CostsForecast"].astype(float)

            merged_data["Date"] = pd.to_datetime(merged_data["Date"])
            merged_data["Date"] = merged_data["Date"].dt.strftime('%Y-%m-%d')

            merged_data["kioskid"] = kioskid
            merged_data["CostsForecast"].fillna(0, inplace=True)
        
            merged_data['profit'] = merged_data.apply(lambda row: 0 if row['CostsForecast'] == 0 else row['SalesForecast'] - row['CostsForecast'], axis=1)
            merged_data['profit'] = merged_data['profit'].astype(float)

            revenue_tables_to_concat.append(merged_data)
            product_tables_to_concat.append(data_from_bigquery.fetch_product_demand_forecasts(kioskid))

        except Exception as e:
            print(e)
            pass

    # merging and uploading the final revenue forecasts to bigquery
    revenue_forecasts = pd.concat(revenue_tables_to_concat, ignore_index=True)
    # print("DONE WITH REVENUE FORECASTS \n",revenue_forecasts.head())
    print("DONE WITH REVENUE FORECASTS and lengeth of it\n",len(revenue_forecasts))
    bigQ.upload_complete_forecast_data(revenue_forecasts)

    # merging and uploading the final product demand forecasts to bigQuery
    pd_forecasts = pd.concat(product_tables_to_concat, ignore_index=True)
    # print("DONE WITH PRODUCTS FORECASTS \n",pd_forecasts.head())
    print("DONE WITH PRODUCTSFORECASTS and lengeth of it\n",len(pd_forecasts))
    bigQ.upload_complete_product_demand_forecast_data(pd_forecasts)
    
    # saving meta data
    available_kiosks = data_from_bigquery.fetch_kiosk_names()["kioskid"].to_list()
    available_products = data_from_bigquery.fetch_product_names()['COLUMN_NAME'].to_list()
    with open('support_files/available_kiosks.txt', 'w') as file:
        data_to_write = '\n'.join(available_kiosks)
        file.write(data_to_write)
    available_products.remove("KioskId")
    with open('support_files/available_products.txt', 'w') as file:
        data_to_write = '\n'.join(available_products)
        file.write(data_to_write) 
        end_time = time.time()

    # Calculate the elapsed time
    execution_time = end_time - start_time

    if log_status:
        logging.info(f"Execution time for data pipeline updation: {execution_time:.4f} seconds")
    else:    
        print(f"Execution time for data pipeline updation: {execution_time:.4f}FMER seconds")

if __name__ == "__main__":
    
    main()







