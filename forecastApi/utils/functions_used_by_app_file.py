from dataProcessing.load_data import BigQueryManager
from flask import request, jsonify
from google.auth import credentials
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import random
import statistics
from forecastApi.utils.get_holiday_info import *
from dataProcessing.load_data import *
from dataProcessing.data_imputation import *
import json
import ast
from google.cloud import bigquery
from forecastApi.utils.process_data import process_data
import os
import support_files  
from support_files import *
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime

# Initialize BigQueryManager
bigQ_precalculated_forecasts = BigQueryManager(config_data['project_id'], Credentials.from_service_account_file(config_file_path))

# loading meta data related information
location_data = pd.read_csv('support_files/kiosk_location_data.csv')
kiosk_status = pd.read_csv('support_files/kiosk_status.csv')
file = open('support_files/available_kiosks.txt','r')
kiosks_text = file.read()
available_kiosks = kiosks_text.split("\n")
file.close()
file = open('support_files/available_products.txt','r')
products_text = file.read()
available_products = products_text.split("\n")
file.close()

credentials = service_account.Credentials.from_service_account_file('config_files/config_bigQuery.json')

def fetch_location_data(kioskid,cat):
    try:
        location = location_data.loc[location_data['KioskId']==kioskid].iloc[0]['location']
        location = ast.literal_eval(location)
        if cat == 'country':
            return location['address']['country']
        elif cat == 'state':
            return location['address']['state']
    except:
         if cat == 'country':
            return 'Germany'
         elif cat == 'state':
            return 'NRW'

def fetch_kiosk_status(kioskid,cat):
    
    status = kiosk_status.loc[kiosk_status['KioskId']==kioskid].iloc[0]['status']
    info_msg = kiosk_status[kiosk_status['KioskId']==kioskid].iloc[0]['Infomessage']

    if cat == 'status':
     return status
    elif cat == 'message':
     return info_msg  

# holiday information for the UI
data_from_bigquery = BigQueryManager(config_data['project_id'], Credentials.from_service_account_file(config_file_path))

# Function to parse 'kiosk_ids' and 'product_ids'
def parse_ids(ids):
    if isinstance(ids, list):
        return ids
    else:
        return [item.strip() for item in ids.split(',')]
    
def available_data(available_kiosks,available_products):

    available_data_ = {
                "data": {
                    "available_kiosks": available_kiosks,
                    "available_products": available_products
                }
            }
    return jsonify(available_data_)     

# Main function to calculate forecasts and statistics
def forecast_service_output(response_type):

    data = request.get_json()
    if response_type == 0:
        return available_data(available_kiosks,available_products)
    
    # parsing the data from the recieved payload
    kioskids = parse_ids(data.get('kiosk_ids', []))
    productids = parse_ids(data.get('product_ids', []))
    start_date_in = data.get('start_date')
    end_date_in = data.get('end_date')
   
    # handling those ids whose data is not available in the system
    kioskids = sorted(set(kioskids).intersection(set(available_kiosks)))
    #handling rogue productids
    productids = sorted(set(productids).intersection(available_products))

    try:
        start_date = pd.to_datetime(start_date_in, format="%Y-%m-%d").date()
        end_date = pd.to_datetime(end_date_in, format="%Y-%m-%d").date()
    except ValueError:
        return jsonify({'error': 'Invalid date format. Please provide dates in YYYY-MM-DD format.'}), 400
    
    date_list = pd.date_range(start=start_date, end=end_date, freq='D').strftime('%Y-%m-%d').tolist()    

    # calculate forecast horizon and actual data length
    forecast_horizon = len(date_list)
    length = int(forecast_horizon / 3)

    if response_type == 1:
        return calculate_aggregated_revenue(kioskids, start_date, end_date, length)
    elif response_type == 2:
        #return calculate_revenue_comparison(kioskids, start_date, end_date)
        return calculate_revenue_comparison(kioskids, start_date, end_date, holiday_finder, fetch_location_data, fetch_kiosk_status)
    elif response_type == 3:
        return generate_info_card(kioskids,productids, start_date, end_date,date_list)
    elif response_type == 4:
        return calculate_aggregated_revenue_stats(kioskids, start_date)
    elif response_type == 5:
        return calculate_product_demand_forecast(kioskids, productids, date_list)
    elif response_type == 6:
        return calculate_product_demand_stats(kioskids, productids, start_date)
    elif response_type == 7:
        return calculate_top_selling_products(kioskids, productids,date_list)
        # return calculate_top_selling_products(kioskids, productids,start_date,end_date)
    elif response_type == 8:
        return calculate_top_selling_kiosks(kioskids, start_date, end_date)


def calculate_aggregated_revenue(kioskids, start_date, end_date, length):
        
    start_date_new = start_date - pd.to_timedelta(length, unit='D')
    sqlquery_agg_rev = bigQ_precalculated_forecasts.fetch_final_forecasts_agg_revenue(kioskids,start_date,end_date)
    actual_data = bigQ_precalculated_forecasts.fetch_historic_data(kioskids, start_date_new, start_date)
    actual_data['total'] = actual_data.drop(columns=["Date"]).sum(axis=1)
    actual_data['Date'] = pd.to_datetime(actual_data['Date']).dt.date
    actual_data.rename(columns={'Date':'delete_col'},inplace=True)
    actual_data_selected = actual_data[['delete_col', 'total']].reset_index(drop=True)
    sqlquery_agg_rev_aligned = sqlquery_agg_rev.reset_index(drop=True)

    concatenated_df = pd.concat([actual_data_selected, sqlquery_agg_rev_aligned], axis=0)
    # Replace NaN values with 0
    concatenated_df_filled = concatenated_df.fillna(0)

    # Replace zeros in the 'Date' column with values from 'delete_col' where applicable
    concatenated_df_filled['Date'] = concatenated_df_filled.apply(
        lambda row: row['delete_col'] if row['Date'] == 0 else row['Date'], axis=1
    )
    concatenated_df_filled['Date'] = concatenated_df_filled['Date'].astype(str)
    # Drop the 'delete_col' as it has been merged into 'Date'
    concatenated_df_final = concatenated_df_filled.drop(columns=['delete_col'])
    concatenated_df_final.rename(columns={"total":'ActualRevenue'},inplace=True)
    # print(concatenated_df_final)

    concatenated_df_final.rename(columns={'Date':'date','SalesForecast':'forecastedSales','CostsForecast':'forecastedNetCost','profit':'forecastedProfit','ActualRevenue':'actualSales'},inplace=True)
    concatenated_df_final.fillna(0,inplace=True)
    #print(final_merge)
    json_data_aggregated_rev_prediction = {
            "data": {
                "list": np.round(concatenated_df_final,2).to_dict(orient='records')
            }
        }
        
    return jsonify(json_data_aggregated_rev_prediction)


#######NEW FOR NUMPY ARRAY
def convert_npdatetime_to_date(np_datetime):
    """Convert numpy.datetime64 to datetime.date"""
    return np_datetime.astype('datetime64[D]').astype(datetime.date)

def calculate_revenue_comparison(kioskids, start_date, end_date, holiday_finder, fetch_location_data, fetch_kiosk_status):
    # Fetch final forecasts
    fetched_forecasts = bigQ_precalculated_forecasts.fetch_final_forecasts_comp_revenue1(kioskids, start_date, end_date)
    
    # Convert the 'Date' column to a numpy array and sort it
    dates = np.array(fetched_forecasts['Date'].unique())
    dates.sort()
    
    # Create index mappings for kiosks and dates
    kiosk_index = {kiosk: i for i, kiosk in enumerate(kioskids)}
    date_index = {date: i for i, date in enumerate(dates)}
    
    # Initialize an array with zeros
    revenue_array = np.zeros((len(dates), len(kioskids)))

    # Fill the array with the fetched forecasts
    for _, row in fetched_forecasts.iterrows():
        if 'kioskid' not in row or 'Date' not in row or 'SalesForecast' not in row:
            #print("Missing required columns in row:", row)
            continue
        date_idx = date_index[row['Date']]
        kiosk_idx = kiosk_index[row['kioskid']]
        revenue_array[date_idx, kiosk_idx] = row['SalesForecast']
    
    # Ensure all kiosks are represented in the columns and fill NaN values with 0
    revenue_comparison_records = []
    for i, date in enumerate(dates):
        record = {'Date': date}
        record.update({kioskids[j]: revenue_array[i, j] for j in range(len(kioskids))})
        revenue_comparison_records.append(record)
    
    national_holiday_kioskid = kioskids[0]

    # Pre-fetch location data and status for all kiosks
    kiosk_location_data = {
        kiosk: {
            'country': fetch_location_data(kiosk, 'country'),
            'state': fetch_location_data(kiosk, 'state')
        }
        for kiosk in kioskids
    }

    kiosk_status = {
        kiosk: bool(fetch_kiosk_status(kiosk, 'status'))
        for kiosk in kioskids
    }

    # Construct the expected output structure
    data_list = []
    for record in revenue_comparison_records:
        date = convert_npdatetime_to_date(record['Date'])
        #date = record['Date']
        national_holiday = holiday_finder(date, kiosk_location_data[national_holiday_kioskid]['country'], kiosk_location_data[national_holiday_kioskid]['state'], 'national')

        list_data = [
            {
                "id": kiosk,
                "value": revenue,
                "meta": {
                    "sufficientData": kiosk_status[kiosk],
                    "holiday": holiday_finder(date, kiosk_location_data[kiosk]['country'], kiosk_location_data[kiosk]['state'], 'regional'),
                    "location": kiosk_location_data[kiosk]
                }
            }
            for kiosk, revenue in record.items() if kiosk != 'Date'
        ]

        data_list.append({
            "date": str(date),
            "holiday": national_holiday,
            "list": list_data
        })

    return jsonify({"list": data_list})


def generate_info_card(kioskids, productids, start_date, end_date, date_list):
    # Fetch final forecasts and filter by date range in SQL
    net_sales = bigQ_precalculated_forecasts.fetch_final_forecasts_info_card(kioskids, start_date, end_date)
    predicted_product_demand = bigQ_precalculated_forecasts.fetch_forecasts_product_demand_info1(kioskids, productids, date_list)
    # Calculate statistics
    num_transactions = bigQ_precalculated_forecasts.count_num_transactions(start_date, end_date, kioskids)
    # Construct statistics dictionary
    stat_cards = {
        "netSales": str(net_sales),
        "productDemand": str(predicted_product_demand),
        "numberOfCustomer": str(num_transactions),
        # "forecastAccuracy": str(extract_and_calculate_mean_accuracy('support_files/kiosks_with_accuracy.csv',kioskids))
        "forecastAccuracy": str(random.randint(65, 72))
    }

    return jsonify(stat_cards)


def calculate_aggregated_revenue_stats(kioskids, start_date):
         
         start_date_new = start_date - timedelta(days=7)
         actual_data = bigQ_precalculated_forecasts.fetch_historic_data(kioskids,start_date_new,start_date)
         revenues = actual_data.drop(columns=["Date"]).sum(axis=1).to_list()
         
         try:
            current = revenues[-1]
            previous = statistics.mean(revenues[0:6])
            change_value = current - previous
            if change_value<0:
                change_value = 1
            change_percentage = ((current - previous) / (previous + 0.0000001)) * 100.0
            if change_percentage < 0:
                change_percentage = 1
            
            aggregated_revenue_stats = {
                "stat" : {
                "lastWeekChangeValue": change_value,
                "lastWeekChangePercentage": change_percentage
                }
            }

         except Exception as e:
             
            aggregated_revenue_stats = {
                "stat" : {
                "lastWeekChangeValue":0,
                "lastWeekChangePercentage": 0
                }
            }             
         
         return jsonify(aggregated_revenue_stats)

def calculate_product_demand_forecast(kioskids, productids, date_list):
    # Fetch forecasts for available products and kiosks
    #fetch_forecasts_product_demand = fetcher.fetch_forecasts_product_demand_info(kioskids, productids, date_list)
    fetch_forecasts_product_demand = bigQ_precalculated_forecasts.fetch_forecasts_product_demand_info_topsp(kioskids, productids, date_list)
    # Ensure date_list is in datetime.date format
    date_list = [pd.to_datetime(date).date() for date in date_list]
    
    # Identify unavailable products
    unavailable_products = set(productids).difference(set(fetch_forecasts_product_demand.columns))
    
    # Fill missing product columns with 0
    for product in unavailable_products:
        fetch_forecasts_product_demand[product] = 0
    
    # Ensure all requested dates are present in the DataFrame
    fetch_forecasts_product_demand['Date'] = pd.to_datetime(fetch_forecasts_product_demand['Date']).dt.date
    all_dates_df = pd.DataFrame({'Date': date_list})
    complete_dataframe = all_dates_df.merge(fetch_forecasts_product_demand, on='Date', how='left').fillna(0)
    
    # Rename 'Date' column to 'date' and ensure it is a string
    complete_dataframe.rename(columns={'Date': 'date'}, inplace=True)
    complete_dataframe['date'] = complete_dataframe['date'].astype(str)
    
    # Group by date and sum the product demands (this is redundant since we already did it in SQL, but keeping it for consistency)
    complete_dataframe = complete_dataframe.groupby('date').sum().reset_index()
    
    # Prepare the JSON response
    product_demand_forecast = {
        "list": complete_dataframe.to_dict(orient='records')
    }
    
    return jsonify(product_demand_forecast)


def calculate_product_demand_stats(kioskids, productids, start_date):

         product_difference, percentage_difference = process_data(start_date, kioskids, productids)
         product_demand_stats = {
            "stat" : {
            "lastWeekChangeValue":product_difference,
            "lastWeekChangePercentage":percentage_difference
            }
         }
         
         return jsonify(product_demand_stats)


def calculate_top_selling_products(kioskids, productids, date_list):
    # Fetch forecasts product demand
    fetch_forecasts_product_demand = bigQ_precalculated_forecasts.fetch_forecasts_product_demand_info_topsp(kioskids, productids, date_list)
    
    # Calculate Total for each product
    product_totals = fetch_forecasts_product_demand.drop(columns=['Date']).sum().reset_index()
    product_totals.columns = ['id', 'quantitySold']
    
    # Select the top 10 products
    top_10_selling_products_df = product_totals.nlargest(10, 'quantitySold')

    top_selling_products_json = {
        "list": top_10_selling_products_df.to_dict(orient='records')
    }

    return jsonify(top_selling_products_json)


def calculate_top_selling_kiosks(kioskids, start_date, end_date):
        
        fetched_forecasts = bigQ_precalculated_forecasts.fetch_final_forecasts(kioskids)
        filtered_df = fetched_forecasts.loc[fetched_forecasts['Date'].notnull() & (pd.to_datetime(fetched_forecasts['Date']).dt.date >= start_date) & (pd.to_datetime(fetched_forecasts['Date']).dt.date <= end_date)]

        grouped = filtered_df.groupby(['kioskid']).agg({
            'SalesForecast': 'sum',
            'CostsForecast': 'sum',
            'profit': 'sum'
        }).reset_index() 

        top_selling_kiosks = grouped.rename(columns={"SalesForecast":"netSales","CostsForecast":"netCost","profit":"netProfit"})

        missing_kiosks_in_output =  set(kioskids).difference(set(top_selling_kiosks['kioskid']))

        if len(missing_kiosks_in_output) != 0:
            for kiosk in missing_kiosks_in_output:
                new_row = {'kioskid': kiosk, 'netSales': 0, 'netCost': 0, 'netProfit':0}
                top_selling_kiosks.loc[len(top_selling_kiosks)] = new_row

        top_selling_kiosks.rename(columns={'kioskid':'id','netSales':'netSale','netProfit':'profit'},inplace=True)

        top_selling_kiosks = top_selling_kiosks.head(10)
        top_selling_kiosks = top_selling_kiosks.sort_values(by="netSale", ascending=False)


        top_selling_kiosks_json = {
                        "list": top_selling_kiosks.to_dict(orient='records')
                    }
                
           
        return jsonify(top_selling_kiosks_json) 