from dataProcessing.load_data import *
from dataPipeline.data_imputation import *
from forecastApi.utils.functions_used_by_app_file import *
from flask import Flask
from flask import Flask, request, jsonify, after_this_request
from datetime import datetime
from google.cloud import bigquery
from dataProcessing.load_data import BigQueryManager
from forecastApi.utils.functions_used_by_app_file import parse_ids
from forecastApi.utils.functions_used_by_app_file import forecast_service_output
from google.auth import credentials
import warnings 
import gc

warnings.filterwarnings('ignore')

app = Flask(__name__)

@app.route("/", methods=["GET","POST"])
def home():
    with open("home.html", "r") as file:
        home_page = file.read()
    return home_page

@app.route("/fetch_kiosk_ids", methods=["GET","POST"])
def fetch_kiosk_ids():

    response = forecast_service_output(0)
    return response

@app.route("/aggregated_revenue", methods=["POST"])
def aggregated_revenue_response():
  
    response = forecast_service_output(1)
    return response

@app.route("/revenue_comparision", methods=["POST"])
def revenue_comparision_response():

    response = forecast_service_output(2)
    return response

@app.route("/generate_info_card", methods=["POST"])
def generate_info_card_response():

    response = forecast_service_output(3)
    return response

@app.route("/aggregated_revenue_stats", methods=["POST"])
def aggregated_revenue_response_stats():
   
    response = forecast_service_output(4)
    return response

@app.route("/product_demand_forecast", methods=["POST"])
def product_demand_response():
        
    response = forecast_service_output(5)
    return response

@app.route("/product_demand_forecast_stats", methods=["POST"])
def product_demand_response_stats():
  
    response = forecast_service_output(6)
    return response

@app.route("/top_selling_products_table", methods=["POST"])
def top_selling_products_table():
    
    response = forecast_service_output(7)
    return response

@app.route("/top_selling_kiosks_table", methods=["POST"])
def top_selling_kiosks_table():
    response = forecast_service_output(8)
    return response

@app.after_request
def call_gc(response):
    gc.collect()
    return response

if __name__ == '__main__':
    app.run(port=5000,debug=True)