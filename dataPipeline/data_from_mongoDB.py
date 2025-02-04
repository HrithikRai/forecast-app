import os
import pymongo
import json
import certifi
import pandas as pd
from pathlib import Path

class DataProcessFromMongoDB:
    """
    A class for Data Preprocessing of Product and Revenue database.
    """

    def __init__(self):
        """
        Initializes the database connections
        """
        # don't need to run everytime but only if data is updated in MongoDB or can be schedueled. 
        
        current_dir = Path(__file__).resolve().parents[2]
        config_dir = os.path.join(current_dir, "config_files")
        config_file = os.path.join(config_dir, "mongo_db_connection.json")

        with open(config_file, 'r') as f:
            config_data = json.load(f)

        self.db_connection = config_data['db_connection']
        self.client = pymongo.MongoClient(self.db_connection, tlsCAFile=certifi.where())
        self.database = self.client['livello-backend-staging']
        self.final_products = pd.DataFrame()
        self.final_transactions = pd.DataFrame()
        self.company_data = pd.DataFrame()
        self.collection = []

    def product_data_from_mongodb(self):
        """
        Data operations to select only columns important for forecasting from productlines document
        """
        self.collection = self.database['productlines']
        products = []
        for x in self.collection.find():
            products.append(x)

        products_df = pd.DataFrame.from_dict(products)

        self.final_products['ProductId'] = products_df['_id']
        self.final_products['ProductName'] = products_df['name']
        self.final_products['cost_price'] = products_df['defaultCost']

        return self.final_products

    def transaction_data_from_mongodb(self):

        """
        Data operations to select only columns important for forecasting from transaction document
        """
        self.collection = self.database['transactions']
        dummy = []
        for x in self.collection.find():
            dummy.append(x)

        sorted_df = pd.json_normalize(dummy, record_path='itemsPurchased')
        # print(df)

        #sorted_df = df.sort_values("created")
        #   print(sorted_df)

        # we might need to check for different timestamp
        sorted_df['created'] = pd.to_datetime(sorted_df['created']).dt.tz_localize(None)
        sorted_df['expirationDate'] = pd.to_datetime(sorted_df['expirationDate']).dt.tz_localize(None)

        self.final_transactions['Date'] = sorted_df['created']
        self.final_transactions['ProductId'] = sorted_df['productLine']
        self.final_transactions['KioskId'] = sorted_df['kiosk']
        self.final_transactions['GrossPrice'] = sorted_df['price']
        self.final_transactions['KioskName'] = sorted_df['kioskName']
        return self.final_transactions
    
    def organization_data_from_mongodb(self):

        """
        Data operations to get the kiosks and their respective owner organizations
        """

        self.collection = self.database['kiosks']
        kiosks_companies = []
        for x in self.collection.find():
            kiosks_companies.append(x)

        company_df = pd.DataFrame.from_dict(kiosks_companies)
        self.company_data['KioskId'] = company_df['_id']
        self.company_data['CompanyId'] = company_df['ownerOrganization']
        self.company_data['CompanyName'] = company_df['name']
        self.company_data['location'] = company_df['location']

        return self.company_data
