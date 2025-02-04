import pandas as pd

class GenerateSalesTables:

    def __init__(self,processed_dataframe):

        self.processed_dataframe = processed_dataframe

    def kiosks_data(self):

        if 'index' in self.processed_dataframe.columns:
          self.processed_dataframe.drop(['index'],axis=1,inplace=True)

        data = self.processed_dataframe.drop(['ProductId','KioskName','ProductName'],axis=1)
        data = data.dropna(subset=['KioskId'])

        data['GrossPrice'] = data['GrossPrice'].astype(float)
        data['cost_price'] = data['cost_price'].astype(float)
        data["Date"] = pd.to_datetime(data["Date"]).dt.date.astype(str)

        grouped_df = data.groupby(['Date', 'KioskId'])[['GrossPrice', 'cost_price']].sum().reset_index()
        return grouped_df
    
    def generate_sales_data(self):

        grouped_df = self.kiosks_data()
        sales_df = grouped_df.pivot_table(index='Date', columns='KioskId', values='GrossPrice', aggfunc='sum', fill_value=0)
        sales_df.reset_index(inplace=True)
        sales_df.fillna(0, inplace=True)
        sales_df.columns.name = None
        sales_df = sales_df[sales_df.columns[:-1]]
        return sales_df

    def generate_cost_data(self):

        grouped_df = self.kiosks_data()
        cost_df = grouped_df.pivot_table(index='Date', columns='KioskId', values='cost_price', aggfunc='sum', fill_value=0)
        cost_df.reset_index(inplace=True)
        cost_df.fillna(0, inplace=True)
        cost_df.columns.name = None
        cost_df = cost_df[cost_df.columns[:-1]]
        return cost_df
    
    def generate_agg_sales(self,sales_df,cost_df):

        net_sales = pd.DataFrame({'Date': sales_df.Date, 'NetSales': sales_df.sum(axis=1).values})
        net_cost = pd.DataFrame({'Date': cost_df.Date, 'NetCost': cost_df.sum(axis=1).values})
        agg_df = pd.merge(net_sales, net_cost, on='Date')
        agg_df['NetProfit'] = agg_df['NetSales'] - agg_df['NetCost']
        return agg_df


    



        