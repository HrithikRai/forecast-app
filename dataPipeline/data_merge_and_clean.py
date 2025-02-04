import pandas as pd

class DataMerge:
    """
    A class for Data Preprocessing of Transaction database.
    """
    def __init__(self, final_products, final_transactions):
        """
        Initializes the data to be merged.
        """
        self.final_products = final_products
        self.final_transactions = final_transactions

    def data_merge_clean(self):
        """
        Merge the selected data from transaction and product into one and clean the data
        """
        final_data = pd.DataFrame()
        final_data = pd.merge(self.final_products, self.final_transactions)
        final_data = final_data.sort_values("Date")
        df_val = final_data
        final_data = df_val.dropna(subset=['Date'])
        final_data = final_data.sort_values("Date")
        final_data['Date'] = final_data['Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_data['KioskId'] = final_data['KioskId'].apply(lambda x: str(x))
        final_data['ProductId'] = final_data['ProductId'].apply(lambda x: str(x))
        final_data = final_data.reset_index()
        final_data = final_data.drop(columns=["index"])
        return final_data
