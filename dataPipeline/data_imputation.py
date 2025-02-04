import pandas as pd
import holidays
import datetime as dt
import meteomatics.api as api
import matplotlib.pyplot as plt
from meteostat import Point, Daily
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="my_user_agent")
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()

country_codes = {
        'Deutschland': 'DE',
        'Germany': 'DE',
        'Switzerland' : 'CH',
        'Austria' : 'AT',
        'Österreich' : 'AT'
    }

state_codes = {
    # German States
    'Brandenburg': 'BB',
    'Berlin': 'BE',
    'Baden-Württemberg': 'BW',
    'Bavaria': 'BYP',
    'Bremen': 'HB',
    'Hesse': 'HE',
    'Hamburg': 'HH',
    'Mecklenburg-Vorpommern': 'MV',
    'Lower Saxony': 'NI',
    'North Rhine-Westphalia': 'NW',
    'NRW':'NW',
    'Rhineland-Palatinate': 'RP',
    'Schleswig-Holstein': 'SH',
    'Saarland': 'SL',
    'Saxony': 'SN',
    'Saxony-Anhalt': 'ST',
    'Thuringia': 'TH',
    'Brandenburg': 'BB',
    'Berlin': 'BE',
    'Baden-Württemberg': 'BW',
    'Bayern': 'BYP',
    'Bremen': 'HB',
    'Hessen': 'HE',
    'Hamburg': 'HH',
    'Mecklenburg-Vorpommern': 'MV',
    'Niedersachsen': 'NI',
    'Nordrhein-Westfalen': 'NW',
    'Rheinland-Pfalz': 'RP',
    'Schleswig-Holstein': 'SH',
    'Saarland': 'SL',
    'Sachsen': 'SN',
    'Sachsen-Anhalt': 'ST',
    'Thüringen': 'TH',
    # Swiss Cantons
    'Canton of Zürich': 'ZH',
    'canton of Bern': 'BE',
    'Canton of Lucerne': 'LU',
    'Uri': 'UR',
    'Schwyz': 'SZ',
    'Obwalden': 'OW',
    'Nidwalden': 'NW',
    'Glarus': 'GL',
    'Canton of Zug': 'ZG',
    'Fribourg': 'FR',
    'Solothurn': 'SO',
    'Basel-City': 'BS',
    'Basel-Landschaft': 'BL',
    'Canton of Schaffhausen': 'SH',
    'Appenzell Ausserrhoden': 'AR',
    'Appenzell Innerrhoden': 'AI',
    'St. Gallen': 'SG',
    'Graubünden': 'GR',
    'Aargau': 'AG',
    'Thurgau': 'TG',
    'Ticino': 'TI',
    'Vaud': 'VD',
    'Valais': 'VS',
    'Neuchâtel': 'NE',
    'Geneva': 'GE',
    'Jura': 'JU',
    #Austrian States
    'Burgenland': '1',
    'Carinthia': '2',
    'Lower Austria': '3',
    'Upper Austria': '4',
    'Salzburg': '5',
    'Styria': '6',
    'Tyrol': '7',
    'Vorarlberg': '8',
    'Vienna': '9'

}    

def holiday_finder(date, country, state, cat, national_holidays):

    country_code = country_codes.get(country, 'DE')
    state_code = state_codes.get(state)

    if cat == 'regional' and state_code:
        regional_holidays = holidays.country_holidays(country_code, subdiv=state_code)
        return date in regional_holidays
    
    if cat == 'national':
        national_holidays = national_holidays.get(country_code, holidays.DE())
        return date in national_holidays
    
    return False


def impute_revenue(row, data, window_size=5):

    # Impute missing data points in the time series
    date = row['Date']  
    column_name = data.columns[1]
    missing_revenue = row[column_name]
    
    if date.dayofweek >= 5:  # If Saturday (5) or Sunday (6)
        return missing_revenue
    
    local_mean = data[(data['Date'] >= date - pd.DateOffset(days=window_size)) & 
                      (data['Date'] <= date + pd.DateOffset(days=window_size))][column_name].mean()
    
    similar_days = data[(data['Date'] < date) & (data['Date'].dt.dayofweek == date.dayofweek)]
    similar_days = similar_days.dropna(subset=[column_name]).tail(window_size) 
    similar_mean = similar_days[column_name].mean() if not similar_days.empty else data[column_name].mean()
    imputed_value = 0.7 * local_mean + 0.3 * similar_mean
    
    if pd.isna(missing_revenue):
        return imputed_value
    
    return missing_revenue

def transform_into_ts(data,KioskId,kiosk_location,national_holidays):
    
    kiosk_region = kiosk_location['state']
    kiosk_country = kiosk_location['country']

    city =kiosk_location['city']
    country = kiosk_country
    loc = geolocator.geocode(city+','+ country)

    data = data.drop(['index','ProductName','KioskName'],axis=1)
    data = data.dropna(subset=['KioskId'])

    grouped_data = data.groupby('KioskId')
    k1 = grouped_data.get_group(KioskId).copy()
    k1['Date'] = pd.to_datetime(k1['Date'])

    start = k1.Date.min()
    end = k1.Date.max()
    location = Point(loc.latitude, loc.longitude)
    temp = Daily(location, start, end)

    temp = temp.fetch()

    temp['Date'] = temp.index

    k1['Date'] = pd.to_datetime(k1['Date']).dt.date
    temp['Date'] = pd.to_datetime(temp['Date']).dt.date ##
    temp[['tavg','prcp','wspd','pres']] = scaler.fit_transform(temp[['tavg','prcp','wspd','pres']])
    temp.drop(['tmin','tmax','snow','wdir','wpgt','tsun'],axis=1,inplace=True)
    k1 = k1.merge(temp,on='Date',how='left')
    k1.fillna(0,inplace=True)
    k1['Date'] = pd.to_datetime(k1['Date'])
    k1.index = k1.Date
    k1.drop(['Date'],axis=1,inplace=True)
    k1_rev = k1.resample('D').sum()
    k1_rev.rename(columns={"GrossPrice":"revenue"},inplace=True)
    k1_rev['Date'] = k1_rev.index
    k1_rev = k1_rev[['Date','revenue','tavg','prcp','wspd','pres']]

    print(k1_rev.shape)

    product_count_df = k1.groupby(['Date', 'ProductId']).size().unstack(fill_value=0)
    product_count_df.reset_index(inplace=True)
    product_count_df.columns.name = None
    product_count_df['Date'] = pd.to_datetime(product_count_df['Date'])
    product_count_df.index = product_count_df.Date
    product_count_df.drop(['Date'],axis=1,inplace=True)
    product_count_df = product_count_df.resample('D').sum()
    product_count_df['Date'] = product_count_df.index

    k1_rev.reset_index(drop = True, inplace = True)
    product_count_df.reset_index(drop = True, inplace = True)
    final_df = pd.merge(k1_rev, product_count_df, on="Date")
    final_df.sort_values(by='Date',inplace=True)
    final_df['Date'] = final_df['Date'].astype(str)

    final_df['national_holiday'] = final_df.apply(lambda row: holiday_finder(row['Date'],kiosk_country,kiosk_region,'national', national_holidays), axis=1)
    final_df['regional_holiday'] = final_df.apply(lambda row: holiday_finder(row['Date'],kiosk_country,kiosk_region,'regional', national_holidays), axis=1)
    final_df['national_holiday'] = final_df['national_holiday'].astype(int)
    final_df['regional_holiday'] = final_df['regional_holiday'].astype(int)

    ### Filter final_df to include only the dates present in the original data
    data_date = grouped_data.get_group(KioskId).copy().reset_index(drop=True)
    data_date['Date'] = pd.to_datetime(data_date['Date']).dt.date
    original_dates = data_date['Date'].unique()
    final_df = final_df[final_df['Date'].isin(original_dates.astype(str))]
    print(final_df.shape)
    return final_df

def perform_imputation(time_series_df):
    
    """
    Make sure that the time_series_df has only 2 columns - Date, Value
    """
    # complete the date range and make it continuous 
    date_range = pd.date_range(start=time_series_df['Date'].min(), end=time_series_df['Date'].max(), freq='D')
    complete_dates = pd.DataFrame({'Date': date_range})

    complete_dates["Date"] = pd.to_datetime(complete_dates["Date"]).dt.date   
    time_series_df["Date"] = pd.to_datetime(time_series_df["Date"]).dt.date

    complete_data = complete_dates.merge(time_series_df, on='Date', how='left')
    complete_data["Date"] = pd.to_datetime(complete_data["Date"])

    # performing data imputation
    column = complete_data.columns[1]
    complete_data[column] = complete_data.apply(lambda row: impute_revenue(row, complete_data, 5), axis=1)
    complete_data[column] = complete_data[column].interpolate(method='linear',axis=0)

    complete_data = complete_data.fillna(0)
    complete_data.Date = complete_data.Date.dt.date

    return complete_data
