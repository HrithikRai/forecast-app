import holidays

country_codes = {
        'Deutschland': 'DE',
        'Germany': 'DE',
        'Switzerland' : 'CH',
        'Austria' : 'AT',
        'Österreich' : 'AT'
    }

state_codes = {
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
    'canton of Zürich': 'ZH',
    'canton of Bern': 'BE_CH',
    'Canton of Lucerne': 'LU',
    'Uri': 'UR',
    'Schwyz': 'SZ',
    'Obwalden': 'OW',
    'Nidwalden': 'NW_CH',
    'Glarus': 'GL',
    'Canton of Zug': 'ZG',
    'Fribourg': 'FR',
    'Solothurn': 'SO',
    'Basel-City': 'BS',
    'Basel-Landschaft': 'BL',
    'Canton of Schaffhausen': 'SH_CH',
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
    # Austrian States
    'Burgenland': 'B',
    'Carinthia': 'K',
    'Lower Austria': 'NÖ',
    'Upper Austria': 'OÖ',
    'Salzburg': 'S',
    'Styria': 'STY',
    'Steiermark': 'STY',
    'Tyrol': 'T',
    'Vorarlberg': 'V',
    'Vienna': 'W'
}


def holiday_finder(date,country,state,cat):

    if country == 'Germany' or country == 'Deutschland':
        national_holidays = holidays.DE()
    elif country == 'Austria' or country == 'Österreich':
        national_holidays = holidays.AT()
    elif country == 'Switzerland':
        national_holidays = holidays.CH()
    else: national_holidays = holidays.DE()

    try:
        regional_holidays = holidays.country_holidays(country_codes[country], subdiv=state_codes[state])
    except:
        regional_holidays = holidays.country_holidays(country_codes['Germany'], subdiv=state_codes['NRW'])
    
    if cat == 'regional':
        if date in regional_holidays:
            return regional_holidays.get(date)
    
    elif cat == 'national':
        if date in national_holidays:
            return national_holidays.get(date)
        
    else:
        return ''
    
