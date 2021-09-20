###############
# Custom module of scripts for day-to-day work, particularly for processing
# and querying large public-record parcel files.
###############

import pandas as pd
import numpy as np
from pathlib import Path
from sodapy import Socrata
import re
import os
pd.set_option('max_colwidth', 0)
pd.set_option('max_columns', 0)

KCT = os.environ['KCT']


def bulk_oc_data_dfs(dir, sheet_name="company_data" ):

    sheet_name_dict = {
        "company_data":0,
        "officers":1,
        "filings":2,
        "events":3
    }

    sheet_name = sheet_name_dict[sheet_name]

    '''Takes in a folder of multiple LLCs and return a single DataFrame with 
    company_data sheets collated into one. Only works with the 'company_data' sheet.'''
    llc_dfs = []
    for index, path in enumerate(Path(dir).rglob('*.xlsx')):

        xl_file = path.absolute()
        df = pd.read_excel(
            xl_file, 
            sheet_name=sheet_name, 
            squeeze=True, 
            index_col=0, 
            header=0)
        if sheet_name == 0 and df.iloc[2] != "Active" and df.iloc[2] != "Good Standing":
            df = df.drop(index='dissolve_date')
        llc_dfs.append(df)

    bulk_df = pd.concat(llc_dfs, axis=1)
    bulk_df = bulk_df.drop_duplicates(keep=False).transpose()
    return bulk_df

def extract_kc_metro_cities(addr):
    """For use in a Series.apply lambda expression. 
    Will return two variables in a tuple: (matched_city, addr_with_city_removed).

    ---------
    Example: property_df["city"] = property_df["address"].apply( lambda x: extract_kc_metro_cities(x)[0])
             property_df["address"] = property_df["address"].apply( lambda x: extract_kc_metro_cities(x)[1])"""

    
    
    city_file = os.path.join(KCT, "kc_parcel_data/kc_metro_cities.txt")
    with open(city_file, "r") as f:
        cities = f.read()
        cities = cities.split(',')
    for city in cities:
        city_lower = city.lower().replace("'", '')
        addr_lower = addr.lower().replace("'", '')
        if city_lower in addr_lower:
            return (city, addr)
    



def extract_fuzzy_match(query_series, choice_series,limit=1, scorer="WRatio"):
    from statistics import mean
    from rapidfuzz import fuzz, process
    """
    Takes in two series, returns a DataFrame of the matches.
    
    Parameters:

    ---------

     query series : pandas Series or list
            Series of values you wish to match.

     choice_series: pandas Series or list
            Series to query against.

     limit: int
            Number of matches result to return, default=1.
    
    scorer: default "WRatio"
            rapidfuzz.fuzz scoring algorithm. also includes 
            "partial_ratio", "token_set_ratio" "ratio" etc.

    Returns

    --------

    match_df: pandas.DataFrame
        DataFrame with the matches, scores and indices as well as average
        match score.

      """
    def check_if_series(item):
        if not isinstance(item, pd.Series):
            item = pd.Series(item)
        return item

    query_series = check_if_series(query_series)
    choice_series = check_if_series(choice_series)

    scorer_dict = {
        'ratio': fuzz.ratio,
        'partial_ratio': fuzz.partial_ratio,
        'token_set_ratio': fuzz.token_set_ratio,
        'partial_token_set_ratio': fuzz.partial_token_set_ratio,
        'token_sort_ratio': fuzz.token_sort_ratio,
        'partial_token_sort_ratio': fuzz.partial_token_sort_ratio,
        'token_ratio': fuzz.token_ratio,
        'partial_token_ratio': fuzz.partial_token_ratio,
        'WRatio': fuzz.WRatio,
        'QRatio': fuzz.QRatio
        }

    scorer = scorer_dict[scorer]

    matches = query_series.apply(
        lambda x: process.extract(x, choice_series, limit=limit, scorer=scorer)
        )

    master_match_list = []
    name = {
        0:"match",
        1:"score",
        2:"index"
    }
    for index, sub_list in enumerate(matches):
        match_dict = {}
        match_dict['query'] = query_series.iloc[index]
        average_list = []
        for match_num, tuple in enumerate(sub_list):
            suffix= f"_{match_num+1}"
            for i, item in enumerate(tuple):
                key = f"{name[i]+suffix}"
                value = sub_list[match_num][i]
                if i == 1:
                    average_list.append(value)
                match_dict.update({key:value})
        match_dict['score_avg'] = mean(average_list)
        master_match_list.append(match_dict)
    match_df = pd.DataFrame(master_match_list)

    return match_df

def get_full_kc_parcels(city_owned=True, masked=False):

    file_path = os.path.join(KCT, 'kc_parcel_data/kc_parcel_data.csv')
    kc_df = pd.read_csv(file_path, dtype='object')
    kc_df.columns = kc_df.columns.str.lower().str.replace(" ", "_")
    kc_df = kc_df.replace(np.nan, '')
    category_columns = ['own_city','own_state','own_zip','prefix',
                        'street_type','landusecode','block'
                        ]
    kc_df[category_columns] = kc_df[category_columns].astype('category')
    if not city_owned:
        city_owned = pd.read_csv(os.path.join(KCT, 'kc_parcel_data/city_owned.csv'), dtype='object')
        city_owned.columns = city_owned.columns.str.lower()
        kc_df = pd.concat([kc_df, city_owned]).drop_duplicates(subset=['kivapin', 'apn'], keep=False)
    if masked:
        mask = ['own_name', 'own_name2', 'own_addr',
       'own_addr2', 'own_city', 'own_state', 'own_zip', 'address', 'addr',
       'fraction', 'prefix', 'street', 'street_type',]

        kc_df = kc_df[mask]
    
    return kc_df


class GetKCOpenData():
    
    APP_TOKEN = "SMzaH663n0szHDQXkHY7WiUYT"
    data_set_choices = {
        "311": '7at3-sxhp',
        'parcels': '3vhm-urud'
        
    }



    columns = {
        "311_columns":
            [
                'case_id','source','department','work_group',
        'request_type','category','type','detail','creation_date','creation_time',
        'creation_month','creation_year','status','exceeded_est_timeframe',
        'closed_date','closed_month','closed_year','days_to_close','street_address',
        'address_with_geocode','zip_code','neighborhood','county','council_district',
        'police_district','parcel_id_no','ycoordinate','xcoordinate','case_url'],
        
        "parcel_columns":
        [
            'the_geom','objectid','parceltype','kivapin','apn','platname','lot',
        'own_name','own_addr','own_city','own_state','own_zip','assessed_land_value',
        'assessment_effective_date','legal','shape_area','shape_len','address','addr',
        'prefix','street','street_type','landusecode','assessed_improve_value',
        'exempt_land_value','exempt_improve_value','suite','own_name2','own_addr2',
        'block','tract','fraction'
        ]
    }

    def __init__(self, dataset, ):
        self.dataset = self.data_set_choices[dataset]
        
    def get_dataset(self, query=None):
        if query:
            query = query
        else:
            query = 'select *'

        client = Socrata('data.kcmo.org', self.APP_TOKEN)
        result = client.get(self.dataset, query=query)
        df = pd.DataFrame(result)
        return df
    

def memory_format(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0        
    return '%1.1f%s%s' % (num, unit, 'Yi')

def memory_test(df, dtype=None):
    """ Takes in a DataFrame and returns a formatted view of it's memory usage."""
    df_to_test = df
    if dtype:
        df_to_test = df.astype(dtype)  
    print(df_to_test.memory_usage().apply(lambda x: memory_format(x)))
    memory_total = memory_format(df_to_test.memory_usage().sum())
    print(f'Total used: {memory_total}')

def clean_street_suffixes(addr, remove=False):

    ''' Takes a string and unifies the format of common street suffixes. If remove == True, will remove all suffixes entirely. '''
    import re

    rc = re.compile

    suffixes = [
        (rc('Avenue', re.IGNORECASE), 'Ave'),
        (rc('Street', re.IGNORECASE), 'St'),
        (rc('Terrace', re.IGNORECASE), 'Ter'),
        (rc('Terr', re.IGNORECASE), 'Ter'),
        (rc('\s(Road)\s?', re.IGNORECASE), ' Rd'),
        (rc('Drive', re.IGNORECASE), 'Dr'),
        (rc('Court', re.IGNORECASE), 'Ct'),
        (rc('Boulevard', re.IGNORECASE), 'Blvd'),
        (rc('Place', re.IGNORECASE), 'Pl'),
        (rc('\sLane\s?,', re.IGNORECASE), ' Ln '),
        (rc('Parkway', re.IGNORECASE), 'Pkwy'),
        (rc('Trafficway', re.IGNORECASE), 'Trfy'),
        (rc('(\sCircle\s?$)', re.IGNORECASE), ' Cir '),
        (rc('Highway', re.IGNORECASE), ' Hwy '),
        (rc('Plaza', re.IGNORECASE), ' Plz '),
        (rc('(\sWay\s?)$', re.IGNORECASE), ' Way '),
        (rc('(\sTrail\s?)$', re.IGNORECASE), ' Trl '),
    ]

    remove_suffixes = [
        (rc('(\sAve)$',  re.IGNORECASE), ''),
        (rc('(\sSt)$',  re.IGNORECASE), ''),
        (rc('(\sTer)$', re.IGNORECASE  ), ''),
        (rc('(\sTer)$',  re.IGNORECASE), ''),
        (rc('(\sRd)$',  re.IGNORECASE), ''),
        (rc('(\sDr)$',  re.IGNORECASE), ''),
        (rc('(\sCt)$',  re.IGNORECASE), ''),
        (rc('(\sBlvd)$', re.IGNORECASE), ''),
        (rc('(\sPl)$',  re.IGNORECASE), ''),
        (rc('(\sLn)$', re.IGNORECASE  ), ''),
        (rc('(\sPkwy)$',  re.IGNORECASE), ''),
        (rc('(\sTrfy)$',  re.IGNORECASE), ''),
        (rc('(\sHwy)$',  re.IGNORECASE), ''),
        (rc('(\sPlz)$',  re.IGNORECASE), ''),
        (rc('(\sWay)$',  re.IGNORECASE), ''),
        (rc('(\sTrl)$', re.IGNORECASE), ''),
        (rc('(\sCir)$', re.IGNORECASE), ''),
        (rc('Avenue', re.IGNORECASE),''),
        (rc('Street', re.IGNORECASE),''),
        (rc('Terrace', re.IGNORECASE),''),
        (rc('Terr', re.IGNORECASE),''),
        (rc('\s(Road)\s?', re.IGNORECASE), ''),
        (rc('Drive', re.IGNORECASE),''),
        (rc('Court', re.IGNORECASE),''),
        (rc('Boulevard', re.IGNORECASE),''),
        (rc('Place', re.IGNORECASE),''),
        (rc('\sLane\s?,', re.IGNORECASE), ''),
        (rc('Parkway', re.IGNORECASE),''),
        (rc('Trafficway', re.IGNORECASE),''),
        (rc('(\sCircle\s?$)', re.IGNORECASE), ''),
        (rc('Highway', re.IGNORECASE), ''),
        (rc('Plaza', re.IGNORECASE), ''),
        (rc('(\sWay\s?)$', re.IGNORECASE), ''),
        (rc('(\sTrail\s?)$', re.IGNORECASE), '')
    ]

    if remove:
        for pattern, suffix in remove_suffixes:
            if remove:
                if re.search(pattern, addr):
                    clean_addr = re.sub(pattern, suffix, addr)
                    return clean_addr

    for pattern, suffix in suffixes:
       if re.search(pattern, addr):
            clean_addr = re.sub(pattern, suffix, addr)
            return clean_addr
    return addr
            
        
def remove_symbols(string):
    cleaned_string = re.sub('[\-\,\.\&\%\$\?\!\@\#\"\/\(\)]', '', string)
    return cleaned_string

def clean_columns(col):
    if re.search("\s", col) == None:
        col = '_'.join([s for s in re.split("([A-Z][a-z]+)", col) if s])
    col = re.sub('\'|\,', '', col)
    cleaned_string = re.sub('[\.\-|\s]', "_", col)
    
    return cleaned_string.lower()

        

def fuzzy_match_two_columns(two_cols, scorer="WRatio"):
    from rapidfuzz import fuzz

    ''' 
    Takes a two Series in a DataFrame and returns a DataFrame with the values and their match score.

    Example: fuzzy_match_two_columns(df[['col1', 'col2']]) => 
    q1	                   q2	    score	    index
	18 G LAKE SHORE DR	   18 G ST	85.500000	125394
	405 N SPECK AVE	405    SPECK	85.500000	24272
    '''

    scorer_dict = {
        'ratio': fuzz.ratio,
        'partial_ratio': fuzz.partial_ratio,
        'token_set_ratio': fuzz.token_set_ratio,
        'partial_token_set_ratio': fuzz.partial_token_set_ratio,
        'token_sort_ratio': fuzz.token_sort_ratio,
        'partial_token_sort_ratio': fuzz.partial_token_sort_ratio,
        'token_ratio': fuzz.token_ratio,
        'partial_token_ratio': fuzz.partial_token_ratio,
        'WRatio': fuzz.WRatio,
        'QRatio': fuzz.QRatio
        }

    scorer = scorer_dict[scorer]

    col1 = list(two_cols.iloc[:, 0])
    col2 = list(two_cols.iloc[:, 1])
    index = list(two_cols.index)
    compare = zip(col1, col2, index)
    result = []

    for c in compare:
        score = fuzz.WRatio(c[0], c[1])
        result.append(
            {
                'q1': c[0],
                'q2': c[1],
                'score': score,
                'index': c[2]

            }
        )
    df = pd.DataFrame(result)

    return df
