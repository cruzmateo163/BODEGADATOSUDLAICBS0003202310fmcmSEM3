from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_countries():
    try:
        #Variables
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd =  'admin'
        db = 'fmcmdbstg'

        con_db_stg = Db_Connection(type, host, port, user, pwd, db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging database")
      
         #DICTIONARY FOR VALUES OF countries
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }
        #Reading the CVS file
        countries_csv=pd.read_csv("csvs/countries.csv")
        print(countries_csv)
        #Processing the CVS file content
        if not countries_csv.empty:
            for countryid, countryname, countryregion, countryregionid\
                in zip(countries_csv['COUNTRY_ID'], countries_csv['COUNTRY_NAME'],
                countries_csv['COUNTRY_REGION'], countries_csv['COUNTRY_REGION_ID']):
                countries_dict['country_id'].append(countryid)
                countries_dict["country_name"].append(countryname)
                countries_dict["country_region"].append(countryregion)
                countries_dict["country_region_id"].append(countryregionid)
        if countries_dict["country_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE countries")
            df_countries_ext=pd.DataFrame(countries_dict)
            df_countries_ext.to_sql('countries', ses_db_stg, if_exists='append', index=False)


    except:
         traceback.print_exc()
    finally:
        pass