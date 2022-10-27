from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def load_customers(etlpro_id):
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

        #Variables
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd =  'admin'
        db = 'fmcmdbsor'

        con_db_sor = Db_Connection(type, host, port, user, pwd, db)
        ses_db_sor = con_db_sor.start()
        if ses_db_sor == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_sor == -2:
            raise Exception("Error trying to connect to the b2b_dwh_sor database")



            
      
         #DICTIONARY FOR VALUES OF DIM_CHANNELS
        dim_customers_dict = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_number":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[]
            #"etlpro_id":[]
        }
        #Reading the ext table 
        customers_dim=pd.read_sql("SELECT CUST_ID,CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS, CUST_STREET_ADDRESS, CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE,COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT, CUST_EMAIL FROM customers_ext" , ses_db_stg)
        
        #Processing the rows
        if not customers_dim.empty:
            for custid, custfirst_name, custlast_name, custgender, custyear_birth, custmarital, cust_address, cust_postalcode, custcity, cust_province, countryid, cust_phone, cust_income, cust_credit, custemail \
                in zip(customers_dim['CUST_ID'], customers_dim['CUST_FIRST_NAME'], customers_dim['CUST_LAST_NAME'], customers_dim['CUST_GENDER'],
                customers_dim['CUST_YEAR_OF_BIRTH'], customers_dim['CUST_MARITAL_STATUS'],customers_dim['CUST_STREET_ADDRESS'],customers_dim['CUST_POSTAL_CODE'],
                customers_dim['CUST_CITY'],customers_dim['CUST_STATE_PROVINCE'],customers_dim['COUNTRY_ID'],customers_dim['CUST_MAIN_PHONE_NUMBER'],customers_dim['CUST_INCOME_LEVEL'],
                customers_dim['CUST_CREDIT_LIMIT'],customers_dim['CUST_EMAIL']):
                dim_customers_dict['cust_id'].append(custid)
                dim_customers_dict["cust_first_name"].append(custfirst_name)
                dim_customers_dict["cust_last_name"].append(custlast_name)
                dim_customers_dict["cust_gender"].append(custgender)
                dim_customers_dict["cust_year_of_birth"].append(custyear_birth)
                dim_customers_dict["cust_marital_status"].append( custmarital)
                dim_customers_dict["cust_street_address"].append(cust_address)
                dim_customers_dict["cust_postal_code"].append(cust_postalcode)
                dim_customers_dict["cust_city"].append(custcity)
                dim_customers_dict["cust_state_province"].append(cust_province)
                dim_customers_dict["country_id"].append(countryid)
                dim_customers_dict["cust_main_phone_number"].append(cust_phone)
                dim_customers_dict["cust_income_level"].append(cust_income)
                dim_customers_dict["cust_credit_limit"].append(cust_credit)
                dim_customers_dict["cust_email"].append(custemail)
                #dim_channel_dict["etlpro_id"].append(etlpro_id)
        if dim_customers_dict["cust_id"]:
            df_customers_dim=pd.DataFrame(dim_customers_dict)
            df_customers_dim.to_sql('dim_customers', ses_db_sor, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass
