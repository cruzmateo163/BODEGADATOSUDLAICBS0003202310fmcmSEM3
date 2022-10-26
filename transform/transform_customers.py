from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def tra_customers(etlpro_id):
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
      
         #DICTIONARY FOR VALUES OF Customers_TRA
        customers_dict = {
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
            "cust_email":[],
            "etlpro_id":[]
        }
        #Reading the ext table 
        customers_ext=pd.read_sql("SELECT CUST_ID,CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS, CUST_STREET_ADDRESS, CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE,COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT, CUST_EMAIL FROM customers_ext", ses_db_stg)
        
        #Processing the rows
        if not customers_ext.empty:
            for custid, custfirst_name, custlast_name, custgender, custyear_birth, custmarital, cust_address, cust_postalcode, custcity, cust_province, countryid, cust_phone, cust_income, cust_credit, custemail \
                in zip(customers_ext['CUST_ID'], customers_ext['CUST_FIRST_NAME'], customers_ext['CUST_LAST_NAME'], customers_ext['CUST_GENDER'],
                customers_ext['CUST_YEAR_OF_BIRTH'], customers_ext['CUST_MARITAL_STATUS'],customers_ext['CUST_STREET_ADDRESS'],customers_ext['CUST_POSTAL_CODE'],
                customers_ext['CUST_CITY'],customers_ext['CUST_STATE_PROVINCE'],customers_ext['COUNTRY_ID'],customers_ext['CUST_MAIN_PHONE_NUMBER'],customers_ext['CUST_INCOME_LEVEL'],
                customers_ext['CUST_CREDIT_LIMIT'],customers_ext['CUST_EMAIL']):
                customers_dict['cust_id'].append(custid)
                customers_dict["cust_first_name"].append(custfirst_name)
                customers_dict["cust_last_name"].append(custlast_name)
                customers_dict["cust_gender"].append(custgender)
                customers_dict["cust_year_of_birth"].append(custyear_birth)
                customers_dict["cust_marital_status"].append( custmarital)
                customers_dict["cust_street_address"].append(cust_address)
                customers_dict["cust_postal_code"].append(cust_postalcode)
                customers_dict["cust_city"].append(custcity)
                customers_dict["cust_state_province"].append(cust_province)
                customers_dict["country_id"].append(countryid)
                customers_dict["cust_main_phone_number"].append(cust_phone)
                customers_dict["cust_income_level"].append(cust_income)
                customers_dict["cust_credit_limit"].append(cust_credit)
                customers_dict["cust_email"].append(custemail)
                customers_dict["etlpro_id"].append(etlpro_id)
        if customers_dict["cust_id"]:
            df_customers_tra=pd.DataFrame(customers_dict)
            df_customers_tra.to_sql('customers_tra',  ses_db_stg, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass