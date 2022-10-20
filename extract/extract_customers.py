from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_customers():
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
      
         #DICTIONARY FOR VALUES OF products
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
            "cust_email":[]
        }
        #Reading the CVS file
        customers_csv=pd.read_csv("csvs/customers.csv")
        print(customers_csv)
        #Processing the CVS file content
        if not customers_csv.empty:
            for custid, custfirst_name, custlast_name, custgender, custyear_birth, custmarital, cust_address, cust_postalcode, custcity, cust_province, countryid, cust_phone, cust_income, cust_credit, custemail \
                in zip(customers_csv['CUST_ID'], customers_csv['CUST_FIRST_NAME'], customers_csv['CUST_LAST_NAME'], customers_csv['CUST_GENDER'],
                customers_csv['CUST_YEAR_OF_BIRTH'], customers_csv['CUST_MARITAL_STATUS'],customers_csv['CUST_STREET_ADDRESS'],customers_csv['CUST_POSTAL_CODE'],
                customers_csv['CUST_CITY'],customers_csv['CUST_STATE_PROVINCE'],customers_csv['COUNTRY_ID'],customers_csv['CUST_MAIN_PHONE_NUMBER'],customers_csv['CUST_INCOME_LEVEL'],
                customers_csv['CUST_CREDIT_LIMIT'],customers_csv['CUST_EMAIL']):
                customers_dict['cust_id'].append(custid)
                customers_dict["cust_first_name"].append( custfirst_name)
                customers_dict["cust_last_name"].append(custlast_name)
                customers_dict["cust_gender"].append( custgender)
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
        if customers_dict["cust_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE customers")
            df_customers_ext=pd.DataFrame(customers_dict)
            df_customers_ext.to_sql('customers', ses_db_stg, if_exists='append', index=False)


    except:
         traceback.print_exc()
    finally:
        pass