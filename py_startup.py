from load.load_channels import load_channels
from transform.transform_channels import  tra_channels
from util.db_connection import Db_Connection
from extract.extract_channels import ext_channels
from extract.extract_sales import ext_sales
from extract.extract_promotions import ext_promotions
from extract.extract_countries import ext_countries
from extract.extract_products import ext_products
from extract.extract_times import ext_times
from extract.extract_customers import ext_customers
from util.ETL_PROinse import ETLinsert
import pandas as pd
import traceback
from util.extract import extract_pro
from util.load import load_pro
from util.transform import transf_pro


try:
    
    etl_process = ETLinsert()
     
    extract_pro()

    transf_pro(etl_process)

    load_pro(etl_process)

    
    



except:
    traceback.print_exc()
finally:
    pass