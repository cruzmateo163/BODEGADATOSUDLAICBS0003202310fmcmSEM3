
from util.db_connection import Db_Connection

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