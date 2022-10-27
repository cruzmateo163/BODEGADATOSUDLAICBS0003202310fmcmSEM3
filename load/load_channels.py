from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def load_channels(etlpro_id):
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
        dim_channel_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[],
            "etlpro_id":[]
        }
        #Reading the ext table 
        channel_dim=pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra", ses_db_stg)
        
        #Processing the rows
        if not channel_dim.empty:
            for id, des, cla, cla_id \
                in zip(channel_dim['CHANNEL_ID'], channel_dim['CHANNEL_DESC'],
                channel_dim['CHANNEL_CLASS'], channel_dim['CHANNEL_CLASS_ID']):
                dim_channel_dict['channel_id'].append(id)
                dim_channel_dict["channel_desc"].append(des)
                dim_channel_dict["channel_class"].append(cla)
                dim_channel_dict["channel_class_id"].append(cla_id)
                dim_channel_dict["etlpro_id"].append(etlpro_id)
        if dim_channel_dict["channel_id"]:
            df_channels_dim=pd.DataFrame(dim_channel_dict)
            df_channels_dim.to_sql('dim_channels', ses_db_sor, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass





