from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def tra_channels(etlpro_id):
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
      
         #DICTIONARY FOR VALUES OF CHANNELS_TRA
        channel_tra_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[],
            "etlpro_id":[]
        }
        #Reading the ext table 
        channel_ext=pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_ext", ses_db_stg)
        
        #Processing the rows
        if not channel_ext.empty:
            for id, des, cla, cla_id \
                in zip(channel_ext['CHANNEL_ID'], channel_ext['CHANNEL_DESC'],
                channel_ext['CHANNEL_CLASS'], channel_ext['CHANNEL_CLASS_ID']):
                channel_tra_dict['channel_id'].append(id)
                channel_tra_dict["channel_desc"].append(des)
                channel_tra_dict["channel_class"].append(cla)
                channel_tra_dict["channel_class_id"].append(cla_id)
                channel_tra_dict["etlpro_id"].append(etlpro_id)
        if channel_tra_dict["channel_id"]:
            df_channels_tra=pd.DataFrame(channel_tra_dict)
            df_channels_tra.to_sql('channels_tra',  ses_db_stg, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass





