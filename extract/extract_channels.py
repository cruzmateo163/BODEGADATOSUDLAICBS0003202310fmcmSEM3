from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_channels():
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
      
         #DICTIONARY FOR VALUES OF CHANNELS
        channel_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        #Reading the CVS file
        channel_csv=pd.read_csv("csvs/channels.csv")
        print(channel_csv)
        #Processing the CVS file content
        if not channel_csv.empty:
            for id, des, cla, cla_id \
                in zip(channel_csv['CHANNEL_ID'], channel_csv['CHANNEL_DESC'],
                channel_csv['CHANNEL_CLASS'], channel_csv['CHANNEL_CLASS_ID']):
                channel_dict['channel_id'].append(id)
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(cla_id)
        if channel_dict["channel_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE channels")
            df_channels_ext=pd.DataFrame(channel_dict)
            df_channels_ext.to_sql('channels', ses_db_stg, if_exists='append', index=False)


    except:
         traceback.print_exc()
    finally:
        pass

