from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def load_times(etlpro_id):
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
        dim_times_dict = {
            "time_id":[],
            "day_name":[],
            "day_number_in_week":[],
            "day_number_in_month":[],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
            #"etlpro_id":[]
        }
        #Reading the ext table 
        times_dim=pd.read_sql("SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_MONTH_NAME, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times_tra " , ses_db_stg)
        
        #Processing the rows
        if not times_dim.empty:
            for timeid, dayname, day_numweek, day_nummonth,calendarweek_num, calendarmonth_num,calendarmonth_desc, endcal_month, calendarquarter_desc, calendaryear \
               in zip(times_dim['TIME_ID'], times_dim['DAY_NAME'], times_dim['DAY_NUMBER_IN_WEEK'], times_dim['DAY_NUMBER_IN_MONTH'], times_dim['CALENDAR_WEEK_NUMBER'],
                times_dim['CALENDAR_MONTH_NUMBER'],times_dim['CALENDAR_MONTH_DESC'],times_dim['END_OF_CAL_MONTH'], times_dim['CALENDAR_QUARTER_DESC'],
                times_dim['CALENDAR_YEAR']):
                dim_times_dict['time_id'].append(timeid)
                dim_times_dict["day_name"].append(dayname)
                dim_times_dict["day_number_in_week"].append(day_numweek)
                dim_times_dict["day_number_in_month"].append(day_nummonth)
                dim_times_dict["calendar_week_number"].append(calendarweek_num)
                dim_times_dict["calendar_month_number"].append(calendarmonth_num)
                dim_times_dict["calendar_month_desc"].append(calendarmonth_desc)
                dim_times_dict["end_of_cal_month"].append(endcal_month)
                dim_times_dict["calendar_month_name"].append("")
                dim_times_dict["calendar_quarter_desc"].append( calendarquarter_desc)
                dim_times_dict["calendar_year"].append(calendaryear)
                #dim_channel_dict["etlpro_id"].append(etlpro_id)
        if dim_times_dict["time_id"]:
            df_times_dim=pd.DataFrame(dim_times_dict)
            df_times_dim.to_sql('dim_times', ses_db_sor, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass