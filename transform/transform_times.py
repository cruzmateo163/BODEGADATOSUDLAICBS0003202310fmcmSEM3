from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime
from transform.transformations import obt_date

def tra_times(etlpro_id):
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
      
         #DICTIONARY FOR VALUES OF TIMES_TRA
        times_dict = {
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
            "calendar_year":[],
            "etlpro_id":[]
        }
        #Reading the ext table 
        times_ext=pd.read_sql("SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_MONTH_NAME, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times_ext", ses_db_stg)
        
        #Processing the rows
        if not times_ext.empty:
            for timeid, dayname, day_numweek, day_nummonth,calendarweek_num, calendarmonth_num,calendarmonth_desc, endcal_month, calendarquarter_desc, calendaryear \
               in zip(times_ext['TIME_ID'], times_ext['DAY_NAME'], times_ext['DAY_NUMBER_IN_WEEK'], times_ext['DAY_NUMBER_IN_MONTH'], times_ext['CALENDAR_WEEK_NUMBER'],
                times_ext['CALENDAR_MONTH_NUMBER'],times_ext['CALENDAR_MONTH_DESC'],times_ext['END_OF_CAL_MONTH'], times_ext['CALENDAR_QUARTER_DESC'],
                times_ext['CALENDAR_YEAR']):
                times_dict['time_id'].append(obt_date(timeid))
                times_dict["day_name"].append(dayname)
                times_dict["day_number_in_week"].append(day_numweek)
                times_dict["day_number_in_month"].append(day_nummonth)
                times_dict["calendar_week_number"].append(calendarweek_num)
                times_dict["calendar_month_number"].append(calendarmonth_num)
                times_dict["calendar_month_desc"].append(calendarmonth_desc)
                times_dict["end_of_cal_month"].append(obt_date(endcal_month))
                times_dict["calendar_month_name"].append("")
                times_dict["calendar_quarter_desc"].append( calendarquarter_desc)
                times_dict["calendar_year"].append(calendaryear)
                times_dict["etlpro_id"].append(etlpro_id)
        if times_dict["time_id"]:
            df_times_tra=pd.DataFrame(times_dict)
            df_times_tra.to_sql('times_tra',  ses_db_stg, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass