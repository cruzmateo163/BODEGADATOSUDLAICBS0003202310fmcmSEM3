from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_times():
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
      
         #DICTIONARY FOR VALUES OF times
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
            "calendar_year":[]
        }
        #Reading the CVS file
        times_csv=pd.read_csv("csvs/times.csv")
        print(times_csv)
        #Processing the CVS file content
        if not times_csv.empty:
            for timeid, dayname, day_numweek, day_nummonth,calendarweek_num, calendarmonth_num,calendarmonth_desc, endcal_month, calendarquarter_desc, calendaryear \
                in zip(times_csv['TIME_ID'], times_csv['DAY_NAME'], times_csv['DAY_NUMBER_IN_WEEK'], times_csv['DAY_NUMBER_IN_MONTH'], times_csv['CALENDAR_WEEK_NUMBER'],
                times_csv['CALENDAR_MONTH_NUMBER'],times_csv['CALENDAR_MONTH_DESC'],times_csv['END_OF_CAL_MONTH'], times_csv['CALENDAR_QUARTER_DESC'],
                times_csv['CALENDAR_YEAR']):
                times_dict['time_id'].append(timeid)
                times_dict["day_name"].append(dayname)
                times_dict["day_number_in_week"].append(day_numweek)
                times_dict["day_number_in_month"].append(day_nummonth)
                times_dict["calendar_week_number"].append(calendarweek_num)
                times_dict["calendar_month_number"].append(calendarmonth_num)
                times_dict["calendar_month_desc"].append(calendarmonth_desc)
                times_dict["end_of_cal_month"].append(endcal_month)
                times_dict["calendar_month_name"].append("")
                times_dict["calendar_quarter_desc"].append( calendarquarter_desc)
                times_dict["calendar_year"].append(calendaryear)
        if times_dict["time_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE times")
            df_times_ext=pd.DataFrame(times_dict)
            df_times_ext.to_sql('times', ses_db_stg, if_exists='append', index=False)


    except:
         traceback.print_exc()
    finally:
        pass