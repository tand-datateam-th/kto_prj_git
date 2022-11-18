# basic
import gc
import os
import sys
import warnings
warnings.filterwarnings(action='ignore') 
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
import pickle

#--------------------#
# handling
#--------------------#
import math
import time
import random
# import openpyxl
import importlib
import xlsxwriter
import numpy as np
import pandas as pd
from collections import Counter
from datetime import datetime, timedelta
from scipy.stats import pearsonr
import difflib

#--------------------#
# sphere package
#--------------------#
sys.path.append("/home/das_share/sphere_class/")
import SpherePackage
from SpherePackage import *
for pkg in [SpherePackage] :
    _ = importlib.reload(pkg)


############################################################
# setting variables
INPUT_PATH = '/home/das_share/analysis/data/app_log/' #+ 'app_commerce/' 
DATA_TYPE = '/device/'
APP_KEY = '/kto/' 
input_path = "/home/das_share/analysis/data/app_log/device/kto/custom_data"

KEY_ID_DEVICE = 'uid' # default
KEY_ID_USER = 'user_id'

e_date = '20221113' 
today = datetime.strptime(e_date, '%Y%m%d') + timedelta(days = 1) 


############################################################
class Pickling():
    def __init__(self):
        pass

    def kto_user_id_prep(x):
        if isinstance(x, str) :
            if x[-2:] in [',N', ',Y'] : return x[:-2]
            else : return x
        else : return x

    ## searchmain & userid prep
    def preprocess_events(_event, df_target, have_params = False):
        ###################################
        # df_target : default -> df_app_log
        ###################################

        ## 1) userid prep
        df_target[KEY_ID_USER] = df_target[KEY_ID_USER].apply(lambda x : Pickling.kto_user_id_prep(x))

        ## 2) searchmain prep
        ### create abs_events
        df_target['abs_events'] = df_target['events'].apply(lambda x : DataImport.abstract_events(x))

        ### target data setting
        df_target_prep = df_target[
                    df_target['abs_events'].apply(
                        lambda x: True if _event in x else False)]

        dict_events_save = {}    

        ### append events(should be saved) at list by option = have_params
        #### have_params: event에서 params가 비어있으면 지우는 옵션 => True이면 params가 있음을, False는 params가 없음을 의미
        if have_params == False:
            for i in list(df_target_prep.index):
                dict_events_save[i] = []
                for x in df_target_prep['events'][i]:
                    if x['name'] != _event:
                        dict_events_save[i].append(x)
                    else:
                        pass
        
        else:
            for i in list(df_target_prep.index):
                dict_events_save[i] = []
                for x in df_target_prep['events'][i]:
                    if bool(x['params']) == True:
                        dict_events_save[i].append(x)
                    else:
                        pass

        ### merge 'events' column with df_target
        df_output = df_target.reset_index()
        df_output['events'] = df_output[['index', 'events']].apply(
            lambda x : dict_events_save[x['index']] 
            if x['index'] in dict_events_save.keys() 
            else x['events'], 
            axis =1)

        ### make abs_events again
        df_output = df_output.drop(columns = 'abs_events', axis = 1)
        df_output['abs_events'] = df_output['events'].apply(lambda x : DataImport.abstract_events(x))
        df_output = df_output.set_index('index')

        return df_output


    def return_pickle_file(_input_path, lst):
        for _date in lst:
            _date_prep = datetime.strptime(_date, '%Y%m%d')

            df_app_log = DataImport.read_files(
                        input_path = INPUT_PATH + DATA_TYPE + APP_KEY, 
                        today = _date_prep + timedelta(days = 1), 
                        dates = 1,
                        platform_total = True, reduce_memory = True, verbose = False)
            
            df_app_log_prep = Pickling.preprocess_events(
                                _event = 'searchMain', df_target = df_app_log)

            df_app_log_prep.to_pickle(_input_path+'/'+f'{_date}')



class CheckDate():
    def __init__(self):
        pass
    
    def check_date_return_pickle(e_date, s_date = '20220510'):
        lst_json_date_x = []
        lst_json_date_o = []
        lst_pickle_date_o = []
        lst_pickle_date_x = []

        lst_date = []
        today = datetime.strptime(e_date, '%Y%m%d') + timedelta(days = 1) 
        dates = (datetime.strptime(e_date, '%Y%m%d') - datetime.strptime(s_date, '%Y%m%d')).days + 1 

        for i in range(1, dates+1):
            date = today - timedelta(days=i) # 설정한 today를 기준으로 과거 n일 json
            y = str(date.year)
            m = str(date.month).zfill(2)
            d = str(date.day).zfill(2)
            _date = y+m+d
            lst_date.append(_date)
        
        for p_date in lst_date:
            ## data setting
            j_date = p_date[2:]
            j_input_path = INPUT_PATH + DATA_TYPE + APP_KEY

            ## 세 플랫폼에 모두 json 파일이 존재하지 않는다면 lst_json_date_x에 해당 일자 추가
            if (os.path.isfile(j_input_path + 'ios/' + j_date)==False)&\
            (os.path.isfile(j_input_path + 'android/' + j_date)==False)&\
            (os.path.isfile(j_input_path + 'web/' + j_date)):
                lst_json_date_x.append(j_date)
            
            ## 한 플랫폼에라도 json 파일 존재할 경우 lst_json_date_o에 해당 일자 추가
            else:
                lst_json_date_o.append(j_date)

            ## pickle 파일 없는 경우 lst_pickle_date_x에 일자 추가
            if (os.path.isfile(input_path + '/' + p_date)==False):
                lst_pickle_date_x.append(p_date)

            ## pickle 파일 있는 경우 lst_pickle_date_o에 일자 추가
            else:
                lst_pickle_date_o.append(p_date)

        Pickling.return_pickle_file(input_path, lst = lst_pickle_date_x)
    

class ReadFile():
    def __init__(self):
        pass

    def read_pickle(e_date, s_date = '20220510', input_path = input_path):
        lst = []
        df_list = []
        _today = datetime.strptime(e_date, '%Y%m%d') + timedelta(days = 1) 
        dates = (datetime.strptime(e_date, '%Y%m%d') - datetime.strptime(s_date, '%Y%m%d')).days + 1 

        for i in range(1, dates+1):
            _d = _today - timedelta(days=i) # 설정한 today를 기준으로 과거 n일 json
            y = str(_d.year)
            m = str(_d.month).zfill(2)
            d = str(_d.day).zfill(2)
            _d = y+m+d
            lst.append(_d)

            df = pd.read_pickle(input_path + '/' + _d)
            df_list.append(df)
        
        df_total = pd.concat(df_list).reset_index(drop = True)

        return df_total
    