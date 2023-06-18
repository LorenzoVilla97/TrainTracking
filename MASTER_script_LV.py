# -*- coding: utf-8 -*-
"""
MASTER SCRIPT
@author: villa
"""

import pandas as pd
import time
import pytz
import datetime as DT
from tqdm import tqdm

from TrainTracking_master_LV import set_orario,set_DB
from TrainTracking_master_LV import RUN_train_list, RUN_train_schedule, UPDATE_DB
from ItaloTrainTracking_master_LV import trainlist_update, update_DB

from API_core import viaggiatreno
from API_core import italotreno

#Initializing
api = viaggiatreno.API()
api_italo = italotreno.ItaloAPI()
CodStazione = "S01700"
station = 'Milano Centrale'
station_code = 'MC_'
""" PARTENZE DA MILANO CENTRALE """
tab_type_dep="partenze"
DB_filename_dep = "DB_Milano_Centrale_Partenze.pkl" 
DB_filename_dep_italo = "DB_ITALO_Milano_Centrale_Partenze.pkl" 
""" ARRIVI A MILANO CENTRALE """
tab_type_arr="arrivi"
DB_filename_arr = "DB_Milano_Centrale_Arrivi.pkl" 
DB_filename_arr_italo = "DB_ITALO_Milano_Centrale_Arrivi.pkl"



    
orario=set_orario()
DB_Trains_dep = set_DB(DB_filename_dep)
DB_Trains_arr = set_DB(DB_filename_arr)
DB_Trains_dep_italo = set_DB(DB_filename_dep_italo)
DB_Trains_arr_italo = set_DB(DB_filename_arr_italo)

''' TRENITALIA '''
Success=False;count_dep=0;count_arr=0
while not Success:
    try:        
        #NOTE: fixing 'dep' or 'arr' force the departure and the arrival delays
        #to be computedon MI Centrale and not on real Origin/Destination of the train
        #PARTENZE
        train_list_dep = RUN_train_list(CodStazione,orario,tab_type_dep)
        train_schedule_dep = RUN_train_schedule(train_list_dep,CodStazione,'dep')
        DB_MI_Centrale_Partenze = UPDATE_DB(train_schedule_dep, DB_Trains_dep, DB_filename_dep,CodStazione,'dep')
        count_dep=0
    except:
        print("Error in Depratures... at: ",DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%H:%M")); count_dep=count_dep+1
        
    try:
        #ARRIVI
        train_list_arr = RUN_train_list(CodStazione,orario,tab_type_arr)
        train_schedule_arr = RUN_train_schedule(train_list_arr,CodStazione,'arr')
        DB_MI_Centrale_Arrivi = UPDATE_DB(train_schedule_arr, DB_Trains_arr, DB_filename_arr,CodStazione,'arr')
        count_arr=0
    except:    
        print("Error in Arrivals... at: ",DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%H:%M")); count_arr=count_arr+1
        
    if count_dep+count_arr == 0:
        Success=True;
        print("TRENITALIA Last update at: ",DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%H:%M"))
        print(len(DB_MI_Centrale_Partenze)," trains departing from MI Centrale have been recorded")
        print(len(DB_MI_Centrale_Arrivi)," trains arriving to MI Centrale have been recorded")
    else:
        print("Error! Repeating.."); time.sleep(2);
        if count_dep+count_arr<20:
            pass
        else:
            print("Terminating Error!")
            Success = False
            break
        
''' ITALO '''
Success_italo=False; count=0
while not Success_italo:
    try:
        #Station table update
        italo_tab = api_italo.call('RicercaStazioneService',station,station_code)
        italo_tab_arr = italo_tab["ListaTreniArrivo"]
        italo_tab_dep = italo_tab["ListaTreniPartenza"]
    
        #NOTE: fixing 'dep' or 'arr' force the departure and the arrival delays
        #to be computedon MI Centrale and not on real Origin/Destination of the train

        italo_dep = trainlist_update(italo_tab_dep,station_code,'dep')
        italo_arr = trainlist_update(italo_tab_arr,station_code,'arr')
    
        Success_italo=True; count=0
        print("ITALO Last update at: ",DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%H:%M"))
        
        DB_MI_Centrale_Partenze_Italo = update_DB(italo_dep, DB_Trains_dep_italo, DB_filename_dep_italo,station_code, 'dep')
        DB_MI_Centrale_Arrivi_Italo = update_DB(italo_arr, DB_Trains_arr_italo, DB_filename_arr_italo, station_code, 'arr')
        print(len(DB_MI_Centrale_Partenze_Italo)," ITALOtrains departing from MI Centrale have been recorded")
        print(len(DB_MI_Centrale_Arrivi_Italo)," ITALOtrains arriving to MI Centrale have been recorded")
        
    except:
        print("Error! Repeating.."); count=count+1
        if count<10:
            pass
        else:
            print("Terminating Error!")
            Success_italo = False
            break
'''    
if Success and Success_italo:
    for i in tqdm(range(5*60),desc="Waiting: "):
        time.sleep(1)
else:
    for i in tqdm(range(60),desc="Trying once more after: "):
        time.sleep(1)
         
'''
