# -*- coding: utf-8 -*-
"""
TRACKING ITALO TRAINS
@author: villa
"""

import sys
import datetime as DT
import time
import json
import pickle
import pandas as pd
from API_core import italotreno
from tqdm import tqdm
import pytz

api_italo = italotreno.ItaloAPI()

def is_valid_timestamp(ts):
    return (ts is not None) and (ts > 0) and (ts < 2147483648000)
    
def format_timestamp_orario(ts, fmt='%H:%M'):
    if is_valid_timestamp(ts):
        return DT.datetime.fromtimestamp(ts/1000,tz=pytz.timezone('Europe/Rome')).strftime(fmt)
    else:
        return ts

def format_timestamp_data(ts, fmt='%Y-%m-%d'):
    if is_valid_timestamp(ts):
        return DT.datetime.fromtimestamp(ts/1000,tz=pytz.timezone('Europe/Rome')).strftime(fmt)
    else:
        return ts 

def set_DB(DB_filename):
    try:    
        with open(DB_filename, 'rb') as file:          
                DB = pickle.load(file)
        print(len(DB),' trains are present in ',DB_filename,' database')
    except:
        print("Warning NO DATABASE to update")
        DB = pd.DataFrame()
        with open(DB_filename, 'wb') as file:         
            pickle.dump(DB, file)
    
    return DB

def organize_train_data(t,info_t,departure_station ,arrival_station,setStation='dep'):
    
    sched = info_t["TrainSchedule"]
    stazioni = [sched['StazionePartenza']] + sched["StazioniFerme"] + sched["StazioniNonFerme"]
    for s in stazioni:
        if s["LocationCode"] == arrival_station:
            arrivo_teorico = s['EstimatedArrivalTime']
            arrivo_reale = s['ActualArrivalTime']
            if (DT.datetime.strptime(arrivo_reale,'%H:%M') - DT.datetime.strptime(arrivo_teorico,'%H:%M')).days<0:
                ritardo_arrivo = 1440 - round((DT.datetime.strptime(arrivo_reale,'%H:%M') - DT.datetime.strptime(arrivo_teorico,'%H:%M')).seconds/60)
            else:
                ritardo_arrivo = round((DT.datetime.strptime(arrivo_reale,'%H:%M') - DT.datetime.strptime(arrivo_teorico,'%H:%M')).seconds/60)
            
        if s["LocationCode"] == departure_station:
            partenza_teorica = s['EstimatedDepartureTime']
            partenza_reale = s['ActualDepartureTime']
            if (DT.datetime.strptime(partenza_reale,'%H:%M') - DT.datetime.strptime(partenza_teorica,'%H:%M')).days<0:
                ritardo_partenza = 1440 - round((DT.datetime.strptime(partenza_reale,'%H:%M') - DT.datetime.strptime(partenza_teorica,'%H:%M')).seconds/60)
            else:
                ritardo_partenza = round((DT.datetime.strptime(partenza_reale,'%H:%M') - DT.datetime.strptime(partenza_teorica,'%H:%M')).seconds/60)    
    
    data = {"categoria": 'Italo',
            "numeroTreno": t["numeroTreno"],
            "circolante": bool(sched["Distruption"]["RunningState"]) ,
            "codOrigine": sched["DepartureStation"] ,
            "origine": sched["DepartureStationDescription"] ,
            "destinazione": sched["ArrivalStationDescription"],
            "ritardo": sched["Distruption"]["DelayAmount"],
            "nonPartito": sched["Distruption"]["Warning"],
            "arrivo a:":arrival_station,
            "arrivo_teorico": arrivo_teorico,
            "arrivoReale": arrivo_reale,
            "ritardoArrivo": ritardo_arrivo,
            "partenza da:": departure_station,
            "partenza_teorica": partenza_teorica,
            "partenzaReale": partenza_reale,
            "ritardoPartenza": ritardo_partenza,
            "dataPartenzaTreno": format_timestamp_data(round(DT.datetime.now(pytz.timezone('Europe/Rome')).timestamp())*1000),
            "descrizione": t['descrizione']
            }
    if setStation=='dep': 
        date_idx = data['partenza_teorica'] 
    else: 
        date_idx = data['arrivo_teorico']
        
    idx = (data["numeroTreno"], date_idx ,data["dataPartenzaTreno"])
    new=pd.DataFrame([data], index=[idx])
    
    return new


def trainlist_update(train_list,station_code, setStation):
    updated_list = pd.DataFrame()
    
    for t in train_list:
        t["numeroTreno"]=t["Numero"]
        t["descrizione"]=t["Descrizione"]
        t_update,check = singleTrainUpdate(t,station_code,setStation)
        if not check: continue #Avoid error due to train not found
        
        updated_list = pd.concat([updated_list,t_update])
      
    return updated_list

def singleTrainUpdate(train,station_code,setStation):
    info_t = api_italo.call('RicercaTrenoService',train["numeroTreno"])
    
    if info_t["IsEmpty"]:
        #print("WARNING! Train "+ train["numeroTreno"]+ " Not Found!")
        return None,False
    
    #Default inputs
    arrival_station = info_t["TrainSchedule"]["ArrivalStation"]
    departure_station = info_t["TrainSchedule"]["StazionePartenza"]["LocationCode"]
    #Forcing departure or arrival station
    if setStation == 'dep': departure_station = station_code
    if setStation == 'arr': arrival_station = station_code
        
    t_update = organize_train_data(train,info_t,departure_station,arrival_station,setStation)
    
    return t_update,True
    
 
def update_DB(schedule,DB,DB_file, station_code, setStation):    
    
    DB = DB.drop(schedule.index, errors='ignore')
    DB = pd.concat([DB,schedule])
    
    DB = DB_train_schedule_update(DB, station_code, setStation)
            
    with open(DB_file, 'wb') as file:         
        pickle.dump(DB, file)
    
    return DB   

def DB_train_schedule_update(DB, station_code, setStation, today=round(DT.datetime.now(pytz.timezone('Europe/Rome')).timestamp())*1000):
    for i,row in DB.iterrows():
        if i[2]==format_timestamp_data(today):
            new_t,check = singleTrainUpdate(row, station_code, setStation)
            if not check:
                #print('ERROR! Train: ',i,' not found')
                continue #Avoid error due to train not found
            else:
                DB = DB.drop([i])#, errors='ignore')
                DB = pd.concat([DB,new_t])
        else:
            pass
                
    return DB

'''
#Initializing
api = italotreno.ItaloAPI()
station = 'Milano Centrale'
station_code = 'MC_'

DB_filename_dep = "DB_ITALO_Milano_Centrale_Partenze.pkl" 
DB_filename_arr = "DB_ITALO_Milano_Centrale_Arrivi.pkl" 

try:
    Success=False; count=0
    while True:
        DB_Trains_dep = set_DB(DB_filename_dep)
        DB_Trains_arr = set_DB(DB_filename_arr)
        while not Success:
            try:
                #Station table update
                italo_tab = api.call('RicercaStazioneService',station,station_code)
                italo_tab_arr = italo_tab["ListaTreniArrivo"]
                italo_tab_dep = italo_tab["ListaTreniPartenza"]
            
                #NOTE: fixing 'dep' or 'arr' force the departure and the arrival delays
                #to be computedon MI Centrale and not on real Origin/Destination of the train
        
                italo_dep = trainlist_update(italo_tab_dep,station_code,'dep')
                italo_arr = trainlist_update(italo_tab_arr,station_code,'arr')
            
                Success=True; count=0
                print("Last update at: ",DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%H:%M"))
                
                DB_MI_Centrale_Partenze_Italo = update_DB(italo_dep, DB_Trains_dep, DB_filename_dep,station_code, 'dep')
                DB_MI_Centrale_Arrivi_Italo = update_DB(italo_arr, DB_Trains_arr, DB_filename_arr, station_code, 'arr')
                
            except:
                print("Error! Repeating.."); count=count+1
                if count<10:
                    pass
                else:
                    print("Terminating Error!")
                    Success = False
                    break
              
        if Success:
            for i in tqdm(range(5*60),desc="Waiting: "):
                time.sleep(1)
        else:
            for i in tqdm(range(60),desc="Trying once more after: "):
                time.sleep(1)
        
except KeyboardInterrupt:
    pass 

'''  