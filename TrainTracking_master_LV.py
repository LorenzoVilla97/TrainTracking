# -*- coding: utf-8 -*-
"""
TRACKING TRAINS ARRIVING AND DEPARTING FROM A SINGLE STATION
@author: villa
"""

import sys
import datetime as DT
import time
import json
import pickle
import pandas as pd
from API_core import viaggiatreno
from tqdm import tqdm
import pytz

api = viaggiatreno.API()

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

def set_orario(month=DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%b"), day=DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%d"), year=DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%Y"), ora=DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%H:%M")):
    #If no specific parameters are set they are computed NOW
    orario = month+" "+str(day)+" "+year+" "+ora
    orario=orario.replace(" ","%20")
    return orario

with open('DB_Stazioni.pkl', 'rb') as file:         
    DB_Stazioni = pickle.load(file)
    
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

def clean_trains_data(old):
    keys = ["numeroTreno",
            "circolante",
            "codOrigine",
            "origine",
            "destinazione",
            "ritardo",
            "nonPartito",
            "arrivo_teorico",
            "arrivoReale",
            "ritardoArrivo",
            "partenza_teorica",
            "partenzaReale",
            "ritardoPartenza",
            "dataPartenzaTreno",
            "categoria",
            "orarioArrivo",
            "orarioPartenza"]
    
    new = {key: old[key] for key in keys}
    new=pd.DataFrame([new])
    return new

def clean_trains_df(old):
    new = old.loc[:,["numeroTreno",
            "circolante",
            "codOrigine",
            "origine",
            "destinazione",
            "ritardo",
            "nonPartito",
            "dataPartenzaTreno",
            "categoria",
            "orarioArrivo",
            "orarioPartenza"]]
    return new
    
def train_schedule_API(CodStazione,orario=set_orario(),tab_type="partenze"):
    loop=0
    while loop<10:
        try:
            data = api.call(tab_type, CodStazione,orario, verbose=False)
            break
        except:
            time.sleep(5)
            loop = loop+1
            
    try:
        data = data.strip("[]")
        data = data.split("},")
    except:
        print('API ERROR!') 
    
    return data

def train_list_processing(data):
    
    trains = pd.DataFrame()
    for t in data:
        t=t.strip("{}")
        t="{"+t+"}"
        t=t.replace("null", "0")
        t=json.loads(t)
        t["orarioArrivo"]=format_timestamp_orario(t["orarioArrivo"])
        t["orarioPartenza"]=format_timestamp_orario(t["orarioPartenza"])
        if t["origine"] == 0:
            t["origine"] = DB_Stazioni[t["codOrigine"]]
        
        t = pd.DataFrame([t]) 
        trains = pd.concat([trains,t])
    
    trains = clean_trains_df(trains)
    return trains
    
    
def RUN_train_list(CodStazione,orario,tab_type):
    train_list = train_schedule_API(CodStazione,orario,tab_type)
    train_list = train_list_processing(train_list)
    train_list.index = train_list["numeroTreno"]
        
    return train_list



def train_data_API(t,CodStazione,setStation):
    t = t.copy() 
    loop=0
    while loop<10:
        try:
            t_status = api.call('andamentoTreno', t["codOrigine"],t["numeroTreno"] , t["dataPartenzaTreno"])
            break
        except:
            time.sleep(5)
            loop = loop+1
            
    try:
        #Default input data setting arrival/departure as destination/origin of the train
        CodStazione_arrivo = t_status["fermate"][-1]["id"]
        CodStazione_partenza = t_status["fermate"][0]["id"] 
        if setStation == 'dep': CodStazione_partenza=CodStazione
        if setStation == 'arr': CodStazione_arrivo=CodStazione
        for f in t_status["fermate"]:
            if f["id"] == CodStazione_arrivo:
                t["arrivo_teorico"] = format_timestamp_orario(f["arrivo_teorico"])
                t["arrivoReale"] = format_timestamp_orario(f["arrivoReale"])
                t["ritardoArrivo"] = f["ritardoArrivo"]
            if f["id"] == CodStazione_partenza:    
                t["partenza_teorica"] = format_timestamp_orario(f["partenza_teorica"])
                t["partenzaReale"] = format_timestamp_orario(f["partenzaReale"])
                t["ritardoPartenza"] = f["ritardoPartenza"]        
    except:
        #print('API ERROR! Train ',str(t["numeroTreno"]),' not updated')
        return t
        
    return t


def RUN_train_schedule(train_list,CodStazione,setStazione):
    updated_list = pd.DataFrame()
    for idx,t in train_list.iterrows():
        #Setting default fields
        t["arrivoReale"] = "N/A"
        t["ritardoArrivo"] = 0
        t["partenzaReale"] = "N/A"
        t["ritardoPartenza"] = 0
        t["partenza_teorica"] = '00:00'
        t["arrivo_teorico"] = '00:00'
        
        t = train_data_API(t,CodStazione,setStazione)
        t.name = t["numeroTreno"]
        updated_list = pd.concat([updated_list,t.to_frame().T])
    
    train_list = train_list.drop(updated_list.index, errors='ignore')
    train_list = pd.concat([train_list,updated_list])    
        
    return train_list


def UPDATE_DB(schedule,DB,DB_file,CodStazione,setStazione):    
    new_index=list()
    for idx,s in schedule.iterrows():
        
        if setStazione == 'dep':
            date_idx = s["orarioPartenza"]
        else:
            date_idx = s["orarioArrivo"]
        
        new = (s["numeroTreno"], date_idx ,format_timestamp_data(s["dataPartenzaTreno"])) 
        new_index.append(new)
    schedule.index = new_index
    
    DB = DB.drop(schedule.index, errors='ignore')
    DB = pd.concat([DB,schedule])
    
    DB = DB_train_schedule_update(DB,CodStazione,setStazione)
    
    new_col_order = ["categoria",
                     "numeroTreno",
                     "nonPartito",
                     "circolante",
                     "origine",
                     "destinazione",
                     "ritardo",
                     "arrivo_teorico",
                     "arrivoReale",
                     "ritardoArrivo",
                     "partenza_teorica",
                     "partenzaReale",
                     "ritardoPartenza",
                     "dataPartenzaTreno",
                     "orarioArrivo",
                     "orarioPartenza",
                     "codOrigine"]
    
    DB = DB.reindex(new_col_order, axis='columns')
            
    with open(DB_file, 'wb') as file:         
        pickle.dump(DB, file)
    
    return DB

def DB_train_schedule_update(DB,CodStazione,setStazione):
    today=round(DT.datetime.now(pytz.timezone('Europe/Rome')).timestamp())*1000
    for i,row in DB.iterrows():
        try:    
            if i[2]==format_timestamp_data(today) or (DT.datetime.strptime(row['partenza_teorica'],'%H:%M') > DT.datetime.strptime(row['arrivo_teorico'],'%H:%M')) : #exceptions for night trains
            
                new_t = train_data_API(row,CodStazione,setStazione)
                DB = DB.drop([i])#, errors='ignore')
                
                if setStazione == 'dep':
                    date_idx = row["orarioPartenza"]
                else:
                    date_idx = row["orarioArrivo"]
                
                new_t.name=(row["numeroTreno"], date_idx ,format_timestamp_data(row["dataPartenzaTreno"]))
                DB = pd.concat([DB,new_t.to_frame().T])
                #print('Train: ',i,' has been updated')
        except:
            #print('Warning! Train: ',i,' not found')
            continue
    return DB

'''
#Initializing
orario=set_orario()
CodStazione = "S01700"
"""
PARTENZE DA MILANO CENTRALE
"""
tab_type_dep="partenze"
DB_filename_dep = "DB_Milano_Centrale_Partenze.pkl" 
train_list_dep = pd.DataFrame()

"""
ARRIVI A MILANO CENTRALE
"""
tab_type_arr="arrivi"
DB_filename_arr = "DB_Milano_Centrale_Arrivi.pkl" 
train_list_arr = pd.DataFrame()
#%%
try:
    Success=False; count=0
    while True:
        orario=set_orario()
        DB_Trains_dep = set_DB(DB_filename_dep)
        DB_Trains_arr = set_DB(DB_filename_arr)
        while not Success:
            try:
                #NOTE: fixing 'dep' or 'arr' force the departure and the arrival delays
                #to be computedon MI Centrale and not on real Origin/Destination of the train
                #PARTENZE
                train_list_dep = RUN_train_list(CodStazione,orario,tab_type_dep,train_list_dep)
                train_schedule_dep = RUN_train_schedule(train_list_dep,CodStazione,'dep')
                #ARRIVI
                train_list_arr = RUN_train_list(CodStazione,orario,tab_type_arr,train_list_arr)
                train_schedule_arr = RUN_train_schedule(train_list_arr,CodStazione,'arr')
            
                Success=True; count=0
                print("Last update at: ",DT.datetime.now(pytz.timezone('Europe/Rome')).strftime("%H:%M"))
                
                DB_MI_Centrale_Partenze = UPDATE_DB(train_schedule_dep, DB_Trains_dep, DB_filename_dep,CodStazione,'dep')
                DB_MI_Centrale_Arrivi = UPDATE_DB(train_schedule_arr, DB_Trains_arr, DB_filename_arr,CodStazione,'arr')
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












