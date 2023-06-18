# -*- coding: utf-8 -*-
"""
CODE for ItaloAPI() class with 'call' method able to call API for:
    
    - tracking a single Italo train starting from its code
        e.g. data = ItaloAPI().call('RicercaTrenoService',XXXX)
            where XXXX is the number of the train to be tracked
    - looking at arrival and departure tables for a station
        e.g. data = ItaloAPI().call('RicercaStazioneService','Milano Centrale','MC_')
            for exact name of the station and code is necessary to inspect the page:
                https://italoinviaggio.italotreno.it/en/station/milano-centrale

@author: Lorenzo Villa (villalorenzo97@outlook.it)
"""

import json
import urllib.parse as urlp

try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen  
    

# Decoders for API Output - TODO: Proper error handling
def _decode_json (s):
    if s == '':
        return None
    return json.loads(s)

def _decode_lines (s, linefunc):
    if s == '':
        return []
    
    lines = s.strip().split('\n')
    result = []
    for line in lines:
        result.append(linefunc(line))
            
    return result
       

class ItaloAPI:
    def __init__ (self, **options):
        self.base = 'https://italoinviaggio.italotreno.it/api/'
        self.__verbose = options.get('verbose', False)
        self.__urlopen = options.get('urlopen', urlopen)
        self.__plainoutput = options.get('plainoutput', False)
        self.__decoders = {
            'RicercaTrenoService':     _decode_json,
            'RicercaStazioneService':      _decode_json,
        }
        self.__default_decoder = lambda x: x

    def __checkAndDecode(self, function, data):
        decoder = self.__decoders.get(function, self.__default_decoder)
        return decoder(data)
    
    def RicercaStazione_query(self,station='Milano Centrale',station_code='MC_'):
        query='&CodiceStazione='+station_code+'&NomeStazione='+urlp.quote_plus(station)
        return query
    
    def RicercaTreno_query(self,train_number):
        query='&TrainNumber='+str(train_number)
        return query
        
    def call (self, function,*params, **options):
        plain = options.get('plainoutput', self.__plainoutput)
        verbose = options.get('verbose', self.__verbose)
        
        if function=='RicercaStazioneService':
            try:
                station = params[0]
                station_code = params[1]
            except: print('ERROR! Insert station name and code');return
                
            query = self.RicercaStazione_query(station,station_code)
        
        elif function == 'RicercaTrenoService':
            try:
                train_number = params[0]
            except: print('ERROR! Insert train number');return
            
            query = self.RicercaTreno_query(train_number)
        else:
            print('ERROR! Function: '+function+' not available')
            return
        
        url = self.base + function + '?' + query
        
        if verbose:
            print (url)

        req = self.__urlopen(url)
        data = req.read().decode('utf-8')
        
        if plain:
            return data
        else:
            return self.__checkAndDecode (function, data)
        
                