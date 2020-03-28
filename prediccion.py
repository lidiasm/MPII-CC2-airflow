#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clase que contiene la lógica de negocio del servicio implementando las distintas
opciones para predecir la temperatura y la humedad de la ciudad de San Francisco.

@author: Lidia Sánchez Mérida
"""
from statsmodels.tsa.arima_model import ARIMA
import pandas as pd
import pmdarima as pm
import pymongo
import pickle
from datetime import datetime, timedelta
import requests
import os
import zipfile

class Prediccion:
    
    def conectar_bd(self):
        """Conectamos con el sistema de almacenamiento cloud MongoDBAtlas"""
        usuario = os.environ.get('USER_ATLAS')
        clave = os.environ.get('PSW_ATLAS')
        if (type(usuario) != str or usuario == None or usuario == "" or
            type(clave) != str or clave == None or clave == ""):
            raise ConnectionError('Credenciales no válidas para acceder a MongoDBAtlas.')
            
        self.mongodb = pymongo.MongoClient(
            "mongodb+srv://"+usuario+":"+clave+"@mascotas-dxlr6.mongodb.net/test?retryWrites=true&w=majority")
        """Accedemos a la base de datos y en particular a la colección"""
        self.coleccion = self.mongodb['PrediccionesBD']['DatosTiempo']
    
    def crear_modelo_arima(self, dataframe, modelo):
        """Crea el modelo ARIMA según el argumento modelo: TEMP (temperatura)/HUM (humedad)."""
        arima_temp = pm.auto_arima(dataframe[modelo].dropna(), start_p=1,
               start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False,
               start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)
        """Guardamos el modelo como objeto en un fichero"""
        pickle.dump(arima_temp, open( "./modelos/modelo_"+modelo+".p", "wb" ) )
        """Lo comprimimos para luego subirlo a GitHub"""
        zipObj = zipfile.ZipFile('./modelos/modelo_'+modelo+'.zip', 'w', zipfile.ZIP_DEFLATED)
        zipObj.write("./modelos/modelo_"+modelo+".p")
        zipObj.close()

    def get_predicciones_arima(self, periodo):
        """Comprobamos si el período es realmente un número."""
        try:
            tiempo = int(periodo)
        except ValueError:
            raise ValueError("El periodo debe ser un número entero.")

        """Obtenemos los datos del contenedor MongoDB"""
        self.conectar_bd()
        datosbd = self.coleccion.find_one({'index':'SF'})
        """Lo pasamos a dataframe para trabajar con ARIMA"""
        dataframe = pd.DataFrame(datosbd["datos"])
        
        """Intentamos cargar el modelo ARIMA si existe. Si no lo creamos y lo guardamos
            como un objeto en un fichero."""
        if os.path.isfile('./modelos/modelo_TEMP.zip') == False:
            self.crear_modelo_arima(dataframe, 'TEMP')
        
        """Extraemos el modelo del fichero comprimido."""
        with zipfile.ZipFile('./modelos/modelo_TEMP.zip', 'r') as zipObj:
           zipObj.extractall("./")
          
        """Cargamos el objeto del modelo desde el fichero"""
        arima_temp = pickle.load( open( './modelos/modelo_TEMP.p', "rb" ) )
        """Predicciones"""
        predicc_temp, confint = arima_temp.predict(n_periods=tiempo, return_conf_int=True)
        
        """HUMEDAD."""
        if os.path.isfile('./modelos/modelo_HUM.zip') == False:
            self.crear_modelo_arima(dataframe, 'HUM')
        
        """Extraemos el modelo del fichero comprimido."""
        with zipfile.ZipFile('./modelos/modelo_HUM.zip', 'r') as zipObj:
           zipObj.extractall("./")
        """Cargamos el objeto del modelo desde el fichero"""
        arima_hum = pickle.load( open( './modelos/modelo_HUM.p', "rb" ) )
        """Predicciones"""
        predicc_hum, confint = arima_hum.predict(n_periods=tiempo, return_conf_int=True)

        """Componemos el resultado de las predicciones"""
        resultado = pd.DataFrame()
        """Rango de horas"""
        primera_fecha = datetime.now() + timedelta(hours=1)
        resultado['hour'] = pd.date_range(primera_fecha.replace(second=0, microsecond=0), periods=tiempo, freq='H')
        resultado['temp'] = predicc_temp
        resultado['hum'] = predicc_hum

        return resultado

    def get_predicciones_api(self, periodo):
        """Comprobamos si el período es realmente un número."""
        try:
            tiempo = int(periodo)
        except ValueError:
            raise ValueError("El periodo debe ser un número entero.")
            
        """Conectamos con la API Dark Sky y obtenemos la predicción por horas.
            Por defecto devuelve 168 horas de predicción meteorológica."""
        API_KEY = os.environ.get('WEATHER_KEY')
        url = "https://api.darksky.net/forecast/"+API_KEY+"/37.774929,-122.4194183?extend=hourly&units=si"
        respuesta = requests.get(url)
        """Convertimos los datos obtenidos a JSON"""
        datos = respuesta.json()
        prediccion = datos['hourly']['data']
        
        """Componemos el resultado con el periodo pasado como argumento"""        
        horas = 0
        api_horas = []
        api_temps = []
        api_hums = []
        for key in prediccion:
            api_horas.append(datetime.utcfromtimestamp(key['time']).strftime('%Y-%m-%d %H:%M:%S'))
            api_temps.append(key['temperature'])
            api_hums.append(key['humidity']*100.0)
            horas += 1
            if (horas == tiempo): break
        
        """Dataframe final"""
        resultado = pd.DataFrame()
        resultado['hour'] = api_horas
        resultado['temp'] = api_temps
        resultado['hum'] = api_hums
    
        return resultado
        
#if __name__== "__main__":
#    predicc = Prediccion()
    #print(predicc.get_predicciones_api(12))
    #print(predicc.get_predicciones_arima(12))