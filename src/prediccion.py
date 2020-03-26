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
import os.path
from datetime import datetime
import numpy
import zipfile

class Prediccion:
    
    def __init__(self):
        """Conectamos con el contenedor MongoDB"""
        self.mongodb = pymongo.MongoClient('mongodb://localhost:27017')
        """Accedemos a la base de datos y en particular a la colección"""
        self.coleccion = self.mongodb['PrediccionesBD']['Datos']
    
    def get_predicciones_arima(self, periodo):
        """Comprobamos si el período es realmente un número."""
        try:
            tiempo = int(periodo)
        except ValueError:
            raise ValueError("El periodo debe ser un número entero.")
            
        """Obtenemos los datos del contenedor MongoDB"""
        datosbd = self.coleccion.find_one({'index':'SF'})
        """Lo pasamos a dataframe para trabajar con ARIMA"""
        dataframe = pd.DataFrame(datosbd["datos"])
        
        """Si existe el modelo creado en un archivo lo cargamos.
            Si no, creamos el modelo y lo guardamos en un fichero."""
        if os.path.isfile('../modelos/temperatura.zip'):
            """Extraemos el modelo del fichero comprimido."""
            with zipfile.ZipFile('../modelos/temperatura.zip', 'r') as zipObj:
               zipObj.extractall("../")
            """Cargamos el objeto del modelo desde el fichero"""
            arima_temp = pickle.load( open( '../modelos/modelo_temp.p', "rb" ) )
        else:
            arima_temp = pm.auto_arima(dataframe['TEMP'].dropna(), start_p=1, 
               start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False,
               start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)
            """Guardamos el modelo como objeto en un fichero"""
            pickle.dump(arima_temp, open( "../modelos/modelo_temp.p", "wb" ) )
            """Lo comprimimos para luego subirlo a GitHub"""
            zipObj = zipfile.ZipFile('../modelos/temperatura.zip', 'w', zipfile.ZIP_DEFLATED)
            zipObj.write("../modelos/modelo_temp.p")
            zipObj.close()
 
        """Obtenemos las predicciones de temperatura"""
        predicc_temp, confint = arima_temp.predict(n_periods=tiempo, return_conf_int=True)

        """Predicciones de humedad"""
        if os.path.isfile('../modelos/humedad.zip'):
            """Extraemos el modelo del fichero comprimido."""
            with zipfile.ZipFile('../modelos/humedad.zip', 'r') as zipObj:
               zipObj.extractall("../")
            """Cargamos el objeto del modelo desde el fichero"""
            arima_hum = pickle.load( open( '../modelos/modelo_hum.p', "rb" ) )
        else:
            arima_hum = pm.auto_arima(dataframe['HUM'].dropna(), start_p=1, 
              start_q=1, test='adf', max_p=3, max_q=3, m=1, d=None, seasonal=False,
              start_P=0, D=0, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)
            """Guardamos el modelo como objeto en un fichero"""
            pickle.dump(arima_hum, open( "../modelos/modelo_hum.p", "wb" ) )
            """Lo comprimimos para luego subirlo a GitHub"""
            zipObj = zipfile.ZipFile('../modelos/humedad.zip', 'w', zipfile.ZIP_DEFLATED)
            zipObj.write("../modelos/modelo_hum.p")
            zipObj.close()
        
        """Obtenemos las predicciones"""
        predicc_hum, confint = arima_hum.predict(n_periods=tiempo, return_conf_int=True)
        
        """Componemos el resultado de las predicciones"""
        resultado = pd.DataFrame()
        resultado['hour'] = pd.date_range(datetime.now().replace(second=0, microsecond=0), periods=tiempo, freq='H')
        resultado['temp'] = numpy.array(predicc_temp)
        resultado['hum'] = numpy.array(predicc_hum)
        
        return resultado


if __name__== "__main__":
    predicc = Prediccion()
    print("\nResultado\n",predicc.get_predicciones_arima(24))