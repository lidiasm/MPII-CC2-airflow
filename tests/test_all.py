#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests para comprobar el funcionamiento acerca de las clases que contienen la lógica
de negocio así como el de los microservicios.

@author: Lidia Sánchez Mérida
"""
import pandas
import pytest
import sys
sys.path.append("../src/")

"""Clase con la lógica de la aplicación para realizar las predicciones"""
import prediccion
pred = prediccion.Prediccion()

"""Microservicio de la versión 1"""
import api_v1
app_v1 = api_v1.app.test_client()

def test1_get_predicciones_arima():
    """Test para comprobar que el período indicado es un número. En este caso
        no lo es y por lo tanto se produce una excepción."""
    with pytest.raises(ValueError):
        assert pred.get_predicciones_arima('hola')
        
def test2_get_predicciones_arima():
    """Test para comprobar que el período indicado es un número. En este caso
        sí lo es y por lo tanto debe de obtener los resultados de la predicción."""
    respuesta = pred.get_predicciones_arima(24)
    assert type(respuesta) == pandas.core.frame.DataFrame

def test_obtener_predicciones_arima():
    """Test para comprobar el funcionamiento del microservicio de la versión 1."""
    respuesta = app_v1.get('/arima/24')
    assert (respuesta.status_code == 200)
    
if __name__ == '__main__':
    pytest.main()
