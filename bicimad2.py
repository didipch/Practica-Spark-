# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
import json
import datetime
from pprint import pprint
import sys


def initSC(cores, memory):
    conf = SparkConf()\
           .setAppName(f"Bicimad Return Trips {cores} cores {memory} mem")
           # .set('spark.cores.max', cores)\
           # .set('spark.executorEnv.PYTHONHASHSEED',"123")\
           # .set('spark.executor.memory', memory)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    #sc.addPyFile("py.zip")
    return sc


#Cogemos los datos sobre el año 2020, pero podria generalizarse
def info_date(date):
    month = int(date[5:7])
    day = int(date[8:10])
    week_day = datetime.datetime(2020, month, day).weekday()
    return week_day, day, month


def get_days(line):
    dias = ['Lunes','Martes','Miércoles','Jueves','Viernes','Sábados','Domingos']
    data = json.loads(line)
    s_date = data["unplug_hourTime"]
    week, day, month = info_date(s_date)
    dia = dias[week]
    if isinstance(s_date, str):
        s_date = s_date[0:10]
        data["unplug_hourTime"] = dia
    else:
        s_date = s_date['$date'][0:10]
        data["unplug_hourTime"] = dia
    return data


def get_info(rdd):
    dias = ['Lunes','Martes','Miércoles','Jueves','Viernes','Sábados','Domingos']
    for i in range(7):
        dia = dias[i]
        trips = rdd.map(get_days)\
            .filter(lambda x: x["unplug_hourTime"] == dia)\
            .map(lambda x: (x["unplug_hourTime"], (x["ageRange"], x["idunplug_station"],x["idplug_station"])))\
            .groupByKey().mapValues(list)\
            .map(cuantas_salen_por_dia_edad).sortBy(lambda x: x)\
            .map(is_return)
        print(trips.collect())



def cuantas_salen_por_dia_edad(tupla): 
    lista = [0]*7
    for terna in tupla[1]:
            lista[terna[0]] += 1
    rango = lista.copy()
    rango.pop(0)
    maximo = max(rango)
    mayor = lista.index(maximo)
    return(tupla[0],lista, mayor)

     
def is_return(info):
    edades = ['no determinada', '0-16 años', '17-18 años', '19-26 años', '27-40 años', '41-65 años', '+ 66 años']
    print(f'Los {info[0]} también se han cogido {info[1][0]} bicicletas del que no se puede determinar el rango de edad del usuario')
    for j in range(6):
        print(f'Los {info[0]} el grupo de edad {edades[j+1]} coge un total de {info[1][j+1]} bicicletas ')
    print(f'En total han salido {sum(info[1])} bicicletas los {info[0]}')
    print(f'Los {info[0]}, {edades[info[2]]} es el rango de edad que coge más bicicletas' )
        
    
def main(sc, months):
     rdd = sc.parallelize([])
     for month in months:
         filename = f"{month}"
         print(f"Adding {filename}")
         rdd = rdd.union(sc.textFile(filename))
     get_info(rdd)
    
     
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Uso: python3 {sys.argv[0]} <file> <file> <file> ...")
    else:
        months = sys.argv[1:]
        
        with initSC(0,0) as sc:
            main(sc, months)