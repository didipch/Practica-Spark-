from pyspark import SparkContext, SparkConf
import json
import datetime

from pprint import pprint
import datetime
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

def info_date(date):
    "Calcular el dia de la semana"
    month = int(date[5:7])
    day = int(date[8:10])
    week_day = datetime.datetime(2020, month, day).weekday()
    return week_day, day, month

def day_traductor(week_day):
    if week_day == 0:
        name_day = 'Lunes'
    elif week_day == 1:
        name_day = 'Martes'
    elif week_day == 2:
        name_day = 'Miércoles'
    elif week_day == 3:
        name_day = 'Jueves'
    elif week_day == 4:
        name_day = 'Viernes'
    elif week_day == 5:
        name_day = 'Sábado'
    else:
        name_day = 'Domingo'
    return name_day


def get_days(line):
    data = json.loads(line)
    s_date = data["unplug_hourTime"]
    week, day, month = info_date(s_date)
    dia = day_traductor(week)
    if isinstance(s_date, str):
        s_date = s_date[0:10]
        data["unplug_hourTime"] = dia
    else:
        s_date = s_date['$date'][0:10]
        data["unplug_hourTime"] = dia
    return data

def test_get_days():
    line = '{"_id": {"$oid": "6042b8391b91312c9c90cbdf"}, "user_day_code": "101d96477723a56139eaa3ff50839ab2d8364c6f1b1cf2482eccb0429aa3d2a1", "idplug_base": 23, "user_type": 1, "idunplug_base": 1, "travel_time": 726, "idunplug_station": 197, "ageRange": 0, "idplug_station": 228, "unplug_hourTime": "2021-03-01T00:00:00Z", "zip_code": ""}'
    data = get_days(line)
    pprint(data)
    
    line = '{ "_id" : { "$oid" : "5c4b07ea2f38432e007daac2" }, "user_day_code" : "b70eb1a71e53c6351b4835f618a459886e2045a44635e489589dc6d20a501ea6", "idplug_base" : 17, "track" : { "type" : "FeatureCollection", "features" : [ { "geometry" : { "type" : "Point", "coordinates" : [ -3.67156629972222, 40.4296773 ] }, "type" : "Feature", "properties" : { "var" : "28006,ES,Madrid,Madrid,CALLE JOSE ORTEGA Y GASSET 94,Madrid", "speed" : 0, "secondsfromstart" : 448 } }, { "geometry" : { "type" : "Point", "coordinates" : [ -3.6732813, 40.4297554997222 ] }, "type" : "Feature", "properties" : { "var" : "28006,ES,Madrid,Madrid,CALLE JOSE ORTEGA Y GASSET 80,Madrid", "speed" : 7.22, "secondsfromstart" : 389 } }, { "geometry" : { "type" : "Point", "coordinates" : [ -3.6771231, 40.4300635997222 ] }, "type" : "Feature", "properties" : { "var" : "28006,ES,Madrid,Madrid,CALLE JOSE ORTEGA Y GASSET 51,Madrid", "speed" : 0.38, "secondsfromstart" : 329 } }, { "geometry" : { "type" : "Point", "coordinates" : [ -3.68121279972222, 40.43016 ] }, "type" : "Feature", "properties" : { "var" : "28006,ES,Madrid,Madrid,CALLE JOSE ORTEGA Y GASSET 30,Madrid", "speed" : 1.66, "secondsfromstart" : 269 } }, { "geometry" : { "type" : "Point", "coordinates" : [ -3.68271279972222, 40.4300327997222 ] }, "type" : "Feature", "properties" : { "var" : "28006,ES,Madrid,Madrid,CALLE JOSE ORTEGA Y GASSET 22,Madrid", "speed" : 7.16, "secondsfromstart" : 209 } }, { "geometry" : { "type" : "Point", "coordinates" : [ -3.6843273, 40.4270603 ] }, "type" : "Feature", "properties" : { "var" : "28001,ES,Madrid,Madrid,CALLE VELAZQUEZ 45,Madrid", "speed" : 6.63, "secondsfromstart" : 150 } }, { "geometry" : { "type" : "Point", "coordinates" : [ -3.68489809972222, 40.4234290997222 ] }, "type" : "Feature", "properties" : { "var" : "28001,ES,Madrid,Madrid,CALLE GURTUBAY 5,Madrid", "speed" : 4.19, "secondsfromstart" : 90 } } ] }, "user_type" : 1, "idunplug_base" : 11, "travel_time" : 468, "idunplug_station" : 100, "ageRange" : 5, "idplug_station" : 171, "unplug_hourTime" : { "$date" : "2019-01-01T00:00:00.000+0100" }, "zip_code" : "28028" }'
    data = get_days(line)
    pprint(data)


def get_edades(rdd):
    trips = rdd.map(get_days)\
        .filter(lambda x: x["ageRange"]!=0 )\
        .map(lambda x: (x["idunplug_station"], (x["unplug_hourTime"], x["ageRange"],x["idplug_station"])))\
        .groupByKey().mapValues(list)\
        .map(edad_estacion_final).sortBy(lambda x: x)\
        .map(is_return)
    return trips.collect()


def is_return(user_info):
    estation = user_info[0]
    edad_diario = user_info[1][0]
    edad_finde = user_info[1][1]
    estacion_diario = user_info[1][2]
    estacion_finde = user_info[1][3]
    return {"Estación": estation, 
          "Rango edad" : { "A diario": edad_diario,"Fin de semana": edad_finde},
          "Estacion que suelen ir": {"A diario": estacion_diario,"Fin de semana": estacion_finde}}
    

def edad_estacion_final(tupla):
    fin_semana = [0]*6
    diario = [0]*6
    estacion_diario = [0] * 219
    estacion_finde = [0] *219
    for terna in tupla[1]:
        if (terna[0] == 6 or terna[0] == 7 ):
            fin_semana[terna[1]-1] = fin_semana[terna[1]-1] +1 
            if terna[2] != tupla[0]:
                estacion_finde[terna[2]-1] = estacion_finde[terna[2]-1] +1
        else:
            diario[terna[1]-1] = diario[terna[1]-1] +1 
            if terna[2] != tupla[0]:
                estacion_diario[terna[2]-1] = estacion_diario[terna[2]-1] +1
    max_weekend = max(fin_semana)
    max_week = max(diario)
    max_station_weekend = max(estacion_finde)
    max_station_week = max(estacion_diario)
    
    return (tupla[0],(diario.index(max_week) + 1, fin_semana.index(max_weekend) + 1, 
            estacion_diario.index(max_station_week) + 1,
            estacion_finde.index(max_station_weekend) + 1))

def main(sc, months):
     rdd = sc.parallelize([])
     for month in months:
         filename = f"{month}"
         print(f"Adding {filename}")
         rdd = rdd.union(sc.textFile(filename))
             
     trips = get_edades(rdd)
     print("............Starting computations.............")
     pprint(trips)
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Uso: python3 {sys.argv[0]} <file> <file> <file> ...")
    else:
        n = len(sys.argv)
        months = sys.argv[1:]
        
        with initSC(0,0) as sc:
            main(sc, months)