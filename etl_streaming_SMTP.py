from datetime import datetime, timedelta
from email import message
import string
import requests
import json
import numpy as np
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import execute_values
import os.path
import os
import smtplib

dag_path = os.getcwd()  

default_args={
    'owner': 'AdriPrietoPulido',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

def etl_movies_series():
    import requests
    import json
    #import pandas as pd
    #import numpy as np

    ##insert movies##
    url='https://api.themoviedb.org/3/movie/top_rated?api_key=76f3ff736cde9a95c8dde41456f28cda&language=en-US&page={}'
    
    response = requests.get(url)
    if response.status_code==200:
      
            movies = pd.DataFrame(response.json()["results"])[['id','original_title','release_date','genre_ids','vote_average','vote_count']]
            
        
    for i in range(1, 4):
             response = requests.get(url)
             temp_df = pd.DataFrame(response.json()["results"])[['id','original_title','release_date','genre_ids','vote_average','vote_count']]
             movies.append(temp_df, ignore_index=False)
        
        
             #print(movies) ##Visualizo los datos
    #convierto el index como columna del df movies
    movies['index'] = movies.index
    ##convierto el campo genre_ids a lista y completo el largo de la sublista##
    genre_ids=movies['genre_ids'].tolist()

    tamanio=len (genre_ids)
    for i in range(tamanio):
        longitud = len(genre_ids[i])
    
        while longitud<5:
            genre_ids[i].append(0)
        
            longitud+=1
    #convierto la lista genre_ids a dataframe##
    df_genre_ids = pd.DataFrame(genre_ids)

    #convierto el index como columna
    df_genre_ids['index'] = df_genre_ids.index
    #print(df_genre_ids)   
     
    #Uno df movies con df idf_genre_ids por el index
    peliculas=pd.merge(movies,df_genre_ids,on='index',how='left')
       
    ## renombro columnas en df peliculas##
    peliculas.rename(columns = {'original_title':'nombre','release_date':'fecha_lanzamiento','genre_ids':'id_genero','vote_average':'voto_prom','vote_count':'voto_cant',0:'G0',1:'G1',2:'G2',3:'G3',4:'G4'}, inplace = True)

    #creo y completo la columna tipo_streaming
    peliculas["tipo_streaming"] = np.nan
    peliculas["tipo_streaming"].fillna("peliculas", inplace = True)
    #print(peliculas)        
    
##insert series ##
    url='https://api.themoviedb.org/3/tv/top_rated?api_key=76f3ff736cde9a95c8dde41456f28cda&language=en-US&page={}'
    
    response = requests.get(url)
    if response.status_code==200:
      
            series = pd.DataFrame(response.json()["results"])[['id','name','first_air_date','genre_ids','vote_average','vote_count']]
        
        
    for i in range(1, 4):
             response = requests.get(url)
             temp_df = pd.DataFrame(response.json()["results"])[['id','name','first_air_date','genre_ids','vote_average','vote_count']]
             series.append(temp_df, ignore_index=False)
        
        
             #print(series) ##Visualizo los datos

    ##convierto el campo genre_ids a lista y completo el largo de la sublista##
    genre_id=series['genre_ids'].tolist()

    tamanio=len (genre_id)
    for i in range(tamanio):
        longitud = len(genre_id[i])
    
        while longitud<5:
            genre_id[i].append(0)
       
            longitud+=1
    #convierto la lista genre_ids a dataframe##
    df_genre_id = pd.DataFrame(genre_id)

    #convierto el index como columna
    df_genre_id['index'] = df_genre_id.index
    
    #convierto el index como columna del df series
    series['index'] = series.index
    
    #Uno df series con df df_genre_id por el index
    series_tv=pd.merge(series,df_genre_id,on='index',how='left')
    
    ## renombro columnas en df series##
    series_tv.rename(columns = {'name':'nombre','first_air_date':'fecha_lanzamiento','genre_ids':'id_genero','vote_average':'voto_prom','vote_count':'voto_cant',0:'G0',1:'G1',2:'G2',3:'G3',4:'G4'}, inplace = True)

    #creo y completo la columna tipo_streaming
    series_tv["tipo_streaming"] = np.nan
    series_tv["tipo_streaming"].fillna("series", inplace = True)
    #print(series_tv)
    
## uno los dataframe ##
    df_streaming=pd.concat([peliculas,series_tv], ignore_index=True)
    #print(df_streaming)
    
###insert  un DF de generos
    url='https://api.themoviedb.org/3/genre/movie/list?api_key=76f3ff736cde9a95c8dde41456f28cda&language=en-US'
    response = requests.get(url)
    if response.status_code==200:
        
                generos = pd.DataFrame(response.json()["genres"])[['id','name']]
            
            
    for i in range(1, 4):
                response = requests.get(url)
                temp_df = pd.DataFrame(response.json()["genres"])[['id','name']]
                generos.append(temp_df, ignore_index=False)
            
        
                #print(generos) ##Visualizo los datos
    
    ## Modifico ID y nombre de DF generos para cruzarlo con cada id generos de DF df_streaming
    G0=generos.copy()
    G0.rename(columns = {'id':'G0','name':'name0'}, inplace = True)

    G1=generos.copy()
    G1.rename(columns = {'id':'G1','name':'name1'}, inplace = True)

    G2=generos.copy()
    G2.rename(columns = {'id':'G2','name':'name2'}, inplace = True)

    G3=generos.copy()
    G3.rename(columns = {'id':'G3','name':'name3'}, inplace = True)

    G4=generos.copy()
    G4.rename(columns = {'id':'G4','name':'name4'}, inplace = True)
            
            
## uno los dataframe ##
    df_streaming=pd.concat([peliculas,series_tv], ignore_index=True)
    ###Cruzo df  df_streaming con el DF detalle de generos
    df_streaming=pd.merge(df_streaming,G0,on='G0',how='left')
    df_streaming=pd.merge(df_streaming,G1,on='G1',how='left')
    df_streaming=pd.merge(df_streaming,G2,on='G2',how='left')
    df_streaming=pd.merge(df_streaming,G3,on='G3',how='left')
    df_streaming=pd.merge(df_streaming,G4,on='G4',how='left')

    ## Reemplazo los nulls con espacios
    df_streaming['name1'].fillna("", inplace = True)
    df_streaming['name2'].fillna("", inplace = True)
    df_streaming['name3'].fillna("", inplace = True)
    df_streaming['name4'].fillna("", inplace = True)

    #concateno los diferentes campos que tieen un genero
    df_streaming['genero'] =pd.DataFrame( np.where(df_streaming['name1']=='' ,df_streaming.name0, df_streaming.name0.str.cat(df_streaming.name1, sep=', ')))
    df_streaming['genero'] =pd.DataFrame( np.where(df_streaming['name2']=='' ,df_streaming.genero, df_streaming.genero.str.cat(df_streaming.name2, sep=', ')))
    df_streaming['genero'] =pd.DataFrame( np.where(df_streaming['name3']=='' ,df_streaming.genero, df_streaming.genero.str.cat(df_streaming.name3, sep=', ')))
    df_streaming['genero'] =pd.DataFrame( np.where(df_streaming['name4']=='' ,df_streaming.genero, df_streaming.genero.str.cat(df_streaming.name4, sep=', ')))
     
    ## Tomo solo las columnas que me interesa llevar ##
    df_streaming=df_streaming.loc[:, ['id','tipo_streaming','nombre','fecha_lanzamiento','genero','voto_prom','voto_cant' ]]

   ### extraer a csv##
    df_str=pd.DataFrame(df_streaming)
    dag_path = os.getcwd()  
    df_str.to_csv(dag_path+'/processed_data/'+"data_streaming"+".csv", index=False, mode='w')
    #print(df_str)
    print("Se ha exportado el Dataframe")
    
    

    
def enviarMail():
        df_mose=pd.read_csv(dag_path+'/processed_data/'+"data_streaming"+".csv")  
        ###Busco columnas con campos vacios###
        df_vacios=pd.DataFrame()
        df_vacios = df_mose[df_mose.isnull().any(1)]
        df_vacios=df_vacios.columns[df_vacios.isnull().any()]
        print(df_vacios)

        for x in df_vacios:
            col_nan=x
        
        if df_vacios.empty == True:
            print('DataFrame is empty')
        else:
            final="Hola"+"\n"+"La informacion de streaming se cargo con valores nulos"+"\n"+"recomendamos revisar la columna: "+col_nan
            print(final)
        try:
            
            x=smtplib.SMTP('smtp.gmail.com',587)
            x.starttls()
            x.login('adrimarpp@gmail.com','gearofypwjqmjkkl')
            subject='Columnas con campos Vacios'
            body_text=final
            message='Subject: {}\n\n{}'.format(subject,body_text)
            x.sendmail('adrimarpp@gmail.com','marce200452@hotmail.com',message)
            print('Exito')
        except Exception as exception:
            print(exception)
            print('Failure')
     
def conect_RedShift():
    ###Conexion Redshift###
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
    data_base="data-engineer-database"
    user="marce200452_coderhouse"
    #with open("C:/Users/Windows/Downloads/pwd_coder.txt",'r') as f:
    pwd="2NA8hfK5c3" #f.read()

    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Connected to Redshift successfully!")
        
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
        

        
            
#  ###Insertar Datos##
def cargar_tabla():
    #Levanto archivo 
    df_mose=pd.read_csv(dag_path+'/processed_data/'+"data_streaming"+".csv")
    #df_mose = df_mose.drop_duplicates()
    #print(df_mose)
    
    ###Insertar Datos##
    ##Variables##
    data_base="data-engineer-database"
    user="marce200452_coderhouse"
    pwd="2NA8hfK5c3" 
    conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )

    table_name='str_videos_mose'
    dtypes= df_mose.dtypes
    df_mose.dtypes

    cols= list(dtypes.index)
    tipos= list(dtypes.values)
    type_map = {'int64': 'INT','object': 'VARCHAR(500)','object': 'VARCHAR(100)','object': 'VARCHAR(100)','object': 'VARCHAR(100)','float64': 'Decimal(10,2)','int64': 'INT'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
        
    # Combine column definitions into the CREATE TABLE statement
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]

    #Crear la tabla
    cur = conn.cursor()

    ## Generar los valores a insertar
    values = [tuple(x) for x in df_mose.to_numpy()]

    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        
    # Execute the transaction to insert the data
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values) ## insertar los datos 
    cur.execute("COMMIT")

    print('Se ha cargado la Tabla')


with DAG( 
    default_args=default_args,
    dag_id='etl_streaming_smtp',
    description= 'ETL de streaming (peliculas, series) SMTP',
    start_date=datetime(2022,8,1,2),
    schedule_interval='@daily' #'0 30 7 ? * WED *'
    ) as dag:
    task1= PythonOperator(
        task_id='etl_movies_series',
        python_callable= etl_movies_series,
    )
    task2= PythonOperator(
        task_id='envio_info',
        python_callable= enviarMail,
    )
    task3= PythonOperator(
        task_id='conect_RedShift',
        python_callable= conect_RedShift,
    )
    task4= PythonOperator(
        task_id='cargar_tabla',
        python_callable= cargar_tabla,
    )
   
    
    
    task1>>task2>>task3>>task4