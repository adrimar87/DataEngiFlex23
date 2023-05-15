import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

### Levanto informacion de la API ###
url='https://api.themoviedb.org/3/movie/top_rated?api_key=76f3ff736cde9a95c8dde41456f28cda&language=en-US&page={}'
    
response = requests.get(url)
if response.status_code==200:
      
            data = pd.DataFrame(response.json()["results"])[['id','original_title','release_date','vote_average','vote_count']]
        
        
for i in range(1, 4):
             response = requests.get(url)
             temp_df = pd.DataFrame(response.json()["results"])[['id','original_title','release_date','vote_average','vote_count']]
             data.append(temp_df, ignore_index=False)
        
        
             #print(data) ##Visualizo los datos
             

### Conexion Redshift ###
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base="data-engineer-database"
user="marce200452_coderhouse"
pwd="2NA8hfK5c3" 

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
    
    
 ### Insertar Datos ###
    def cargar_en_redshift(conn, table_name, dataframe):
        dtypes= data.dtypes
        data.dtypes
        cols= list(dtypes.index )
        tipos= list(dtypes.values)
        type_map = {'int64': 'INT','object': 'VARCHAR(50)','object': 'VARCHAR(50)','float64': 'Decimal(10,2)','int64': 'INT'}
        sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
       
    # Combine column definitions into the CREATE TABLE statement
        column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_defs)}
            );
            """
        #print(table_schema)
    #Crear la tabla
        cur = conn.cursor()
        cur.execute(table_schema)
    # Generar los valores a insertar
        values = [tuple(x) for x in data.to_numpy()]
        # Definir el INSERT
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        
      
    # Execute the transaction to insert the data
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print('Proceso terminado')
    
    ## Llama funcion ##
    cargar_en_redshift(conn=conn, table_name='pelicula_detalle', dataframe=data)
    
    