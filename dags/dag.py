import sqlite3
import pandas as pd
import csv

from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from os import path
from sqlite3 import connect


# Antes de começar é preciso definir o caminho dos arquivos para facilitar a escrita do código

db_sqlite = '/home/luana/airflow_tooltorial/data/Northwind_small.sqlite'

output_orders = '/home/luana/airflow_tooltorial/data/Northwind_small.sqlite'

count_txt = '/home/luana/airflow_tooltorial/data/data/count.txt'

final_output = '/home/luana/airflow_tooltorial/data/final_output.txt'


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##



# Função que exporta o banco de dados sqlite para csv
def export_tables_to_csv():
    # Conectar ao banco de dados Northwind
    conn = sqlite3.connect(db_sqlite)

    # Obter o nome de todas as tabelas do banco de dados
    query = 'SELECT * FROM "Order";'

    df_transform = pd.read_sql_query(query, conn)

    # Para cada tabela, ler os dados em um DataFrame pandas e exportar para um arquivo CSV
    df_transform.to_csv('output_orders.csv', index=False)

    # Fechar a conexão com o banco de dados
    conn.close()


# Função que lê os arquivos da tabela OrderDetail do banco de dados
def quantity_calculate():
    # Conectar ao banco de dados Northwind
    conn = sqlite3.connect(db_sqlite)

    # Obter dados da tabela
    query = 'SELECT * FROM OrderDetail;'
    df_od = pd.read_sql_query(query, conn)

    #Fazer o join da tabela co o csv criado
    df_output = pd.read_csv('output_orders.csv')
    
    # Faz o merge dos dois arquivos
    df_merged = pd.merge(df_od, df_output, left_on='OrderId', right_on='Id', how='inner')

    # Calcular a quantidade para o ShipCity = Rio de Janeiro
    quantity = df_merged[df_merged['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()

    # Salvar o arquivo em .txt
    with open ('count.txt', 'w') as f:
        f.write(str(quantity))

    # Fechar a conexão com o banco de dados
    conn.close()
    


with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """


    # Task que chama a função de exportação em csv
    task1 = PythonOperator(
        task_id='task1',
        python_callable=export_tables_to_csv,
        provide_context=True,
    )
    task1.doc_md = dedent(
        """\
    #### Task Documentation

    Essa task lê os dados da tabela 'Order' do banco de dados Northwind e exporta um arquivo 'output_orders.csv'.

    """
    )


    # Task que faz join da tabela "OrderDetail" com o csv exportado
    task2 = PythonOperator(
        task_id='task2',
        python_callable=quantity_calculate,
        provide_context=True,
    )
    task2.doc_md = dedent(
        """\
    #### Task Documentation

    Essa task lê os dados da tabela 'OrderDetails'; 
    Faz join com o arquivo 'output_orders.csv' gerado;
    Calcula o total de quantidades vendidas para ShipCity == Rio de Janeiro;
    Exporta a contagem em arquivo 'count.txt'
    
    """
    )

 
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )


task1 >> task2 >> export_final_output