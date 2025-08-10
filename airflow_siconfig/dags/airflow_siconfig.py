from airflow.models import DAG
import pendulum
#from airflow.utils.dates import days_ago
#from airflow.operators.python_operator import PythonOperator 
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator 
import requests

from os.path import join
import pandas as pd

with DAG(
    'dados_siconfig',
    schedule_interval='0 0 * * 1', #executar toda segunda-feira,
    start_date=pendulum.datetime(2025, 7, 28, tz="UTC") #Executa todos as segundas
) as dag:
    
    #Criando uma pasta onde será armazenado os dados,caso não houver uma pasta criada
    tarefa_01 = BashOperator(
        task_id = 'cria_pasta',
        bash_command= 'mkdir -p "/home/felipe-ub20/Documentos/pipeline_siconfig/data"'
    )

    def extrai_dados():
        #Colocando os Parâmetros obrigatórios 
        exercicio = 2024 #Ano de referência dos dados
        periodo = 6 #Bimestre de referência dos dados
        tipo_demonstrativo = 'RREO' #Tipo de relatório entregue "RREO ou RREO Simplificado"
        id_ente = 1100205 #Código IBGE do Ente.

        api_base = 'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo'
        url = f"{api_base}?an_exercicio={exercicio}&nr_periodo={periodo}&co_tipo_demonstrativo={tipo_demonstrativo}&id_ente={id_ente}"
        
        
        response = requests.get(url)
        #extraindo os dados da lista que está dentro dos dicionários 
        lista_dados = []
        for pasta in response.json()['items']:
            lista_dados.append(pasta)
        
        #Criando um Data Frame
        dados_brutos  = pd.DataFrame(lista_dados)

        #Salvando os dados brutos 
        caminho_da_pasta = "/home/felipe-ub20/Documentos/pipeline_siconfig/data"
        dados_brutos.to_csv(f"{caminho_da_pasta}/dados_brutos.csv", index=False, encoding="utf-8")


    tarefa_02 = PythonOperator(
        task_id = "extraindo_dados",
        python_callable= extrai_dados
    )

    tarefa_01 >> tarefa_02

