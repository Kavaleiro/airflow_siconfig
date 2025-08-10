from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator 

#Esse é apenas um teste para verficar se o código está certo
"""
Algumas versões mais recentes do Airflow descontinuaram a função days_ago. 
Se ao tentar utilizá-la você receber um aviso, não se preocupe. 
Em substituição, você pode utilizar a biblioteca pendulum
import pendulum
pendulum.today('UTC').add(days=-N)"""


with DAG(
    'meu_primeiro',
    start_date=days_ago(2),
    schedule_interval='@daily'
) as dag:
    
    tarefa_01 = EmptyOperator(task_id='tarefa_1')
    tarefa_02 = EmptyOperator(task_id='tarefa_2')
    tarefa_03 = EmptyOperator(task_id='tarefa_3')
    tarefa_04 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/felipe-ub20/Documentos/pipeline_siconfig/pasta={{data_interval_end}}"'
    )

    tarefa_01 >> [tarefa_02, tarefa_03]
    tarefa_03 >> tarefa_04