from datetime import timedelta
from textwrap import dedent

# El objeto DAG; necesitaremos esto para crear una instancia de un DAG
from airflow import DAG

# Operadores; ¡Necesitamos esto para operar!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
# Estos argumentos se transmitirán a cada operador
# Puede anularlos por tarea durante la inicialización del operador
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['edgarisaias4040@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    'ejemplo',
    default_args=default_args,
    description='Un simple ejemplo de DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    # t1, t2 y t3 son ejemplos de tareas creadas por operadores de instancia
    t1 = BashOperator(
        task_id='imprimir_fecha',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='pausar_8',
        depends_on_past=False,
        bash_command='sleep 8',
        retries=3,
    )
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )

    dag.doc_md = __doc__  # siempre que tenga una cadena de documentos al comienzo del DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # de lo contrario, escríbalo así
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )

    t1 >> [t2, t3]