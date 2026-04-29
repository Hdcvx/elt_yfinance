from airflow.decorators import dag, task
from datetime import datetime
import sys

sys.path.insert(0, "/opt/airflow/src")


@dag(
    dag_id="pipeline_financeiro",
    schedule="0 18 * * 1-5",  # 18h de segunda a sexta
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["financeiro", "elt"],
)
def pipeline_financeiro():

    @task
    def ingerir_bronze():
        from bronze import extract_load, TICKERS
        extract_load(TICKERS, days=30)

    @task
    def transformar_dbt():
        import subprocess
        resultado = subprocess.run(
            ["dbt", "run", "--project-dir", "/opt/airflow/dbt", "--profiles-dir", "/opt/airflow/dbt"],
            capture_output=True,
            text=True,
        )
        if resultado.returncode != 0:
            raise Exception(f"dbt falhou:\n{resultado.stderr}")

    ingerir_bronze() >> transformar_dbt()


pipeline_financeiro()
