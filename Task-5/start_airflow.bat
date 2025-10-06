@echo off
echo Starting Airflow ETL Pipeline Demo...
echo =====================================

cd /d "d:\Data-Engineering-Buildables-Fellowship\Task-5"

echo Activating virtual environment...
call airflow_new\Scripts\activate.bat

echo Setting Airflow home directory...
set "AIRFLOW_HOME=d:\Data-Engineering-Buildables-Fellowship\Task-5\airflow"

echo.
echo Available commands:
echo 1. Run ETL Pipeline manually: python run_etl_dag.py
echo 2. Start Airflow webserver: airflow webserver --port 8080
echo 3. List DAGs: airflow dags list
echo 4. Test DAG: airflow dags test etl_pipeline 2025-10-06
echo.

echo Current DAGs in system:
airflow dags list

echo.
echo To run the ETL pipeline manually, type: python run_etl_dag.py
echo To start Airflow webserver, type: airflow webserver --port 8080
echo.

cmd /k