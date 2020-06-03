covid19_daily_statewise_pipeline
================================
Introduction
----
Airflow Pipeline to fetch daily COVID-19 data statewise.
### Documentation:
https://docs.google.com/document/d/1UZynmpo2r2aj-cf55uNDuRFk_-gpvOt0FQXj2p7EYIk/edit

### COVID API:
https://api.covid19india.org/states_daily.json

### Setup:
    - Install: pip install apache-airflow
    
    - After Installation, a directory named Airflow would be created in the home/user/ directory.

    - Inside the Airflow directory, open airflow.cfg -> locate load_examples flag and set it to False.
    
    - Create folder named dags and output inside Airflow directory respectively.

### Directory Structure Inside airflow
```
+-- airflow
|  +--airflow.cfg.py
|  +--credentials.json
|  +--dags
   |+--covid19_daily_statewise.py
|  +--output
   |+--2020-05-01.csv
   |+--2020-05-02.csv
|  +--logs
```

### Run:
    - Save file covid19_daily_statewise.py inside dags folder.

    - Save credentials.json inside airflow directory.
    
    - Inside covid19_daily_statewise.py, edit CSV_PATH field to the path of airflow/output as per your system.
    
    - Inside covid19_daily_statewise.py, edit credentials field to the path of airflow/credentials.json as per your system.

    - Open terminal and run following commands (different tabs):
        - airflow webserver
        - airflow scheduler
        - airflow initdb
     
    - Open browser -> localhost:8080
    
    - Clock in DAGs and locate pipeline named covid19_daily_statewise.
    
    - Toggle On button.

