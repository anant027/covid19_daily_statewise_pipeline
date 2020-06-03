from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from google.cloud import bigquery

import requests
import csv
from datetime import date
import google.oauth2


args = {'owner': 'anant', 'start_date': datetime(2020, 5, 1),
        # 'end_date': datetime(2020, 6, 30),
        'retries': 2,
        'retry_delay': timedelta(minutes=5)}

dag = DAG('covid19_daily_statewise', default_args=args)

PROJECT_NAME = "chrome-epigram-271706"
DATASET_NAME = "assignment"
TABLE_NAME = "covid19_daily_statewise"
CSV_PATH = '/home/nineleaps/airflow/output'

credentials = google.oauth2.service_account.Credentials.from_service_account_file(
    '/home/nineleaps/airflow/credentials.json')

client = bigquery.Client(credentials=credentials, project=PROJECT_NAME)



def load_csv(**kwargs):
    months = {
    1: 'Jan',
    2: 'Feb',
    3: 'Mar',
    4: 'Apr',
    5: 'May',
    6: 'Jun',
    7: 'Jul',
    8: 'Aug',
    9: 'Sep',
    10: 'Oct',
    11: 'Nov',
    12: 'Dec'
    }

    states = {
        "AN": "Andaman and Nicobar Islands",
        "AP": "Andhra Pradesh",
        "AR": "Arunachal Pradesh",
        "AS": "Assam",
        "BR": "Bihar",
        "CH": "Chandigarh",
        "CT": "Chhattisgarh",
        "DN": "Dadra and Nagar Haveli",
        "DD": "Daman and Diu",
        "DL": "Delhi",
        "GA": "Goa",
        "GJ": "Gujarat",
        "HR": "Haryana",
        "HP": "Himachal Pradesh",
        "JK": "Jammu and Kashmir",
        "JH": "Jharkhand",
        "KA": "Karnataka",
        "KL": "Kerala",
        "LA": "Ladakh",
        "LD": "Lakshadweep",
        "MP": "Madhya Pradesh",
        "MH": "Maharashtra",
        "MN": "Manipur",
        "ML": "Meghalaya",
        "MZ": "Mizoram",
        "NL": "Nagaland",
        "OR": "Odisha",
        "PY": "Puducherry",
        "PB": "Punjab",
        "RJ": "Rajasthan",
        "SK": "Sikkim",
        "TN": "Tamil Nadu",
        "TG": "Telangana",
        "TR": "Tripura",
        "UP": "Uttar Pradesh",
        "UT": "Uttarakhand",
        "WB": "West Bengal"
    }
    d = str(kwargs['execution_date'])
    mon = d.split('-')[1]
    dat = d.split('-')[2].split('T')[0]
    full_date = f'{dat}-{months.get(int(mon))}-20'
    csv_date = f'{d.split("-")[0]}-{mon}-{dat}'

    csv_file = open(f'{CSV_PATH}/{csv_date}.csv', 'w')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['Date', 'State', 'Confirmed', 'Recovered', 'Deceased'])
    
    url = 'https://api.covid19india.org/states_daily.json'

    try:
        res = requests.get(url)

        confirmed = {}
        recovered = {}
        deceased = {}

        for data in res.json()['states_daily']:
            if data['date'] == full_date:
                if data['status'] == 'Confirmed':
                    confirmed = data
                    confirmed.pop('date')
                    confirmed.pop('status')

                elif data['status'] == 'Recovered':
                    recovered = data
                    recovered.pop('date')
                    recovered.pop('status')

                elif data['status'] == 'Deceased':
                    deceased = data
                    deceased.pop('date')
                    deceased.pop('status')

        for k, v in confirmed.items():
            if k not in ['un', 'tt']:
                csv_writer.writerow([csv_date, states.get(k.upper()), int(v), int(recovered.get(k)), int(deceased.get(k))])

        print(f'Loaded Data of Date {csv_date} Into CSV File Successfully!')
    except Exception as e:
        print(f'Exception: {e}')


def load_table(**kwargs):
    schema = [
        bigquery.SchemaField("Date", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("State", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Confirmed", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Recovered", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Deceased", "INTEGER", mode="REQUIRED")
    ]

    dataset_ref = client.get_dataset(DATASET_NAME)
    table_ref = dataset_ref.table(TABLE_NAME)

    # Creating Table if not exists...
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)

    # Uploading CSV File to table...
    d = str(kwargs['execution_date'])
    mon = d.split('-')[1]
    dat = d.split('-')[2].split('T')[0]
    exec_date = f'{d.split("-")[0]}-{mon}-{dat}'

    job_config = bigquery.LoadJobConfig(schema=schema)
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open(f'{CSV_PATH}/{exec_date}.csv', "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()
    print('Upload Complete !')
    print(f"Loaded {job.output_rows} Row(s) Into Big Query Table {DATASET_NAME}.{TABLE_NAME}")


def read_table(**kwargs):
    d = str(kwargs['execution_date'])
    mon = d.split('-')[1]
    dat = d.split('-')[2].split('T')[0]
    exec_date = f'{d.split("-")[0]}-{mon}-{dat}'

    with open(f'{CSV_PATH}/{exec_date}.csv', "r") as f:
        reader = csv.reader(f, delimiter=",")
        data = list(reader)
        row_count = len(data)-1

    # Reading from table
    query_job = client.query(f'SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_NAME} WHERE Date = "{exec_date}"')
    result = query_job.result()
    print(f'Percentage Upload: {round(list(result)[0][0] / row_count * 100, 2)} %')


# schedule_interval='0 */6 * * *',


t1 = PythonOperator(task_id='load_csv',
                    provide_context=True,
                    python_callable=load_csv,
                    dag=dag)

t2 = PythonOperator(task_id='load_table',
                    provide_context=True,
                    python_callable=load_table,
                    dag=dag)

t3 = PythonOperator(task_id='read_table',
                    provide_context=True,
                    python_callable=read_table,
                    dag=dag)


t1 >> t2 >> t3
