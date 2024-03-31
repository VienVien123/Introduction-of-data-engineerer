
# import datetime as dt
# import pandas as pd
# import pymongo
# import random
# import time

# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# def insertMongoDB():
#     try_count = 0
#     max_try = 3
#     success = False

#     client = pymongo.MongoClient('mongodb://admin:admin@mongodb:27017')
#     db = client['test-collection']
#     collection1 = db['collection1']

#     while try_count < max_try and not success:
    
#         df = pd.read_csv("/opt/airflow/dags/employees.csv")
#         random_row = df.sample(n=1)

#         record = random_row.iloc[0]
#         print("Chuẩn bị thêm dữ liệu vào hệ thống")
#         try:
#             collection1.insert_one(record.to_dict())
#             print("Chèn dữ liệu thành công")
#             success = True
#         except pymongo.errors.DuplicateKeyError:
#             print("Đã tồn tại bản ghi trong hệ thống")
#             try_count += 1
#             time.sleep(180) 
#     else:
#         if not success:
#             print("Đã đạt tới số lần thử tối đa. Không thể chèn dữ liệu.")
#     client.close()
# default_args = {
#     'owner': 'VienVien',
#     'start_date': dt.datetime.now() - dt.timedelta(minutes=3),
#     'retries': 1,
#     'retry_delay': dt.timedelta(minutes=1)
# }
# with DAG(
#     'Vien',
#     default_args=default_args,
#     tags=['Labbb'],
#     schedule_interval=dt.timedelta(minutes=1)
# ) as dag:
#     print_starting = BashOperator(
#         task_id='starting',
#         bash_command='echo "Chương trình thêm dữ liệu vào MongoDB..."'
#     )

#     insertData = PythonOperator(
#         task_id='insert_data',
#         python_callable=insertMongoDB
#     )

#     end = BashOperator(
#         task_id='end',
#         bash_command='echo "Hoàn thành việc chèn dữ liệu."'
#     )

# print_starting >> insertData >> end
import datetime as dt
import pandas as pd
import pymongo
import random
import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def insertMongoDB():
    try_count = 0
    max_try = 3
    success = False

    client = pymongo.MongoClient('mongodb://admin:admin@mongodb:27017')
    db = client['test-collection']
    collection1 = db['collection1']

    while try_count < max_try and not success:
    
        df = pd.read_csv("/opt/airflow/dags/employees.csv")
        random_row = df.sample(n=1)

        record = random_row.iloc[0]
        print(f"{dt.datetime.now()} - Chuẩn bị thêm dữ liệu vào hệ thống")
        try:
            collection1.insert_one(record.to_dict())
            print(f"{dt.datetime.now()} - Chèn dữ liệu thành công")
            success = True
        except pymongo.errors.DuplicateKeyError:
            print(f"{dt.datetime.now()} - Đã tồn tại bản ghi trong hệ thống")
            try_count += 1
            time.sleep(180) 
    else:
        if not success:
            print(f"{dt.datetime.now()} - Đã đạt tới số lần thử tối đa. Không thể chèn dữ liệu.")
    client.close()

default_args = {
    'owner': 'VienVien',
    'start_date': dt.datetime.now() - dt.timedelta(minutes=3),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

with DAG(
    'Vien',
    default_args=default_args,
    tags=['Labbb'],
    schedule_interval=dt.timedelta(minutes=1)
) as dag:
    print_starting = BashOperator(
        task_id='starting',
        bash_command='echo "Chương trình thêm dữ liệu vào MongoDB..."'
    )

    insertData = PythonOperator(
        task_id='insert_data',
        python_callable=insertMongoDB
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Hoàn thành việc chèn dữ liệu."'
    )

print_starting >> insertData >> end
