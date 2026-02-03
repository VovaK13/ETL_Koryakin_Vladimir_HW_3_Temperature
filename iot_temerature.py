from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from sqlalchemy import types

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1)
}

def process_and_save_data():
    df = pd.read_csv('/opt/airflow/dags/IOT-temp.csv')

    df_filtered = df[df['out/in'] == 'In'].copy()
    
    df_filtered['noted_date'] = pd.to_datetime(
        df_filtered['noted_date'], 
        format='%d-%m-%Y %H:%M'
    )
    
    df_filtered['date_formatted'] = df_filtered['noted_date'].dt.strftime('%Y-%m-%d')
    df_filtered['date_only'] = df_filtered['noted_date'].dt.date
    
    daily_stats = df_filtered.groupby('date_only').agg({
        'temp': ['mean', 'min', 'max']
    }).reset_index()
    
    daily_stats.columns = ['date', 'avg_temp', 'min_temp', 'max_temp']
    
    hottest_days = daily_stats.sort_values('avg_temp', ascending=False).head(5)
    coldest_days = daily_stats.sort_values('avg_temp', ascending=True).head(5)
    
    print("\n5 самых жарких дней:")
    print(hottest_days.to_string())
    
    print("\n5 самых холодных дней:")
    print(coldest_days.to_string())
    
    lower_bound = np.percentile(df_filtered['temp'], 5)
    upper_bound = np.percentile(df_filtered['temp'], 95)
    
    print(f"\nГраницы очистки: нижняя {lower_bound:.2f}, верхняя {upper_bound:.2f}")
    
    df_cleaned = df_filtered[
        (df_filtered['temp'] >= lower_bound) & 
        (df_filtered['temp'] <= upper_bound)
    ].copy()
    
    print(f"Строк после очистки: {len(df_cleaned)}")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    df_cleaned.to_sql(
        'iot_temperature_processed',
        engine,
        if_exists='replace',
        index=False,
        dtype={
            'id': types.String(length=255),
            'room_id/id': types.String(length=255),
            'noted_date': types.DateTime(),
            'temp': types.Float(),
            'out/in': types.String(length=10),
            'date_formatted': types.String(length=10),
            'date_only': types.Date()
        }
    )
    print("Основные данные сохранены в таблицу 'iot_temperature_processed'")
    
    daily_stats.to_sql(
        'iot_daily_stats',
        engine,
        if_exists='replace',
        index=False,
        dtype={
            'date': types.Date(),
            'avg_temp': types.Float(),
            'min_temp': types.Float(),
            'max_temp': types.Float()
        }
    )
    print("Статистика по дням сохранена в таблицу 'iot_daily_stats'")
    
    hottest_days.to_sql(
        'iot_hottest_days',
        engine,
        if_exists='replace',
        index=False,
        dtype={
            'date': types.Date(),
            'avg_temp': types.Float(),
            'min_temp': types.Float(),
            'max_temp': types.Float()
        }
    )
    print("Самые жаркие дни сохранены в таблицу 'iot_hottest_days'")
    
    coldest_days.to_sql(
        'iot_coldest_days',
        engine,
        if_exists='replace',
        index=False,
        dtype={
            'date': types.Date(),
            'avg_temp': types.Float(),
            'min_temp': types.Float(),
            'max_temp': types.Float()
        }
    )
    print("Самые холодные дни сохранены в таблицу 'iot_coldest_days'")

dag = DAG(
    'iot_temperature',
    default_args=default_args,
    description='Простая обработка данных температуры IoT и сохранение в PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

process_task = PythonOperator(
    task_id='process_and_save_data',
    python_callable=process_and_save_data,
    dag=dag
)

process_task