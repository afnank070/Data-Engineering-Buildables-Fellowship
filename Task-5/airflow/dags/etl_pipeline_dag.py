from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import psycopg2
import boto3
from botocore.exceptions import ClientError
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline: MinIO -> Transform -> PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'minio', 'postgresql', 'movies'],
)

def extract_from_minio(**context):
    """Extract CSV file (simulating MinIO extraction)"""
    try:
        # For demo purposes, we'll use the local file directly
        # In production, this would download from MinIO
        local_file_path = 'd:/Data-Engineering-Buildables-Fellowship/Task-5/Movies.csv'
        
        # Check if file exists
        if os.path.exists(local_file_path):
            print(f"Successfully located CSV file: {local_file_path}")
            print("(In production, this would download from MinIO)")
            return local_file_path
        else:
            raise FileNotFoundError(f"CSV file not found at {local_file_path}")
        
    except Exception as e:
        print(f"Error in extract step: {e}")
        raise

def transform_data(**context):
    """Transform the extracted data"""
    try:
        # Read the CSV file with proper encoding
        input_file = 'd:/Data-Engineering-Buildables-Fellowship/Task-5/Movies.csv'
        try:
            data = pd.read_csv(input_file, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                data = pd.read_csv(input_file, encoding='latin-1')
            except UnicodeDecodeError:
                data = pd.read_csv(input_file, encoding='cp1252')
        
        print(f"Original data shape: {data.shape}")
        print(f"Original columns: {list(data.columns)}")
        
        # Data cleaning and transformation
        # Remove rows with missing critical data
        data = data.dropna(subset=['movie_title', 'imdb_score'])
        
        # Fill missing numeric values with 0
        numeric_columns = ['num_critic_for_reviews', 'duration', 'director_facebook_likes',
                          'actor_3_facebook_likes', 'actor_1_facebook_likes', 'gross',
                          'num_voted_users', 'cast_total_facebook_likes', 'facenumber_in_poster',
                          'num_user_for_reviews', 'budget', 'title_year', 'actor_2_facebook_likes']
        
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)
        
        # Clean movie titles (remove extra characters)
        data['movie_title'] = data['movie_title'].str.strip()
        
        # Ensure IMDB score is within valid range
        data = data[(data['imdb_score'] >= 0) & (data['imdb_score'] <= 10)]
        
        # Select and rename columns to match database schema
        columns_mapping = {
            'movie_title': 'movie_title',
            'num_critic_for_reviews': 'num_critic_for_reviews',
            'duration': 'duration',
            'director_facebook_likes': 'DIRECTOR_facebook_likes',
            'actor_3_facebook_likes': 'actor_3_facebook_likes',
            'actor_1_facebook_likes': 'ACTOR_1_facebook_likes',
            'gross': 'gross',
            'num_voted_users': 'num_voted_users',
            'cast_total_facebook_likes': 'Cast_Total_facebook_likes',
            'facenumber_in_poster': 'facenumber_in_poster',
            'num_user_for_reviews': 'num_user_for_reviews',
            'budget': 'budget',
            'title_year': 'title_year',
            'actor_2_facebook_likes': 'ACTOR_2_facebook_likes',
            'imdb_score': 'imdb_score'
        }
        
        # Select only the columns we need
        available_columns = [col for col in columns_mapping.keys() if col in data.columns]
        data_transformed = data[available_columns].copy()
        
        # Rename columns
        data_transformed = data_transformed.rename(columns=columns_mapping)
        
        print(f"Transformed data shape: {data_transformed.shape}")
        print(f"Transformed columns: {list(data_transformed.columns)}")
        
        # Save transformed data
        output_file = 'd:/Data-Engineering-Buildables-Fellowship/Task-5/movies_transformed.csv'
        data_transformed.to_csv(output_file, index=False)
        
        print(f"Transformed data saved to {output_file}")
        return output_file
        
    except Exception as e:
        print(f"Error in data transformation: {e}")
        raise

def load_to_postgresql(**context):
    """Load transformed data into PostgreSQL"""
    try:
        # Read transformed data
        data = pd.read_csv('d:/Data-Engineering-Buildables-Fellowship/Task-5/movies_transformed.csv')
        
        # Handle duplicate columns if any
        if data.columns.duplicated().any():
            data = data.loc[:, ~data.columns.duplicated()]
        
        print(f"Loading {len(data)} records to PostgreSQL")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="etl_demo",
            user="postgres",
            password="SpeakO13."
        )
        cur = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS movies (
            id SERIAL PRIMARY KEY,
            movie_title VARCHAR(255),
            num_critic_for_reviews INTEGER,
            duration INTEGER,
            DIRECTOR_facebook_likes INTEGER,
            actor_3_facebook_likes INTEGER,
            ACTOR_1_facebook_likes INTEGER,
            gross BIGINT,
            num_voted_users INTEGER,
            Cast_Total_facebook_likes INTEGER,
            facenumber_in_poster INTEGER,
            num_user_for_reviews INTEGER,
            budget BIGINT,
            title_year INTEGER,
            ACTOR_2_facebook_likes INTEGER,
            imdb_score DECIMAL(3,1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_table_query)
        
        # Clear existing data (optional - remove if you want to append)
        cur.execute("TRUNCATE TABLE movies RESTART IDENTITY;")
        
        # Insert data with available columns
        insert_count = 0
        for _, row in data.iterrows():
            try:
                cur.execute("""
                    INSERT INTO movies (
                        movie_title, num_critic_for_reviews, duration, DIRECTOR_facebook_likes,
                        actor_3_facebook_likes, ACTOR_1_facebook_likes, gross, num_voted_users,
                        Cast_Total_facebook_likes, facenumber_in_poster, num_user_for_reviews,
                        budget, title_year, ACTOR_2_facebook_likes, imdb_score
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["movie_title"], 
                    row.get("num_critic_for_reviews", 0), 
                    row.get("duration", 0), 
                    0,  # DIRECTOR_facebook_likes not in transformed data
                    row.get("actor_3_facebook_likes", 0), 
                    0,  # ACTOR_1_facebook_likes not in transformed data
                    row.get("gross", 0), 
                    row.get("num_voted_users", 0),
                    0,  # Cast_Total_facebook_likes not in transformed data
                    row.get("facenumber_in_poster", 0), 
                    row.get("num_user_for_reviews", 0), 
                    row.get("budget", 0), 
                    row.get("title_year", 0), 
                    0,  # ACTOR_2_facebook_likes not in transformed data
                    row.get("imdb_score", 0)
                ))
                insert_count += 1
            except Exception as e:
                print(f"Error inserting row: {e}")
                continue
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Successfully loaded {insert_count} records into PostgreSQL")
        
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_from_minio',
    python_callable=extract_from_minio,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task