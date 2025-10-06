#!/usr/bin/env python3
"""
Manual ETL DAG Runner for Windows
This script runs the ETL pipeline without needing the full Airflow scheduler
"""

import sys
import os
sys.path.append('d:/Data-Engineering-Buildables-Fellowship/Task-5/airflow/dags')

from etl_pipeline_dag import extract_from_minio, transform_data, load_to_postgresql

def run_etl_pipeline():
    """Run the complete ETL pipeline manually"""
    print("Starting ETL Pipeline...")
    print("=" * 50)
    
    try:
        # Step 1: Extract
        print("\nStep 1: Extracting data from MinIO...")
        extract_result = extract_from_minio()
        print(f"Extract completed: {extract_result}")
        
        # Step 2: Transform
        print("\nStep 2: Transforming data...")
        transform_result = transform_data()
        print(f"Transform completed: {transform_result}")
        
        # Step 3: Load
        print("\nStep 3: Loading data to PostgreSQL...")
        load_to_postgresql()
        print("Load completed successfully!")
        
        print("\nETL Pipeline completed successfully!")
        print("=" * 50)
        
        # Validation query
        print("\nValidating data in PostgreSQL...")
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="etl_demo",
            user="postgres",
            password="SpeakO13."
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM movies;")
        count = cur.fetchone()[0]
        cur.execute("SELECT movie_title, imdb_score FROM movies LIMIT 5;")
        sample_data = cur.fetchall()
        cur.close()
        conn.close()
        
        print(f"Total records in database: {count}")
        print("Sample data:")
        for movie, score in sample_data:
            print(f"   - {movie}: {score}")
            
    except Exception as e:
        print(f"Error in ETL pipeline: {e}")
        raise

if __name__ == "__main__":
    run_etl_pipeline()