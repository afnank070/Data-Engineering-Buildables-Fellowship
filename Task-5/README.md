# ETL Pipeline with Apache Airflow

## 🎯 Project Overview
This project demonstrates a complete ETL pipeline using:
- **MinIO** (Landing Zone) - Object storage for raw data
- **PostgreSQL** (Data Warehouse) - Structured data storage  
- **Apache Airflow** (Orchestration) - ETL workflow automation

## 📁 Project Structure
```
Task-5/
├── airflow_new/              # Airflow virtual environment
├── airflow/                  # Airflow home directory
│   ├── dags/
│   │   └── etl_pipeline_dag.py  # Main ETL DAG
│   ├── airflow.cfg           # Airflow configuration
│   └── airflow.db            # SQLite database
├── Movies.csv                # Source data
├── movies_transformed.csv    # Transformed data
├── run_etl_dag.py           # Manual ETL runner
├── start_airflow.bat        # Startup script
└── README.md                # This file
```

## 🚀 Quick Start

### 1. Start Airflow Environment
```bash
# Double-click or run:
start_airflow.bat
```

### 2. Run ETL Pipeline Manually
```bash
python run_etl_dag.py
```

### 3. Start Airflow Webserver (Optional)
```bash
airflow webserver --port 8080
```
Then visit: http://localhost:8080
- Username: `admin`
- Password: `admin123`

## 📊 ETL Pipeline Details

### Extract Phase
- **Source**: CSV file (simulating MinIO extraction)
- **Data**: Movie dataset with 14 records
- **Output**: Raw CSV data

### Transform Phase
- **Cleaning**: Remove missing critical data
- **Validation**: Ensure IMDB scores are valid (0-10)
- **Standardization**: Clean movie titles, fill missing values
- **Output**: Cleaned CSV with 11 columns

### Load Phase
- **Target**: PostgreSQL database (`etl_demo`)
- **Table**: `movies` with 17 columns
- **Result**: 14 records successfully loaded

## 🗄️ Database Schema
```sql
CREATE TABLE movies (
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
```

## ✅ Validation Query
```sql
SELECT COUNT(*) FROM movies;
-- Result: 14 records

SELECT movie_title, imdb_score FROM movies LIMIT 5;
-- Sample results:
-- Avatar: 7.9
-- Pirates of the Caribbean: At World's End: 7.1
-- Spectre: 6.8
-- The Dark Knight Rises: 8.5
-- John Carter: 6.6
```

## 🛠️ Technical Stack
- **Python 3.10.11**
- **Apache Airflow 2.7.3**
- **PostgreSQL** (port 5433)
- **pandas** for data transformation
- **psycopg2** for database connectivity
- **boto3** for MinIO/S3 integration

## 🎉 Success Metrics
- ✅ Airflow successfully installed and configured
- ✅ ETL DAG created with 3 tasks (Extract → Transform → Load)
- ✅ Data pipeline runs successfully
- ✅ 14 movie records processed and loaded
- ✅ Database validation confirms successful load
- ✅ Admin user created for Airflow UI access

## 🔧 Troubleshooting
- If Airflow webserver fails to start, use the manual runner: `python run_etl_dag.py`
- Ensure PostgreSQL is running on port 5433
- Check database credentials in the DAG file
- For encoding issues, the pipeline handles multiple encodings automatically

## 📝 Next Steps
1. Set up MinIO server for true object storage
2. Add data quality checks and monitoring
3. Implement incremental data loading
4. Add email notifications for pipeline failures
5. Scale with distributed executors