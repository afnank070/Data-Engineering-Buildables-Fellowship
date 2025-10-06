#!/usr/bin/env python3
"""
Simple Web Interface for ETL Pipeline Status
Since Airflow webserver has Windows issues, this provides a basic web UI
"""

from flask import Flask, render_template_string, jsonify
import psycopg2
import os
from datetime import datetime

app = Flask(__name__)

# HTML template for the web interface
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>ETL Pipeline Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { text-align: center; color: #2c3e50; margin-bottom: 30px; }
        .status-card { background: #ecf0f1; padding: 20px; margin: 15px 0; border-radius: 8px; border-left: 5px solid #3498db; }
        .success { border-left-color: #27ae60; background: #d5f4e6; }
        .info { border-left-color: #3498db; background: #d6eaf8; }
        .warning { border-left-color: #f39c12; background: #fdeaa7; }
        .btn { background: #3498db; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; margin: 5px; text-decoration: none; display: inline-block; }
        .btn:hover { background: #2980b9; }
        .data-table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        .data-table th, .data-table td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        .data-table th { background-color: #3498db; color: white; }
        .data-table tr:nth-child(even) { background-color: #f2f2f2; }
        .refresh-btn { float: right; }
    </style>
    <script>
        function refreshData() {
            location.reload();
        }
        
        function runETL() {
            fetch('/run-etl', {method: 'POST'})
                .then(response => response.json())
                .then(data => {
                    alert(data.message);
                    location.reload();
                });
        }
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>ETL Pipeline Dashboard</h1>
            <p>Apache Airflow ETL Pipeline Status</p>
            <button class="btn refresh-btn" onclick="refreshData()">Refresh</button>
        </div>
        
        <div class="status-card success">
            <h3>Pipeline Status: OPERATIONAL</h3>
            <p><strong>Last Updated:</strong> {{ current_time }}</p>
            <p><strong>DAG:</strong> etl_pipeline (Extract → Transform → Load)</p>
        </div>
        
        <div class="status-card info">
            <h3>Database Status</h3>
            <p><strong>Total Records:</strong> {{ total_records }}</p>
            <p><strong>Database:</strong> PostgreSQL (etl_demo)</p>
            <p><strong>Table:</strong> movies</p>
        </div>
        
        <div class="status-card warning">
            <h3>Actions</h3>
            <button class="btn" onclick="runETL()">Run ETL Pipeline</button>
            <a href="/data" class="btn">View Data</a>
            <a href="/logs" class="btn">View Logs</a>
        </div>
        
        {% if sample_data %}
        <div class="status-card">
            <h3>Sample Movie Data</h3>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Movie Title</th>
                        <th>IMDB Score</th>
                        <th>Year</th>
                        <th>Budget</th>
                    </tr>
                </thead>
                <tbody>
                    {% for movie in sample_data %}
                    <tr>
                        <td>{{ movie[0] }}</td>
                        <td>{{ movie[1] }}</td>
                        <td>{{ movie[2] }}</td>
                        <td>${{ "{:,}".format(movie[3]) if movie[3] else 'N/A' }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}
        
        <div class="status-card">
            <h3>ETL Pipeline Components</h3>
            <ul>
                <li><strong>Extract:</strong> CSV file processing (simulating MinIO)</li>
                <li><strong>Transform:</strong> Data cleaning and validation with pandas</li>
                <li><strong>Load:</strong> Insert into PostgreSQL database</li>
            </ul>
        </div>
    </div>
</body>
</html>
"""

def get_database_stats():
    """Get database statistics"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="etl_demo",
            user="postgres",
            password="SpeakO13."
        )
        cur = conn.cursor()
        
        # Get total count
        cur.execute("SELECT COUNT(*) FROM movies;")
        total = cur.fetchone()[0]
        
        # Get sample data
        cur.execute("""
            SELECT movie_title, imdb_score, title_year, budget 
            FROM movies 
            ORDER BY imdb_score DESC 
            LIMIT 5;
        """)
        sample_data = cur.fetchall()
        
        # Clean movie titles
        cleaned_sample = []
        for row in sample_data:
            clean_title = row[0].replace('?ÿ', '').strip() if row[0] else 'Unknown'
            cleaned_sample.append((clean_title, row[1], row[2], row[3]))
        
        cur.close()
        conn.close()
        
        return total, cleaned_sample
        
    except Exception as e:
        print(f"Database error: {e}")
        return 0, []

@app.route('/')
def dashboard():
    """Main dashboard"""
    total_records, sample_data = get_database_stats()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return render_template_string(HTML_TEMPLATE, 
                                current_time=current_time,
                                total_records=total_records,
                                sample_data=sample_data)

@app.route('/data')
def view_data():
    """View all data"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="etl_demo",
            user="postgres",
            password="SpeakO13."
        )
        cur = conn.cursor()
        cur.execute("SELECT movie_title, imdb_score, title_year, budget FROM movies ORDER BY imdb_score DESC;")
        all_data = cur.fetchall()
        cur.close()
        conn.close()
        
        # Clean data
        cleaned_data = []
        for row in all_data:
            clean_title = row[0].replace('?ÿ', '').strip() if row[0] else 'Unknown'
            cleaned_data.append((clean_title, row[1], row[2], row[3]))
        
        return jsonify({"data": cleaned_data, "count": len(cleaned_data)})
        
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/run-etl', methods=['POST'])
def run_etl():
    """Run ETL pipeline"""
    try:
        import subprocess
        result = subprocess.run(['python', 'run_etl_dag.py'], 
                              capture_output=True, text=True, cwd='d:/Data-Engineering-Buildables-Fellowship/Task-5')
        
        if result.returncode == 0:
            return jsonify({"message": "ETL Pipeline executed successfully!"})
        else:
            return jsonify({"message": f"ETL Pipeline failed: {result.stderr}"})
            
    except Exception as e:
        return jsonify({"message": f"Error running ETL: {str(e)}"})

@app.route('/logs')
def view_logs():
    """View logs"""
    return jsonify({"message": "Logs feature coming soon!"})

if __name__ == '__main__':
    print("Starting ETL Pipeline Web Interface...")
    print("Dashboard will be available at: http://localhost:5000")
    print("Make sure PostgreSQL is running on port 5433")
    app.run(host='0.0.0.0', port=5000, debug=True)