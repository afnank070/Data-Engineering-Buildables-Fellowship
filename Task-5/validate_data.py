import psycopg2

# Connect and validate data
conn = psycopg2.connect(
    host='localhost', 
    port=5433, 
    database='etl_demo', 
    user='postgres', 
    password='SpeakO13.'
)
cur = conn.cursor()

# Get total count
cur.execute('SELECT COUNT(*) FROM movies;')
total = cur.fetchone()[0]
print(f'Total records in database: {total}')

# Get top movies by IMDB score
cur.execute('SELECT movie_title, imdb_score FROM movies ORDER BY imdb_score DESC LIMIT 3;')
print('\nTop 3 movies by IMDB score:')
for movie, score in cur.fetchall():
    clean_title = movie.replace('?ÿ', '').strip()
    print(f'  {clean_title}: {score}')

cur.close()
conn.close()
print('\nETL Pipeline validation completed successfully!')