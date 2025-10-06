import pandas as pd
import psycopg2

# ---------------- Read Transformed CSV ----------------
data = pd.read_csv("movies_transformed.csv")  # from Transform step

# Handle duplicate columns automatically
if data.columns.duplicated().any():
    data = data.loc[:, ~data.columns.duplicated()]

print("Data to load (first row):")
print(data.head(1))

# ---------------- Connect to PostgreSQL ----------------
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="etl_demo",      # your database
    user="postgres",          # your username
    password="SpeakO13."       # your password
)
cur = conn.cursor()

# ---------------- Insert Data ----------------
for _, row in data.iterrows():
    cur.execute("""
        INSERT INTO movies (
            movie_title, num_critic_for_reviews, duration, DIRECTOR_facebook_likes,
            actor_3_facebook_likes, ACTOR_1_facebook_likes, gross, num_voted_users,
            Cast_Total_facebook_likes, facenumber_in_poster, num_user_for_reviews,
            budget, title_year, ACTOR_2_facebook_likes, imdb_score
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row["movie_title"], row["num_critic_for_reviews"], row["duration"], row["DIRECTOR_facebook_likes"],
        row["actor_3_facebook_likes"], row["ACTOR_1_facebook_likes"], row["gross"], row["num_voted_users"],
        row["Cast_Total_facebook_likes"], row["facenumber_in_poster"], row["num_user_for_reviews"],
        row["budget"], row["title_year"], row["ACTOR_2_facebook_likes"], row["imdb_score"]
    ))

conn.commit()
cur.close()
conn.close()

print("\nData loaded into PostgreSQL successfully!")
