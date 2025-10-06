from minio import Minio
import pandas as pd
from io import BytesIO

# ---------------- Extract ----------------
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

response = client.get_object("movies", "Movies.csv")  # bucket & filename

# Fix: specify encoding='ISO-8859-1' to handle special characters
data = pd.read_csv(BytesIO(response.read()), encoding='ISO-8859-1')
response.close()
response.release_conn()

print("Extracted data (first row):")
print(data.head(1))

# ---------------- Transform ----------------
numeric_cols = ["num_critic_for_reviews","duration","DIRECTOR_facebook_likes",
                "actor_3_facebook_likes","ACTOR_1_facebook_likes","gross",
                "num_voted_users","Cast_Total_facebook_likes","facenumber_in_poster",
                "num_user_for_reviews","budget","title_year","ACTOR_2_facebook_likes","imdb_score"]

for col in numeric_cols:
    if col in data.columns:
        data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)

# Remove duplicate column if exists
if data.columns.duplicated().any():
    data = data.loc[:, ~data.columns.duplicated()]

print("\nTransformed data (first row):")
print(data.head(1))

# Optional: Save transformed CSV locally for Load step
data.to_csv("movies_transformed.csv", index=False)
print("\nTransformed CSV saved as movies_transformed.csv")
