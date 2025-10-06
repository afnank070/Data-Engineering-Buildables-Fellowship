from minio import Minio
import pandas as pd
from io import BytesIO

# Connect to MinIO
client = Minio(
    "localhost:9000",        
    access_key="minioadmin", 
    secret_key="minioadmin", 
    secure=False             
)

# Download CSV from bucket
response = client.get_object("movies", "Movies.csv")  

# Use correct encoding
data = pd.read_csv(BytesIO(response.read()), encoding='ISO-8859-1')
response.close()
response.release_conn()

# Show the first row
print(data.head(1))
