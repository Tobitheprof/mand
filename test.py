import psycopg2

conn = psycopg2.connect(
    "postgresql://postgres:3EcNO32CT9dB@db.nuodavvfvkvenxbjeyzn.supabase.co:5432/postgres"
)
print("Connection successful!")

conn.close()
