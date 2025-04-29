import psycopg2

try:
    print("Attempting to connect to database...")
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Datapull",
        user="postgres", 
        password="taha"
    )
    print("Connection successful!")
    with conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
        table_count = cursor.fetchone()[0]
        print(f"Number of tables found: {table_count}")
    conn.close()
except Exception as e:
    print(f"Error connecting to database: {e}")