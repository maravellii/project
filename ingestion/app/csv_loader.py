import pandas as pd
import psycopg2
from io import StringIO
from .config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT


def load_csv():

    print("Loading CSV...")

    df = pd.read_csv("/app/data/data.csv", encoding="latin1")

    df = df[df["Quantity"] > 0]
    df["CustomerID"] = df["CustomerID"].fillna(0).astype(int)

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB
    )

    cursor = conn.cursor()

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor.copy_expert(
        """
        COPY raw.online_retail
        FROM STDIN WITH CSV
        """,
        buffer
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} rows")