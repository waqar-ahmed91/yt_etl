from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = 'yt_api'

def get_conn_hook():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_cursor(conn, cur):
    cur.close()
    conn.close()


def create_schema(schema):

    conn, cur = get_conn_hook()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"

    cur.execute(schema_sql)

    conn.commit()

    close_cursor(conn, cur)

def create_table(schema):

    conn, cur = get_conn_hook()

    if schema == "staging":
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                        "Video_Title" TEXT NOT NULL,
                        "Upload_Date" TIMESTAMP NOT NULL,
                        "Duration" VARCHAR(20) NOT NULL,
                        "Video_Views" INT,
                        "Likes_Counts" INT,
                        "Comment_Counts" INT
                    );
                   """
    else:
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                        "Video_Title" TEXT NOT NULL,
                        "Upload_Date" TIMESTAMP NOT NULL,
                        "Duration" TIME NOT NULL,
                        "Video_Type" VARCHAR(10) NOT NULL,
                        "Video_Views" INT,
                        "Likes_Counts" INT,
                        "Comment_Counts" INT
                    );
                   """

    cur.execute(table_sql)

    conn.commit()

    close_cursor(conn, cur)

def get_video_ids(cur, schema):
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row["Video_ID"] for row in ids]

    return video_ids