from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg2://etl_db_ax00_user:uC5aUq9vOUrLxjnCrOWYlPJUIXjxBPoJ@dpg-d82oc183kofs73d1018g-a.singapore-postgres.render.com/etl_db_ax00"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"sslmode": "require"}
)
