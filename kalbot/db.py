from contextlib import contextmanager
from typing import Generator

from psycopg import Connection, connect
from psycopg.rows import dict_row

from kalbot.settings import get_settings


@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    settings = get_settings()
    conn = connect(settings.database_url, row_factory=dict_row, connect_timeout=5)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
