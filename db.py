import os
import psycopg2
import streamlit as st


def _load_conn_params() -> dict:
    """Load DB connection parameters.

    Priority:
    1) Streamlit secrets: [db] section
    2) Local module: db_local.DB_CONFIG (for dev; keep out of deploy)
    3) Environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    """
    # Prefer Streamlit Cloud/local secrets
    try:
        if "db" in st.secrets:  # type: ignore[attr-defined]
            s = st.secrets["db"]
            return {
                "host": s.get("host"),
                "port": int(s.get("port", 5432)),
                "dbname": s.get("dbname"),
                "user": s.get("user"),
                "password": s.get("password"),
            }
    except Exception:
        # st.secrets may not be available outside Streamlit runtime
        pass

    # Local development module fallback
    try:
        from db_local import DB_CONFIG as LOCAL_DB_CONFIG  # type: ignore
        return {
            "host": LOCAL_DB_CONFIG.get("host"),
            "port": int(LOCAL_DB_CONFIG.get("port", 5432)),
            "dbname": LOCAL_DB_CONFIG.get("dbname"),
            "user": LOCAL_DB_CONFIG.get("user"),
            "password": LOCAL_DB_CONFIG.get("password"),
        }
    except Exception:
        pass

    # Fallback to environment variables
    return {
        "host": os.environ.get("DB_HOST"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
    }


def get_connection():
    """Create and return a psycopg2 connection using configured params.

    Raises a RuntimeError if required parameters are missing.
    """
    params = _load_conn_params()
    missing = [k for k, v in params.items() if v in (None, "")]
    if missing:
        raise RuntimeError(
            "Missing DB configuration. Provide via st.secrets([db]) or env vars: "
            + ", ".join(missing)
        )
    return psycopg2.connect(**params)
