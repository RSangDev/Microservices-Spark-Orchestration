"""
conftest.py — Configura o ambiente Airflow antes de qualquer import.
"""

import os
import sys
import tempfile

# ── AIRFLOW_HOME isolado ───────────────────────────────────────────────────────
_AIRFLOW_HOME = tempfile.mkdtemp(prefix="airflow_test_")
os.environ["AIRFLOW_HOME"] = _AIRFLOW_HOME

# ── SQLite com 4 barras (Windows exige /C:/... → sqlite:////C:/...) ───────────
_db = os.path.join(_AIRFLOW_HOME, "airflow.db").replace("\\", "/")
if not _db.startswith("/"):
    _db = "/" + _db
_conn = f"sqlite:///{_db}"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = _conn
os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = _conn

# ── Configurações mínimas ──────────────────────────────────────────────────────
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] = "False"
os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
os.environ["AIRFLOW__CORE__FERNET_KEY"] = "81HqDtbfh0EU_kU5bqNLUGYKmqatMavLzENBVFjf2Jo="

# ── PYTHONPATH: dags/ e plugins/ acessíveis nos testes ────────────────────────
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in [os.path.join(_root, "dags"), os.path.join(_root, "plugins"), _root]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
