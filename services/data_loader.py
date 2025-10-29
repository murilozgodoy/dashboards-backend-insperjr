import os
import pandas as pd
from datetime import datetime
from typing import Optional

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DATA_DIR = os.path.abspath(DATA_DIR)
CSV_FILENAME = 'Base_Kaiserhaus_Limpa.csv'

_cached_df: Optional[pd.DataFrame] = None
_cached_mtime: Optional[float] = None

def _get_csv_path() -> str:
    return os.path.join(DATA_DIR, CSV_FILENAME)

def load_data(force: bool = False) -> pd.DataFrame:
    global _cached_df, _cached_mtime
    csv_path = _get_csv_path()
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo de dados não encontrado em: {csv_path}")

    mtime = os.path.getmtime(csv_path)
    if force or _cached_df is None or _cached_mtime != mtime:
        df = pd.read_csv(csv_path)
        # Normalizações mínimas
        if 'order_datetime' in df.columns:
            df['order_datetime'] = pd.to_datetime(df['order_datetime'])
        _cached_df = df
        _cached_mtime = mtime
    return _cached_df.copy()


