from fastapi import APIRouter, Query
from typing import Optional, Literal
from datetime import datetime, timedelta
import pandas as pd

from services.data_loader import load_data

router = APIRouter(prefix="/api/home", tags=["home"])

def _default_range() -> tuple[datetime, datetime]:
    fim = datetime.now()
    inicio = fim - timedelta(days=30)
    return inicio, fim

def _parse_date(d: Optional[str], default: datetime) -> datetime:
    if not d:
        return default
    return datetime.fromisoformat(d)

def _normalize_range(inicio: datetime, fim: datetime) -> tuple[datetime, datetime]:
    # garante que o fim inclua o dia inteiro quando vier sem hora (00:00:00)
    end = fim
    if end.hour == 0 and end.minute == 0 and end.second == 0 and end.microsecond == 0:
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    return inicio, end

def _filter_period(df: pd.DataFrame, inicio: datetime, fim: datetime) -> pd.DataFrame:
    if 'order_datetime' not in df.columns:
        return df.iloc[0:0]
    mask = (df['order_datetime'] >= inicio) & (df['order_datetime'] <= fim)
    return df.loc[mask]

def _month_range_for(date_ref: datetime) -> tuple[datetime, datetime]:
    start = date_ref.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # next month start
    if start.month == 12:
        next_start = start.replace(year=start.year + 1, month=1)
    else:
        next_start = start.replace(month=start.month + 1)
    end = next_start - timedelta(microseconds=1)
    return start, end

@router.get("/kpis")
def get_kpis(inicio: Optional[str] = None, fim: Optional[str] = None):
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)

    atual = _filter_period(df, d_inicio, d_fim)

    # mês anterior com base no fim
    ref_prev = (d_fim.replace(day=1) - timedelta(days=1))
    prev_inicio, prev_fim = _month_range_for(ref_prev)
    anterior = _filter_period(df, prev_inicio, prev_fim)

    def safe_mean(series: pd.Series) -> float:
        return float(series.mean()) if len(series) else 0.0

    receita_total = float(atual['total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
    pedidos_total = int(len(atual))
    ticket_medio = float(atual['total_brl'].mean()) if 'total_brl' in atual.columns and len(atual) else 0.0
    satisfacao_media = safe_mean(atual['satisfacao_nivel']) if 'satisfacao_nivel' in atual.columns else 0.0
    taxa_alta = float((atual['satisfacao_nivel'] >= 4).mean()) if 'satisfacao_nivel' in atual.columns and len(atual) else 0.0

    receita_prev = float(anterior['total_brl'].sum()) if 'total_brl' in anterior.columns else 0.0
    pedidos_prev = int(len(anterior))
    ticket_prev = float(anterior['total_brl'].mean()) if 'total_brl' in anterior.columns and len(anterior) else 0.0

    def variacao_pct(atual_v: float, prev_v: float) -> float:
        if prev_v == 0:
            return 0.0
        return float(((atual_v - prev_v) / prev_v) * 100.0)

    return {
        "receita_total": receita_total,
        "receita_variacao_pct": variacao_pct(receita_total, receita_prev),
        "pedidos_totais": pedidos_total,
        "pedidos_variacao_pct": variacao_pct(pedidos_total, pedidos_prev),
        "ticket_medio": ticket_medio,
        "ticket_medio_variacao_pct": variacao_pct(ticket_medio, ticket_prev),
        "satisfacao_media": satisfacao_media,
        "satisfacao_taxa_alta": taxa_alta,
        "periodo": {
            "inicio": d_inicio.date().isoformat(),
            "fim": d_fim.date().isoformat()
        }
    }

@router.get("/date-bounds")
def date_bounds():
    df = load_data()
    if 'order_datetime' not in df.columns or len(df) == 0:
        return {"min": None, "max": None}
    min_dt = pd.to_datetime(df['order_datetime']).min()
    max_dt = pd.to_datetime(df['order_datetime']).max()
    return {"min": str(pd.to_datetime(min_dt).date()), "max": str(pd.to_datetime(max_dt).date())}

@router.get("/receita-tempo")
def receita_tempo(
    granularidade: Literal['dia','semana','mes'] = 'dia',
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    cur = _filter_period(df, d_inicio, d_fim)
    if len(cur) == 0:
        return {"granularidade": granularidade, "dados": []}

    cur = cur.sort_values('order_datetime')
    cur = cur.set_index('order_datetime')

    if granularidade == 'dia':
        grp = cur.resample('D')
    elif granularidade == 'semana':
        grp = cur.resample('W')
    else:
        grp = cur.resample('MS')

    total = grp['total_brl'].sum() if 'total_brl' in cur.columns else grp.size()
    pedidos = grp.size()

    dados = []
    for idx, valor in total.items():
        receita = float(valor) if isinstance(valor, (int, float)) else float(valor)
        dados.append({
            "periodo": idx.date().isoformat(),
            "receita": receita,
            "pedidos": int(pedidos.loc[idx]) if idx in pedidos.index else 0
        })

    return {"granularidade": granularidade, "dados": dados}

@router.get("/plataformas")
def plataformas(
    inicio: Optional[str] = None,
    fim: Optional[str] = None,
    metric: Literal['pedidos','receita'] = 'pedidos'
):
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    cur = _filter_period(df, d_inicio, d_fim)
    if len(cur) == 0 or 'platform' not in cur.columns:
        return {"metric": metric, "plataformas": []}

    if metric == 'receita' and 'total_brl' in cur.columns:
        agg = cur.groupby('platform')['total_brl'].sum().sort_values(ascending=False)
        total_val = float(agg.sum())
        out = [
            {"nome": k, "receita": float(v), "pedidos": int(cur[cur['platform']==k].shape[0]), "pct": (float(v)/total_val if total_val else 0.0)}
            for k, v in agg.items()
        ]
    else:
        agg = cur.groupby('platform').size().sort_values(ascending=False)
        total_cnt = int(agg.sum())
        out = [
            {"nome": k, "pedidos": int(v), "receita": float(cur[cur['platform']==k]['total_brl'].sum()) if 'total_brl' in cur.columns else 0.0, "pct": (int(v)/total_cnt if total_cnt else 0.0)}
            for k, v in agg.items()
        ]
    return {"metric": metric, "plataformas": out}

@router.get("/resumo-mensal")
def resumo_mensal(mes: Optional[str] = None):
    df = load_data()
    if mes:
        # mes esperado: YYYY-MM
        ref = datetime.fromisoformat(mes + "-01")
    else:
        ref = datetime.now()
    start, end = _month_range_for(ref)
    cur = _filter_period(df, start, end)
    if len(cur) == 0:
        return {
            "melhor_dia_semana": None,
            "horario_pico": None,
            "plataforma_mais_usada": None,
            "bairro_top_receita": None
        }

    # Melhor dia da semana
    cur['dow'] = cur['order_datetime'].dt.day_name(locale='pt_BR') if hasattr(cur['order_datetime'].dt, 'day_name') else cur['order_datetime'].dt.dayofweek
    by_dow = cur.groupby('dow').size().sort_values(ascending=False)
    melhor_dia = by_dow.index[0] if len(by_dow) else None

    # Horário de pico (hora cheia)
    cur['hora'] = cur['order_datetime'].dt.hour
    by_hour = cur.groupby('hora').size().sort_values(ascending=False)
    if len(by_hour):
        h = int(by_hour.index[0])
        horario_pico = f"{h:02d}:00-{(h+1)%24:02d}:00"
    else:
        horario_pico = None

    # Plataforma mais usada
    if 'platform' in cur.columns:
        plat = cur.groupby('platform').size().sort_values(ascending=False)
        plataforma_mais_usada = str(plat.index[0]) if len(plat) else None
    else:
        plataforma_mais_usada = None

    bairro_top_pedidos = None
    bairro_top_receita = None
    if 'bairro_destino' in cur.columns:
        # por pedidos
        b_cnt = cur.groupby('bairro_destino').size().sort_values(ascending=False)
        bairro_top_pedidos = str(b_cnt.index[0]) if len(b_cnt) else None
        # por receita (opcional)
        if 'total_brl' in cur.columns:
            b_val = cur.groupby('bairro_destino')['total_brl'].sum().sort_values(ascending=False)
            bairro_top_receita = str(b_val.index[0]) if len(b_val) else None

    return {
        "melhor_dia_semana": melhor_dia,
        "horario_pico": horario_pico,
        "plataforma_mais_usada": plataforma_mais_usada,
        "bairro_top_pedidos": bairro_top_pedidos,
        "bairro_top_receita": bairro_top_receita,
        "mes": start.strftime('%Y-%m')
    }

@router.get("/resumo-periodo")
def resumo_periodo(inicio: Optional[str] = None, fim: Optional[str] = None):
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    cur = _filter_period(df, d_inicio, d_fim)
    if len(cur) == 0:
        return {
            "melhor_dia_semana": None,
            "horario_pico": None,
            "plataforma_mais_usada": None,
            "bairro_top_pedidos": None,
            "bairro_top_receita": None,
            "inicio": d_inicio.date().isoformat(),
            "fim": d_fim.date().isoformat()
        }

    # Melhor dia da semana (por pedidos)
    cur = cur.copy()
    cur['dow'] = cur['order_datetime'].dt.day_name(locale='pt_BR') if hasattr(cur['order_datetime'].dt, 'day_name') else cur['order_datetime'].dt.dayofweek
    by_dow = cur.groupby('dow').size().sort_values(ascending=False)
    melhor_dia = by_dow.index[0] if len(by_dow) else None

    # Horário de pico (hora cheia)
    cur['hora'] = cur['order_datetime'].dt.hour
    by_hour = cur.groupby('hora').size().sort_values(ascending=False)
    if len(by_hour):
        h = int(by_hour.index[0])
        horario_pico = f"{h:02d}:00-{(h+1)%24:02d}:00"
    else:
        horario_pico = None

    # Plataforma mais usada
    if 'platform' in cur.columns:
        plat = cur.groupby('platform').size().sort_values(ascending=False)
        plataforma_mais_usada = str(plat.index[0]) if len(plat) else None
    else:
        plataforma_mais_usada = None

    # Bairros topo
    bairro_top_pedidos = None
    bairro_top_receita = None
    if 'bairro_destino' in cur.columns:
        b_cnt = cur.groupby('bairro_destino').size().sort_values(ascending=False)
        bairro_top_pedidos = str(b_cnt.index[0]) if len(b_cnt) else None
        if 'total_brl' in cur.columns:
            b_val = cur.groupby('bairro_destino')['total_brl'].sum().sort_values(ascending=False)
            bairro_top_receita = str(b_val.index[0]) if len(b_val) else None

    return {
        "melhor_dia_semana": melhor_dia,
        "horario_pico": horario_pico,
        "plataforma_mais_usada": plataforma_mais_usada,
        "bairro_top_pedidos": bairro_top_pedidos,
        "bairro_top_receita": bairro_top_receita,
        "inicio": d_inicio.date().isoformat(),
        "fim": d_fim.date().isoformat()
    }


