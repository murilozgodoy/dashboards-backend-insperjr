from fastapi import APIRouter, Query
from typing import Optional, Literal
from datetime import datetime, timedelta
import pandas as pd

from services.data_loader import load_data

router = APIRouter(prefix="/api/operacional", tags=["operacional"])

def _default_range() -> tuple[datetime, datetime]:
    fim = datetime.now()
    inicio = fim - timedelta(days=30)
    return inicio, fim

def _parse_date(d: Optional[str], default: datetime) -> datetime:
    if not d:
        return default
    return datetime.fromisoformat(d)

def _normalize_range(inicio: datetime, fim: datetime) -> tuple[datetime, datetime]:
    end = fim
    if end.hour == 0 and end.minute == 0 and end.second == 0 and end.microsecond == 0:
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    return inicio, end

def _filter_period(df: pd.DataFrame, inicio: datetime, fim: datetime) -> pd.DataFrame:
    if 'order_datetime' not in df.columns:
        return df.iloc[0:0]
    mask = (df['order_datetime'] >= inicio) & (df['order_datetime'] <= fim)
    return df.loc[mask]

@router.get("/kpis")
def get_kpis(
    inicio: Optional[str] = None, 
    fim: Optional[str] = None,
    threshold_minutos: int = Query(10, description="Limite de minutos para considerar atraso")
):
    """Retorna KPIs operacionais principais"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    if len(atual) == 0:
        return {
            "tempo_preparo_medio": 0.0,
            "tempo_entrega_medio": 0.0,
            "precisao_eta_pct": 0.0,
            "taxa_atraso_pct": 0.0,
            "eficiencia_media": 0.0,
            "desempenho_eta": 0.0
        }
    
    def safe_mean(series: pd.Series) -> float:
        if len(series) == 0:
            return 0.0
        mean_val = series.mean()
        return float(mean_val) if pd.notna(mean_val) else 0.0
    
    #Tempo médio de preparo
    if 'tempo_preparo_minutos' in atual.columns:
        tempo_preparo_medio = safe_mean(atual['tempo_preparo_minutos'])
    else:
        tempo_preparo_medio = 0.0
    
    #Tempo médio de entrega
    if 'actual_delivery_minutes' in atual.columns:
        tempo_entrega_medio = safe_mean(atual['actual_delivery_minutes'])
    else:
        tempo_entrega_medio = 0.0
    
    #Precisão do ETA (pedidos entregues dentro do prazo)
    if 'eta_minutes_quote' in atual.columns and 'actual_delivery_minutes' in atual.columns:
        valid_mask = atual['eta_minutes_quote'].notna() & atual['actual_delivery_minutes'].notna()
        if valid_mask.sum() > 0:
            dentro_prazo = (atual.loc[valid_mask, 'actual_delivery_minutes'] <= atual.loc[valid_mask, 'eta_minutes_quote']).sum()
            precisao_eta_pct = (dentro_prazo / valid_mask.sum()) * 100.0
        else:
            precisao_eta_pct = 0.0
    else:
        precisao_eta_pct = 0.0
    
    #Taxa de atraso (pedidos com atraso > threshold_minutos)
    if 'eta_minutes_quote' in atual.columns and 'actual_delivery_minutes' in atual.columns:
        valid_mask = atual['eta_minutes_quote'].notna() & atual['actual_delivery_minutes'].notna()
        if valid_mask.sum() > 0:
            atraso_minutos = atual.loc[valid_mask, 'actual_delivery_minutes'] - atual.loc[valid_mask, 'eta_minutes_quote']
            atrasos_threshold = (atraso_minutos > threshold_minutos).sum()
            taxa_atraso_pct = (atrasos_threshold / valid_mask.sum()) * 100.0
        else:
            taxa_atraso_pct = 0.0
    else:
        taxa_atraso_pct = 0.0
    
    #Eficiência média (tempo/distância)
    if 'actual_delivery_minutes' in atual.columns and 'distance_km' in atual.columns:
        valid_mask = atual['actual_delivery_minutes'].notna() & atual['distance_km'].notna() & (atual['actual_delivery_minutes'] > 0)
        if valid_mask.sum() > 0:
            eficiencia = atual.loc[valid_mask, 'distance_km'] / atual.loc[valid_mask, 'actual_delivery_minutes']
            eficiencia_media = safe_mean(eficiencia)
        else:
            eficiencia_media = 0.0
    else:
        eficiencia_media = 0.0
    
    #Desempenho relativo ao ETA (quanto % acima/abaixo do ETA)
    if 'eta_minutes_quote' in atual.columns and 'actual_delivery_minutes' in atual.columns:
        valid_mask = atual['eta_minutes_quote'].notna() & atual['actual_delivery_minutes'].notna() & (atual['eta_minutes_quote'] > 0)
        if valid_mask.sum() > 0:
            diff_pct = ((atual.loc[valid_mask, 'actual_delivery_minutes'] - atual.loc[valid_mask, 'eta_minutes_quote']) / atual.loc[valid_mask, 'eta_minutes_quote'] * 100)
            desempenho_eta = safe_mean(diff_pct)
        else:
            desempenho_eta = 0.0
    else:
        desempenho_eta = 0.0
    
    return {
        "tempo_preparo_medio": tempo_preparo_medio,
        "tempo_entrega_medio": tempo_entrega_medio,
        "precisao_eta_pct": precisao_eta_pct,
        "taxa_atraso_pct": taxa_atraso_pct,
        "eficiencia_media": eficiencia_media,
        "desempenho_eta": desempenho_eta
    }

@router.get("/tempo-preparo-tempo")
def tempo_preparo_tempo(
    granularidade: Literal['dia', 'semana', 'mes'] = 'dia',
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Tempo médio de preparo ao longo do tempo"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    if len(cur) == 0 or 'tempo_preparo_minutos' not in cur.columns:
        return {"granularidade": granularidade, "dados": []}
    
    cur = cur.sort_values('order_datetime')
    cur = cur.set_index('order_datetime')
    
    if granularidade == 'dia':
        grp = cur.resample('D')
    elif granularidade == 'semana':
        grp = cur.resample('W')
    else:
        grp = cur.resample('MS')
    
    tempo_medio = grp['tempo_preparo_minutos'].mean()
    
    dados = []
    for idx, valor in tempo_medio.items():
        dados.append({
            "periodo": idx.date().isoformat(),
            "tempo_medio": float(valor) if pd.notna(valor) else 0.0
        })
    
    return {"granularidade": granularidade, "dados": dados}

@router.get("/tempo-entrega-distancia")
def tempo_entrega_distancia(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Tempo de entrega por faixa de distância"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    if len(cur) == 0 or 'distance_km' not in cur.columns or 'actual_delivery_minutes' not in cur.columns:
        return {"dados": []}
    

    def faixa_distancia(km):
        if pd.isna(km):
            return "Sem distância"
        km_float = float(km)
        if km_float < 2:
            return "0-2 km"
        elif km_float < 5:
            return "2-5 km"
        elif km_float < 10:
            return "5-10 km"
        else:
            return "10+ km"
    
    cur['faixa'] = cur['distance_km'].apply(faixa_distancia)
    agg = cur.groupby('faixa')['actual_delivery_minutes'].agg(['mean', 'count']).reset_index()
    
    dados = []
    for _, row in agg.iterrows():
        dados.append({
            "faixa": str(row['faixa']),
            "tempo_medio": float(row['mean']) if pd.notna(row['mean']) else 0.0,
            "quantidade": int(row['count'])
        })
    
    return {"dados": dados}

@router.get("/eta-vs-real")
def eta_vs_real(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Comparação entre ETA estimado e tempo real"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    if len(cur) == 0 or 'eta_minutes_quote' not in cur.columns or 'actual_delivery_minutes' not in cur.columns:
        return {"dados": []}
    
    eta_medio = float(cur['eta_minutes_quote'].mean()) if pd.notna(cur['eta_minutes_quote'].mean()) else 0.0
    real_medio = float(cur['actual_delivery_minutes'].mean()) if pd.notna(cur['actual_delivery_minutes'].mean()) else 0.0
    
    return {
        "dados": [
            {"tipo": "ETA Estimado", "tempo": eta_medio},
            {"tipo": "Tempo Real", "tempo": real_medio}
        ]
    }

@router.get("/distribuicao-tempos")
def distribuicao_tempos(
    tipo: Literal['preparo', 'entrega'] = 'preparo',
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Distribuição de tempos de preparo ou entrega"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if tipo == 'preparo':
        col = 'tempo_preparo_minutos'
    else:
        col = 'actual_delivery_minutes'
    
    if len(cur) == 0 or col not in cur.columns:
        return {"faixas": []}


    def faixa_tempo(minutos):
        if pd.isna(minutos):
            return "Sem dados"
        min_float = float(minutos)
        if min_float < 10:
            return "0-10 min"
        elif min_float < 20:
            return "10-20 min"
        elif min_float < 30:
            return "20-30 min"
        elif min_float < 45:
            return "30-45 min"
        else:
            return "45+ min"
    
    cur['faixa'] = cur[col].apply(faixa_tempo)
    agg = cur.groupby('faixa').size().reset_index(name='quantidade')
    
    faixas = []
    for _, row in agg.iterrows():
        faixas.append({
            "faixa": str(row['faixa']),
            "quantidade": int(row['quantidade'])
        })
    
    return {"faixas": faixas}

@router.get("/atrasos")
def get_atrasos(
    threshold_minutos: int = Query(10, description="Limite de minutos para considerar atraso"),
    inicio: Optional[str] = None,
    fim: Optional[str] = None,
    limit: int = Query(50, description="Limite de registros retornados")
):
    """Lista pedidos com atraso acima do threshold"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'eta_minutes_quote' not in cur.columns or 'actual_delivery_minutes' not in cur.columns:
        return {"dados": []}
    
    #Calcular atraso
    cur = cur.copy()
    cur['atraso_minutos'] = cur['actual_delivery_minutes'] - cur['eta_minutes_quote']
    atrasos = cur[cur['atraso_minutos'] > threshold_minutos].copy()
    
    if len(atrasos) == 0:
        return {"dados": []}
    
    #Ordenar por maior atraso e limitar
    atrasos = atrasos.sort_values('atraso_minutos', ascending=False).head(limit)
    
    dados = []
    for _, row in atrasos.iterrows():
        dados.append({
            "data": row['order_datetime'].isoformat() if pd.notna(row['order_datetime']) else None,
            "nome_cliente": str(row.get('nome_cliente', '')) if pd.notna(row.get('nome_cliente')) else '',
            "eta_minutos": float(row['eta_minutes_quote']) if pd.notna(row['eta_minutes_quote']) else 0.0,
            "tempo_real_minutos": float(row['actual_delivery_minutes']) if pd.notna(row['actual_delivery_minutes']) else 0.0,
            "atraso_minutos": float(row['atraso_minutos']),
            "distancia_km": float(row.get('distance_km', 0)) if pd.notna(row.get('distance_km')) else 0.0,
            "platform": str(row.get('platform', '')) if pd.notna(row.get('platform')) else ''
        })
    
    return {"dados": dados}

@router.get("/precisao-eta-hora")
def precisao_eta_hora(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Precisão do ETA por hora do dia (heatmap)"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'eta_minutes_quote' not in cur.columns or 'actual_delivery_minutes' not in cur.columns:
        return {"dados": []}
    
    cur = cur.copy()
    cur['hora'] = cur['order_datetime'].dt.hour
    cur['dentro_prazo'] = (cur['actual_delivery_minutes'] <= cur['eta_minutes_quote']).astype(int)
    
    agg = cur.groupby('hora').agg({
        'dentro_prazo': ['sum', 'count']
    }).reset_index()
    agg.columns = ['hora', 'dentro_prazo', 'total']
    agg['precisao_pct'] = (agg['dentro_prazo'] / agg['total'] * 100).round(2)
    
    dados = []
    for _, row in agg.iterrows():
        dados.append({
            "hora": int(row['hora']),
            "precisao_pct": float(row['precisao_pct']),
            "total_pedidos": int(row['total'])
        })
    
    return {"dados": dados}

@router.get("/tempos-por-modo")
def tempos_por_modo(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Tempos médios por modo de pedido"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'order_mode' not in cur.columns:
        return {"dados": []}
    
    dados = []
    for modo in cur['order_mode'].unique():
        if pd.isna(modo):
            continue
        modo_df = cur[cur['order_mode'] == modo]
        
        preparo_medio = float(modo_df['tempo_preparo_minutos'].mean()) if 'tempo_preparo_minutos' in modo_df.columns else 0.0
        entrega_medio = float(modo_df['actual_delivery_minutes'].mean()) if 'actual_delivery_minutes' in modo_df.columns else 0.0
        
        dados.append({
            "modo": str(modo),
            "tempo_preparo_medio": preparo_medio,
            "tempo_entrega_medio": entrega_medio,
            "quantidade": int(len(modo_df))
        })
    
    return {"dados": dados}

