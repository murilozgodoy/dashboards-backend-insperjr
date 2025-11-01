from fastapi import APIRouter, Query
from typing import Optional, Literal
from datetime import datetime, timedelta
import pandas as pd

from services.data_loader import load_data

router = APIRouter(prefix="/api/operacional", tags=["operacional"])

def safe_mean(series: pd.Series) -> float:
    """Calcula média de forma segura, retornando 0.0 se vazio ou NaN"""
    if len(series) == 0:
        return 0.0
    mean_val = series.mean()
    return float(mean_val) if pd.notna(mean_val) else 0.0

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
    
    #Taxa de atraso
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

@router.get("/analise-por-periodo")
def analise_por_periodo(
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
        return {"dados": []}
    
    def classificar_periodo(hora: int) -> str:
        if hora >= 0 and hora < 6:
            return "Madrugada"
        elif hora >= 6 and hora < 12:
            return "Manhã"
        elif hora >= 12 and hora < 18:
            return "Tarde"
        else: 
            return "Noite"
    
    #extrair hora do datetime
    if 'order_datetime' in cur.columns:
        cur['hora'] = pd.to_datetime(cur['order_datetime']).dt.hour
        cur['periodo'] = cur['hora'].apply(classificar_periodo)
    elif 'hora' in cur.columns:
        cur['periodo'] = cur['hora'].apply(classificar_periodo)
    else:
        return {"dados": []}
    
    periodos = ['Madrugada', 'Manhã', 'Tarde', 'Noite']
    dados = []
    
    for periodo in periodos:
        periodo_df = cur[cur['periodo'] == periodo]
        
        if len(periodo_df) == 0:
            dados.append({
                "periodo": periodo,
                "quantidade": 0,
                "tempo_preparo_medio": 0.0,
                "tempo_entrega_medio": 0.0,
                "taxa_atraso_pct": 0.0,
                "precisao_eta_pct": 0.0
            })
            continue
        
       
        quantidade = len(periodo_df)
        
    
        preparo_medio = safe_mean(periodo_df['tempo_preparo_minutos']) if 'tempo_preparo_minutos' in periodo_df.columns else 0.0
        
        #Tempo de entrega (apenas para entregas)
        if 'actual_delivery_minutes' in periodo_df.columns:
            entrega_df = periodo_df[periodo_df['actual_delivery_minutes'].notna() & (periodo_df['actual_delivery_minutes'] > 0)]
            entrega_medio = safe_mean(entrega_df['actual_delivery_minutes']) if len(entrega_df) > 0 else 0.0
        else:
            entrega_medio = 0.0
        
        #Taxa de atraso (ETA vs Real)
        taxa_atraso = 0.0
        if 'eta_minutes_quote' in periodo_df.columns and 'actual_delivery_minutes' in periodo_df.columns:
            valid_mask = periodo_df['eta_minutes_quote'].notna() & periodo_df['actual_delivery_minutes'].notna()
            if valid_mask.sum() > 0:
                atrasos = (periodo_df.loc[valid_mask, 'actual_delivery_minutes'] > periodo_df.loc[valid_mask, 'eta_minutes_quote']).sum()
                taxa_atraso = (atrasos / valid_mask.sum()) * 100
        
        # Precisão do ETA (diferença <= 5 min)
        precisao_eta = 0.0
        if 'eta_minutes_quote' in periodo_df.columns and 'actual_delivery_minutes' in periodo_df.columns:
            valid_mask = periodo_df['eta_minutes_quote'].notna() & periodo_df['actual_delivery_minutes'].notna()
            if valid_mask.sum() > 0:
                diff = abs(periodo_df.loc[valid_mask, 'actual_delivery_minutes'] - periodo_df.loc[valid_mask, 'eta_minutes_quote'])
                precisos = (diff <= 5).sum()
                precisao_eta = (precisos / valid_mask.sum()) * 100
        
        dados.append({
            "periodo": periodo,
            "quantidade": quantidade,
            "tempo_preparo_medio": preparo_medio,
            "tempo_entrega_medio": entrega_medio,
            "taxa_atraso_pct": taxa_atraso,
            "precisao_eta_pct": precisao_eta
        })
    
    return {"dados": dados}

@router.get("/eta-vs-real-scatter")
def eta_vs_real_scatter(
    inicio: Optional[str] = None,
    fim: Optional[str] = None,
    limit: int = Query(1000, description="Limite de pontos no scatter (amostra)")
):
  
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'eta_minutes_quote' not in cur.columns or 'actual_delivery_minutes' not in cur.columns:
        return {"pontos": []}
    
    valid_mask = cur['eta_minutes_quote'].notna() & cur['actual_delivery_minutes'].notna()
    valid = cur.loc[valid_mask, ['eta_minutes_quote', 'actual_delivery_minutes']].copy()
    
    if len(valid) == 0:
        return {"pontos": []}
    
    
    if len(valid) > limit:
        valid = valid.sample(n=limit, random_state=42)
    
    pontos = []
    for _, row in valid.iterrows():
        pontos.append({
            "eta": float(row['eta_minutes_quote']),
            "real": float(row['actual_delivery_minutes'])
        })
    
    return {"pontos": pontos}

@router.get("/tempos-por-hora")
def tempos_por_hora(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'order_datetime' not in cur.columns:
        return {"dados": []}
    
    cur = cur.copy()
    cur['hora'] = cur['order_datetime'].dt.hour
    
    dados = []
    for hora in range(24):
        hora_df = cur[cur['hora'] == hora]
        if len(hora_df) == 0:
            continue
        
        preparo_medio = float(hora_df['tempo_preparo_minutos'].mean()) if 'tempo_preparo_minutos' in hora_df.columns and hora_df['tempo_preparo_minutos'].notna().any() else 0.0
        entrega_medio = float(hora_df['actual_delivery_minutes'].mean()) if 'actual_delivery_minutes' in hora_df.columns and hora_df['actual_delivery_minutes'].notna().any() else 0.0
        
        dados.append({
            "hora": hora,
            "tempo_preparo_medio": preparo_medio,
            "tempo_entrega_medio": entrega_medio,
            "quantidade": int(len(hora_df))
        })
    
    return {"dados": dados}

@router.get("/estatisticas-tempos")
def estatisticas_tempos(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):

    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    def calc_stats(series: pd.Series) -> dict:
        if len(series) == 0 or series.notna().sum() == 0:
            return {"min": 0.0, "max": 0.0, "media": 0.0, "p50": 0.0, "p75": 0.0, "p95": 0.0}
        valid = series.dropna()
        return {
            "min": float(valid.min()),
            "max": float(valid.max()),
            "media": float(valid.mean()),
            "p50": float(valid.quantile(0.5)),
            "p75": float(valid.quantile(0.75)),
            "p95": float(valid.quantile(0.95))
        }
    
    preparo_stats = calc_stats(cur['tempo_preparo_minutos']) if 'tempo_preparo_minutos' in cur.columns else {"min": 0.0, "max": 0.0, "media": 0.0, "p50": 0.0, "p75": 0.0, "p95": 0.0}
    entrega_stats = calc_stats(cur['actual_delivery_minutes']) if 'actual_delivery_minutes' in cur.columns else {"min": 0.0, "max": 0.0, "media": 0.0, "p50": 0.0, "p75": 0.0, "p95": 0.0}
    
    return {
        "preparo": preparo_stats,
        "entrega": entrega_stats
    }

@router.get("/outliers-detalhados")
def outliers_detalhados(
    inicio: Optional[str] = None,
    fim: Optional[str] = None,
    preparo_min: int = Query(30, description="Limite de minutos para considerar outlier de preparo"),
    entrega_min: int = Query(60, description="Limite de minutos para considerar outlier de entrega"),
    limit: int = Query(50, description="Limite de registros retornados")
):
  
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0:
        return {"outliers_preparo": [], "outliers_entrega": [], "resumo": {}}
    
    # Outliers de preparo
    outliers_preparo = []
    if 'tempo_preparo_minutos' in cur.columns:
        prep_out = cur[cur['tempo_preparo_minutos'] > preparo_min].copy()
        prep_out = prep_out.sort_values('tempo_preparo_minutos', ascending=False).head(limit)
        
        for _, row in prep_out.iterrows():
            outliers_preparo.append({
                "data": row['order_datetime'].isoformat() if pd.notna(row['order_datetime']) else None,
                "tempo_preparo": float(row['tempo_preparo_minutos']),
                "tempo_entrega": float(row.get('actual_delivery_minutes', 0)) if pd.notna(row.get('actual_delivery_minutes')) else 0.0,
                "distancia_km": float(row.get('distance_km', 0)) if pd.notna(row.get('distance_km')) else 0.0,
                "hora": int(row['order_datetime'].hour) if pd.notna(row['order_datetime']) else None,
                "bairro": str(row.get('bairro_destino', '')) if pd.notna(row.get('bairro_destino')) else '',
                "platform": str(row.get('platform', '')) if pd.notna(row.get('platform')) else '',
                "modo": str(row.get('order_mode', '')) if pd.notna(row.get('order_mode')) else ''
            })
    
  
    outliers_entrega = []
    if 'actual_delivery_minutes' in cur.columns:
        entrega_out = cur[cur['actual_delivery_minutes'] > entrega_min].copy()
        entrega_out = entrega_out.sort_values('actual_delivery_minutes', ascending=False).head(limit)
        
        for _, row in entrega_out.iterrows():
            outliers_entrega.append({
                "data": row['order_datetime'].isoformat() if pd.notna(row['order_datetime']) else None,
                "tempo_preparo": float(row.get('tempo_preparo_minutos', 0)) if pd.notna(row.get('tempo_preparo_minutos')) else 0.0,
                "tempo_entrega": float(row['actual_delivery_minutes']),
                "distancia_km": float(row.get('distance_km', 0)) if pd.notna(row.get('distance_km')) else 0.0,
                "hora": int(row['order_datetime'].hour) if pd.notna(row['order_datetime']) else None,
                "bairro": str(row.get('bairro_destino', '')) if pd.notna(row.get('bairro_destino')) else '',
                "platform": str(row.get('platform', '')) if pd.notna(row.get('platform')) else '',
                "modo": str(row.get('order_mode', '')) if pd.notna(row.get('order_mode')) else ''
            })
    
 
    total_pedidos = len(cur)
    prep_out_count = len(cur[cur['tempo_preparo_minutos'] > preparo_min]) if 'tempo_preparo_minutos' in cur.columns else 0
    entrega_out_count = len(cur[cur['actual_delivery_minutes'] > entrega_min]) if 'actual_delivery_minutes' in cur.columns else 0
    
    return {
        "outliers_preparo": outliers_preparo,
        "outliers_entrega": outliers_entrega,
        "resumo": {
            "total_pedidos": total_pedidos,
            "outliers_preparo_count": prep_out_count,
            "outliers_entrega_count": entrega_out_count,
            "pct_preparo": (prep_out_count / total_pedidos * 100) if total_pedidos > 0 else 0.0,
            "pct_entrega": (entrega_out_count / total_pedidos * 100) if total_pedidos > 0 else 0.0
        }
    }

