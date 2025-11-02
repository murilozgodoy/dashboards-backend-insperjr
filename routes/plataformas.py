from fastapi import APIRouter, Query
from typing import Optional, Literal
from datetime import datetime, timedelta
import pandas as pd

from services.data_loader import load_data

router = APIRouter(prefix="/api/plataformas", tags=["plataformas"])

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

def safe_mean(series: pd.Series) -> float:
    
    if len(series) == 0:
        return 0.0
    mean_val = series.mean()
    return float(mean_val) if pd.notna(mean_val) else 0.0

@router.get("/receita-tempo")
def get_receita_tempo(
    granularidade: Literal['dia', 'semana', 'mes'] = 'dia',
    metric: Literal['receita', 'pedidos'] = 'receita',
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Evolução temporal de receita ou pedidos por plataforma individual"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'platform' not in cur.columns:
        return {"granularidade": granularidade, "metric": metric, "dados": []}
    
    cur = cur.sort_values('order_datetime')
    cur = cur.set_index('order_datetime')
    
    if granularidade == 'dia':
        grp = cur.resample('D')
    elif granularidade == 'semana':
        grp = cur.resample('W')
    else:
        grp = cur.resample('MS')
    
    dados = []
    
    for idx, group_df in grp:
        periodo_dict = {"periodo": idx.date().isoformat()}
        
        if 'platform' in group_df.columns:
            for platform in group_df['platform'].unique():
                if pd.isna(platform):
                    continue
                
                mask_plat = group_df['platform'] == platform
                plat_df = group_df.loc[mask_plat]
                
                if metric == 'receita' and 'total_brl' in plat_df.columns:
                    valor = float(plat_df['total_brl'].sum())
                else:
                    valor = int(len(plat_df))
                
                
                platform_key = str(platform).lower().replace(' ', '_').replace('-', '_')
                periodo_dict[platform_key] = valor
        
        dados.append(periodo_dict)
    
    return {"granularidade": granularidade, "metric": metric, "dados": dados}

@router.get("/tempos-medios")
def get_tempos_medios(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'platform' not in cur.columns:
        return {"plataformas": []}
    
    plataformas = []
    
    for platform in cur['platform'].unique():
        if pd.isna(platform):
            continue
        
        mask_plat = cur['platform'] == platform
        plat_df = cur.loc[mask_plat]
        
        tempo_preparo = safe_mean(plat_df['tempo_preparo_minutos']) if 'tempo_preparo_minutos' in plat_df.columns else 0.0
        tempo_entrega = safe_mean(plat_df['actual_delivery_minutes']) if 'actual_delivery_minutes' in plat_df.columns else 0.0
        eta_medio = safe_mean(plat_df['eta_minutes_quote']) if 'eta_minutes_quote' in plat_df.columns else 0.0
        
        plataformas.append({
            "plataforma": str(platform),
            "tempo_preparo_medio": round(tempo_preparo, 2),
            "tempo_entrega_medio": round(tempo_entrega, 2),
            "eta_medio": round(eta_medio, 2)
        })
    
    return {"plataformas": sorted(plataformas, key=lambda x: x['tempo_entrega_medio'], reverse=True)}

@router.get("/satisfacao")
def get_satisfacao(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Satisfação média e distribuição por plataforma"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'platform' not in cur.columns or 'satisfacao_nivel' not in cur.columns:
        return {"plataformas": []}
    
    plataformas = []
    
    for platform in cur['platform'].unique():
        if pd.isna(platform):
            continue
        
        mask_plat = cur['platform'] == platform
        satisfacoes = cur.loc[mask_plat, 'satisfacao_nivel'].dropna()
        
        if len(satisfacoes) == 0:
            continue
        
        satisfacao_media = safe_mean(satisfacoes)
        
    
        distribuicao = {}
        for nivel in range(1, 6):
            count = int((satisfacoes == nivel).sum())
            distribuicao[f"nivel_{nivel}"] = count
        
        plataformas.append({
            "plataforma": str(platform),
            "satisfacao_media": round(satisfacao_media, 2),
            "total_avaliacoes": int(len(satisfacoes)),
            "distribuicao": distribuicao
        })
    
    return {"plataformas": sorted(plataformas, key=lambda x: x['satisfacao_media'], reverse=True)}

@router.get("/volume-hora")
def get_volume_hora(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Volume de pedidos por hora do dia (0-23) por plataforma"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'platform' not in cur.columns:
        # Retornar todas as 24 horas com valores zerados
        dados = []
        for hora in range(24):
            dados.append({"hora": hora, "ifood": 0, "rappi": 0, "site_proprio": 0, "whatsapp": 0})
        return {"dados": dados}
    
    cur = cur.copy()
    cur['hora'] = cur['order_datetime'].dt.hour
    
   
    dados_dict = {}
    for hora in range(24):
        dados_dict[hora] = {}
    
    
    for hora in range(24):
        mask_hora = cur['hora'] == hora
        hora_df = cur.loc[mask_hora]
        
        if len(hora_df) == 0:
            for platform in ['ifood', 'rappi', 'site_proprio', 'whatsapp']:
                dados_dict[hora][platform] = 0
            continue
        
        for platform in hora_df['platform'].unique():
            if pd.isna(platform):
                continue
            platform_key = str(platform).lower().replace(' ', '_')
            mask_plat = hora_df['platform'] == platform
            count = int(mask_plat.sum())
            dados_dict[hora][platform_key] = count
        
        
        for platform in ['ifood', 'rappi', 'site_proprio', 'whatsapp']:
            if platform not in dados_dict[hora]:
                dados_dict[hora][platform] = 0
    
    dados = []
    for hora in range(24):
        dados.append({
            "hora": hora,
            **dados_dict[hora]
        })
    
    return {"dados": dados}

@router.get("/volume-dia-semana")
def get_volume_dia_semana(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Volume de pedidos por dia da semana por plataforma"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'platform' not in cur.columns:
        return {"dados": []}
    
    cur = cur.copy()
    cur['dia_semana'] = cur['order_datetime'].dt.day_name()
    
    dias_pt = {
        'Monday': 'Segunda',
        'Tuesday': 'Terça',
        'Wednesday': 'Quarta',
        'Thursday': 'Quinta',
        'Friday': 'Sexta',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    
  
    dados_dict = {}
    ordem_dias = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    for dia in ordem_dias:
        dados_dict[dia] = {}
        mask_dia = cur['dia_semana'] == dia
        dia_df = cur.loc[mask_dia]
        
        if len(dia_df) == 0:
            for platform in ['ifood', 'rappi', 'site_proprio', 'whatsapp']:
                dados_dict[dia][platform] = 0
            continue
        
        for platform in dia_df['platform'].unique():
            if pd.isna(platform):
                continue
            platform_key = str(platform).lower().replace(' ', '_')
            mask_plat = dia_df['platform'] == platform
            count = int(mask_plat.sum())
            dados_dict[dia][platform_key] = count
        
        
        for platform in ['ifood', 'rappi', 'site_proprio', 'whatsapp']:
            if platform not in dados_dict[dia]:
                dados_dict[dia][platform] = 0
    
    #converter para lista
    dados = []
    for dia in ordem_dias:
        dados.append({
            "dia": dias_pt[dia],
            "dia_ordem": ordem_dias.index(dia),
            **dados_dict[dia]
        })
    
    return {"dados": dados}

@router.get("/modos-pedido")
def get_modos_pedido(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Quantidade de pedidos por modo (delivery/retirada) por plataforma"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'platform' not in cur.columns or 'order_mode' not in cur.columns:
        return {"plataformas": []}
    
    plataformas = []
    
    for platform in cur['platform'].unique():
        if pd.isna(platform):
            continue
        
        mask_plat = cur['platform'] == platform
        plat_df = cur.loc[mask_plat]
        
        delivery = int((plat_df['order_mode'].str.lower() == 'delivery').sum()) if 'order_mode' in plat_df.columns else 0
        retirada = int((plat_df['order_mode'].str.lower() == 'retirada').sum()) if 'order_mode' in plat_df.columns else 0
        
        plataformas.append({
            "plataforma": str(platform),
            "delivery": delivery,
            "retirada": retirada,
            "total": delivery + retirada
        })
    
    return {"plataformas": sorted(plataformas, key=lambda x: x['total'], reverse=True)}

