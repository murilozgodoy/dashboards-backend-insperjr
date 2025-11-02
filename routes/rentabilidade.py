from fastapi import APIRouter, Query
from typing import Optional, Literal
from datetime import datetime, timedelta
import pandas as pd

from services.data_loader import load_data

router = APIRouter(prefix="/api/rentabilidade", tags=["rentabilidade"])

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

def _is_canal_proprio(platform: str) -> bool:
    """Identifica se a plataforma é um canal próprio (sem comissão)"""
    if pd.isna(platform):
        return False
    platform_lower = str(platform).lower()
    return 'site' in platform_lower or 'whatsapp' in platform_lower or 'proprio' in platform_lower

@router.get("/kpis")
def get_kpis(inicio: Optional[str] = None, fim: Optional[str] = None):
    """KPIs Financeiros: Receita Bruta, Comissões, Receita Líquida, Margens"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    if len(atual) == 0:
        return {
            "receita_bruta_total": 0.0,
            "comissoes_totais": 0.0,
            "receita_liquida": 0.0,
            "margem_liquida_pct": 0.0,
            "receita_liquida_vs_bruta_pct": 0.0
        }
    
    # Receita bruta total
    receita_bruta = float(atual['total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
    
    # Calcular comissões
    comissoes = 0.0
    if 'platform_commission_pct' in atual.columns and 'total_brl' in atual.columns:
        mask_comissao = atual['platform_commission_pct'].notna()
        if mask_comissao.any():
            comissoes = float((atual.loc[mask_comissao, 'total_brl'] * atual.loc[mask_comissao, 'platform_commission_pct']).sum())
    
    receita_liquida = receita_bruta - comissoes
    
    # Margens
    margem_liquida_pct = (receita_liquida / receita_bruta * 100) if receita_bruta > 0 else 0.0
    receita_liquida_vs_bruta_pct = ((receita_liquida - receita_bruta) / receita_bruta * 100) if receita_bruta > 0 else 0.0
    
    return {
        "receita_bruta_total": receita_bruta,
        "comissoes_totais": comissoes,
        "receita_liquida": receita_liquida,
        "margem_liquida_pct": margem_liquida_pct,
        "receita_liquida_vs_bruta_pct": receita_liquida_vs_bruta_pct
    }

@router.get("/waterfall")
def get_waterfall(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Gráfico de cascata: Receita bruta -> Comissões iFood -> Comissões Rappi -> Receita líquida"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    receita_bruta = float(atual['total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
    
    comissao_ifood = 0.0
    comissao_rappi = 0.0
    
    if 'platform' in atual.columns and 'platform_commission_pct' in atual.columns and 'total_brl' in atual.columns:
        # iFood
        mask_ifood = (atual['platform'].str.lower().str.contains('ifood', na=False)) & atual['platform_commission_pct'].notna()
        if mask_ifood.any():
            comissao_ifood = float((atual.loc[mask_ifood, 'total_brl'] * atual.loc[mask_ifood, 'platform_commission_pct']).sum())
        
        # Rappi
        mask_rappi = (atual['platform'].str.lower().str.contains('rappi', na=False)) & atual['platform_commission_pct'].notna()
        if mask_rappi.any():
            comissao_rappi = float((atual.loc[mask_rappi, 'total_brl'] * atual.loc[mask_rappi, 'platform_commission_pct']).sum())
    
    receita_liquida = receita_bruta - comissao_ifood - comissao_rappi
    
    return {
        "receita_bruta": receita_bruta,
        "menos_comissao_ifood": -comissao_ifood,
        "menos_comissao_rappi": -comissao_rappi,
        "receita_liquida_final": receita_liquida
    }

@router.get("/margens-por-plataforma")
def get_margens_por_plataforma(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Margem % por plataforma"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    if len(atual) == 0 or 'platform' not in atual.columns:
        return {"plataformas": []}
    
    plataformas = []
    
    for platform in atual['platform'].unique():
        if pd.isna(platform):
            continue
        
        mask_plat = atual['platform'] == platform
        receita_bruta = float(atual.loc[mask_plat, 'total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
        
        # Calcular comissão
        comissao = 0.0
        if 'platform_commission_pct' in atual.columns:
            mask_comissao = mask_plat & atual['platform_commission_pct'].notna()
            if mask_comissao.any():
                comissao = float((atual.loc[mask_comissao, 'total_brl'] * atual.loc[mask_comissao, 'platform_commission_pct']).sum())
        
        receita_liquida = receita_bruta - comissao
        
        # Margem % (100% para canais próprios)
        if _is_canal_proprio(str(platform)):
            margem_pct = 100.0
        else:
            margem_pct = (receita_liquida / receita_bruta * 100) if receita_bruta > 0 else 0.0
        
        plataformas.append({
            "plataforma": str(platform),
            "receita_bruta": receita_bruta,
            "comissao_pct": (comissao / receita_bruta * 100) if receita_bruta > 0 else 0.0,
            "comissao_brl": comissao,
            "receita_liquida": receita_liquida,
            "margem_pct": margem_pct
        })
    
    return {"plataformas": sorted(plataformas, key=lambda x: x['receita_bruta'], reverse=True)}

@router.get("/canais-vs-marketplace")
def get_canais_vs_marketplace(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Comparação detalhada: Canais Próprios vs Marketplaces"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    if len(atual) == 0 or 'platform' not in atual.columns:
        return {
            "marketplaces": {
                "receita_bruta": 0.0,
                "comissao_pct": 0.0,
                "comissao_brl": 0.0,
                "receita_liquida": 0.0,
                "margem_pct": 0.0
            },
            "canais_proprios": {
                "receita_bruta": 0.0,
                "comissao_brl": 0.0,
                "receita_liquida": 0.0,
                "margem_pct": 100.0
            }
        }
    
    # Separar marketplaces e canais próprios
    mask_marketplace = atual['platform'].apply(lambda x: not _is_canal_proprio(str(x)) if pd.notna(x) else False)
    mask_proprio = atual['platform'].apply(lambda x: _is_canal_proprio(str(x)) if pd.notna(x) else False)
    
    # Marketplaces
    receita_bruta_mkt = float(atual.loc[mask_marketplace, 'total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
    comissao_mkt = 0.0
    if 'platform_commission_pct' in atual.columns:
        mask_comissao_mkt = mask_marketplace & atual['platform_commission_pct'].notna()
        if mask_comissao_mkt.any():
            comissao_mkt = float((atual.loc[mask_comissao_mkt, 'total_brl'] * atual.loc[mask_comissao_mkt, 'platform_commission_pct']).sum())
    receita_liquida_mkt = receita_bruta_mkt - comissao_mkt
    margem_mkt = (receita_liquida_mkt / receita_bruta_mkt * 100) if receita_bruta_mkt > 0 else 0.0
    
    # Canais próprios
    receita_bruta_prop = float(atual.loc[mask_proprio, 'total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
    receita_liquida_prop = receita_bruta_prop  # Sem comissão
    
    return {
        "marketplaces": {
            "receita_bruta": receita_bruta_mkt,
            "comissao_pct": (comissao_mkt / receita_bruta_mkt * 100) if receita_bruta_mkt > 0 else 0.0,
            "comissao_brl": comissao_mkt,
            "receita_liquida": receita_liquida_mkt,
            "margem_pct": margem_mkt
        },
        "canais_proprios": {
            "receita_bruta": receita_bruta_prop,
            "comissao_brl": 0.0,
            "receita_liquida": receita_bruta_prop,
            "margem_pct": 100.0
        }
    }

@router.get("/simulacao")
def get_simulacao(
    pct_canal_proprio: float = Query(..., description="Percentual de pedidos que seriam via canal próprio (0-100)"),
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Simulação: Impacto de mudar X% dos pedidos para canal próprio"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    if len(atual) == 0:
        return {
            "economia_comissoes": 0.0,
            "aumento_receita_liquida_pct": 0.0,
            "receita_bruta_atual": 0.0,
            "comissoes_atual": 0.0,
            "receita_liquida_atual": 0.0,
            "receita_bruta_simulada": 0.0,
            "comissoes_simulada": 0.0,
            "receita_liquida_simulada": 0.0
        }
    
    # Situação atual
    receita_bruta_atual = float(atual['total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
    comissoes_atual = 0.0
    if 'platform_commission_pct' in atual.columns:
        mask_comissao = atual['platform_commission_pct'].notna()
        if mask_comissao.any():
            comissoes_atual = float((atual.loc[mask_comissao, 'total_brl'] * atual.loc[mask_comissao, 'platform_commission_pct']).sum())
    receita_liquida_atual = receita_bruta_atual - comissoes_atual
    
    # Simulação: pct_canal_proprio dos pedidos marketplace viram próprios
    pct_canal_proprio = min(100.0, max(0.0, pct_canal_proprio))
    pct_convertido = pct_canal_proprio / 100.0
    
    # Calcular receita marketplace atual
    mask_marketplace = atual['platform'].apply(lambda x: not _is_canal_proprio(str(x)) if pd.notna(x) else False)
    receita_marketplace_atual = float(atual.loc[mask_marketplace, 'total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
    comissao_marketplace_atual = 0.0
    if 'platform_commission_pct' in atual.columns:
        mask_comissao_mkt = mask_marketplace & atual['platform_commission_pct'].notna()
        if mask_comissao_mkt.any():
            comissao_marketplace_atual = float((atual.loc[mask_comissao_mkt, 'total_brl'] * atual.loc[mask_comissao_mkt, 'platform_commission_pct']).sum())
    
    # Receita que seria convertida
    receita_convertida = receita_marketplace_atual * pct_convertido
    comissao_evitada = comissao_marketplace_atual * pct_convertido
    
    # Nova situação
    receita_bruta_simulada = receita_bruta_atual  # Mesma receita bruta
    comissoes_simulada = comissoes_atual - comissao_evitada
    receita_liquida_simulada = receita_bruta_simulada - comissoes_simulada
    
    economia_comissoes = comissao_evitada
    aumento_receita_liquida_pct = ((receita_liquida_simulada - receita_liquida_atual) / receita_liquida_atual * 100) if receita_liquida_atual > 0 else 0.0
    
    return {
        "economia_comissoes": economia_comissoes,
        "aumento_receita_liquida_pct": aumento_receita_liquida_pct,
        "receita_bruta_atual": receita_bruta_atual,
        "comissoes_atual": comissoes_atual,
        "receita_liquida_atual": receita_liquida_atual,
        "receita_bruta_simulada": receita_bruta_simulada,
        "comissoes_simulada": comissoes_simulada,
        "receita_liquida_simulada": receita_liquida_simulada
    }

@router.get("/rentabilidade-por-tipo")
def get_rentabilidade_por_tipo(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Rentabilidade por tipo de pedido (Família, Combo, Prato único)"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    if len(atual) == 0 or 'classe_pedido' not in atual.columns:
        return {"tipos": []}
    
    tipos = []
    
    for classe in atual['classe_pedido'].unique():
        if pd.isna(classe):
            continue
        
        mask_classe = atual['classe_pedido'] == classe
        receita_bruta = float(atual.loc[mask_classe, 'total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
        ticket_medio = float(atual.loc[mask_classe, 'total_brl'].mean()) if 'total_brl' in atual.columns else 0.0
        
        # Calcular comissão média
        comissao_total = 0.0
        if 'platform_commission_pct' in atual.columns:
            mask_comissao = mask_classe & atual['platform_commission_pct'].notna()
            if mask_comissao.any():
                comissao_total = float((atual.loc[mask_comissao, 'total_brl'] * atual.loc[mask_comissao, 'platform_commission_pct']).sum())
        
        receita_liquida = receita_bruta - comissao_total
        margem_pct = (receita_liquida / receita_bruta * 100) if receita_bruta > 0 else 0.0
        
        tipos.append({
            "tipo": str(classe),
            "ticket_medio": ticket_medio,
            "receita_bruta": receita_bruta,
            "comissao_brl": comissao_total,
            "receita_liquida": receita_liquida,
            "margem_pct": margem_pct
        })
    
    return {"tipos": sorted(tipos, key=lambda x: x['receita_bruta'], reverse=True)}

@router.get("/evolucao-temporal")
def get_evolucao_temporal(
    granularidade: Literal['dia', 'semana', 'mes'] = 'mes',
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Evolução da rentabilidade ao longo do tempo"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    if len(atual) == 0:
        return {"granularidade": granularidade, "dados": []}
    
    atual = atual.sort_values('order_datetime')
    atual = atual.set_index('order_datetime')
    
    if granularidade == 'dia':
        grp = atual.resample('D')
    elif granularidade == 'semana':
        grp = atual.resample('W')
    else:
        grp = atual.resample('MS')
    
    dados = []
    for idx, group_df in grp:
        receita_bruta = float(group_df['total_brl'].sum()) if 'total_brl' in group_df.columns else 0.0
        
        comissoes = 0.0
        if 'platform_commission_pct' in group_df.columns:
            mask_comissao = group_df['platform_commission_pct'].notna()
            if mask_comissao.any():
                comissoes = float((group_df.loc[mask_comissao, 'total_brl'] * group_df.loc[mask_comissao, 'platform_commission_pct']).sum())
        
        receita_liquida = receita_bruta - comissoes
        margem_pct = (receita_liquida / receita_bruta * 100) if receita_bruta > 0 else 0.0
        
        dados.append({
            "periodo": idx.date().isoformat(),
            "receita_bruta": receita_bruta,
            "comissoes": comissoes,
            "receita_liquida": receita_liquida,
            "margem_pct": margem_pct
        })
    
    return {"granularidade": granularidade, "dados": dados}

@router.get("/roi-por-plataforma")
def get_roi_por_plataforma(inicio: Optional[str] = None, fim: Optional[str] = None):
    """ROI por plataforma: Investimento (comissão) vs Retorno (receita líquida)"""
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    atual = _filter_period(df, d_inicio, d_fim)
    
    if len(atual) == 0 or 'platform' not in atual.columns:
        return {"plataformas": []}
    
    plataformas = []
    
    for platform in atual['platform'].unique():
        if pd.isna(platform):
            continue
        
        mask_plat = atual['platform'] == platform
        receita_bruta = float(atual.loc[mask_plat, 'total_brl'].sum()) if 'total_brl' in atual.columns else 0.0
        
        # Investimento (comissão)
        investimento = 0.0
        if 'platform_commission_pct' in atual.columns:
            mask_comissao = mask_plat & atual['platform_commission_pct'].notna()
            if mask_comissao.any():
                investimento = float((atual.loc[mask_comissao, 'total_brl'] * atual.loc[mask_comissao, 'platform_commission_pct']).sum())
        
        # Retorno (receita líquida)
        retorno = receita_bruta - investimento
        
        # ROI %
        roi_pct = ((retorno - investimento) / investimento * 100) if investimento > 0 else (float('inf') if retorno > 0 else 0.0)
        
        # Payback (meses - estimativa baseada no período)
        dias_periodo = (d_fim - d_inicio).days + 1
        meses_periodo = dias_periodo / 30.0 if dias_periodo > 0 else 1.0
        payback_meses = (investimento / (retorno / meses_periodo)) if (retorno / meses_periodo) > 0 else 0.0
        
        plataformas.append({
            "plataforma": str(platform),
            "investimento": investimento,
            "retorno": retorno,
            "roi_pct": roi_pct if roi_pct != float('inf') else 999.99,
            "payback_meses": payback_meses
        })
    
    # Ordenar por ROI (maior primeiro)
    plataformas_sorted = sorted(plataformas, key=lambda x: x['roi_pct'], reverse=True)
    
    return {"plataformas": plataformas_sorted}

