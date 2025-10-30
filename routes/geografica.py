from fastapi import APIRouter
from typing import Optional
from datetime import datetime, timedelta
import pandas as pd

from services.data_loader import load_data

router = APIRouter(prefix="/api/geografica", tags=["geografica"])

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

def _get_min_pedidos(inicio: datetime, fim: datetime) -> int:
    """Retorna o mínimo de pedidos necessário baseado no tamanho do período"""
    dias = (fim - inicio).days + 1
    if dias <= 1:
        return 1  # Para 1 dia, aceita qualquer bairro com pedido
    elif dias <= 7:
        return 2  # Para até 1 semana, mínimo de 2 pedidos
    else:
        return 3  # Para períodos maiores, mínimo de 3 pedidos

@router.get("/volume-por-bairro")
def volume_por_bairro(inicio: Optional[str] = None, fim: Optional[str] = None, top_n: int = 20):
    """
    Retorna o volume de pedidos por bairro, ordenado do maior para o menor
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'bairro_destino' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    volume = cur.groupby('bairro_destino').size().sort_values(ascending=False).head(top_n)
    
    dados = [
        {"bairro": str(bairro), "volume": int(qtd)}
        for bairro, qtd in volume.items()
    ]
    
    return {"dados": dados}

@router.get("/receita-por-bairro")
def receita_por_bairro(inicio: Optional[str] = None, fim: Optional[str] = None, top_n: int = 20):
    """
    Retorna a receita por bairro, ordenada da maior para a menor
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'bairro_destino' not in cur.columns or 'total_brl' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    receita = cur.groupby('bairro_destino')['total_brl'].sum().sort_values(ascending=False).head(top_n)
    
    dados = [
        {"bairro": str(bairro), "receita": float(valor)}
        for bairro, valor in receita.items()
    ]
    
    return {"dados": dados}

@router.get("/ticket-medio-por-bairro")
def ticket_medio_por_bairro(inicio: Optional[str] = None, fim: Optional[str] = None, top_n: int = 20):
    """
    Retorna o ticket médio por bairro, ordenado do maior para o menor
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'bairro_destino' not in cur.columns or 'total_brl' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    # Filtrar bairros com mínimo de pedidos (dinâmico baseado no período)
    min_pedidos = _get_min_pedidos(d_inicio, d_fim)
    bairros_relevantes = cur.groupby('bairro_destino').size()
    bairros_relevantes = bairros_relevantes[bairros_relevantes >= min_pedidos].index
    
    cur_filtrado = cur[cur['bairro_destino'].isin(bairros_relevantes)]
    
    if len(cur_filtrado) == 0:
        return {"dados": []}
    
    ticket_medio = cur_filtrado.groupby('bairro_destino')['total_brl'].mean().sort_values(ascending=False).head(top_n)
    
    dados = [
        {"bairro": str(bairro), "ticket_medio": float(valor)}
        for bairro, valor in ticket_medio.items()
    ]
    
    return {"dados": dados}

@router.get("/satisfacao-por-bairro")
def satisfacao_por_bairro(inicio: Optional[str] = None, fim: Optional[str] = None, top_n: int = 20):
    """
    Retorna a satisfação média por bairro, ordenada da maior para a menor
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'bairro_destino' not in cur.columns or 'satisfacao_nivel' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue e com satisfação válida
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    cur = cur.dropna(subset=['satisfacao_nivel'])
    
    # Filtrar bairros com mínimo de pedidos (dinâmico baseado no período)
    min_pedidos = _get_min_pedidos(d_inicio, d_fim)
    bairros_relevantes = cur.groupby('bairro_destino').size()
    bairros_relevantes = bairros_relevantes[bairros_relevantes >= min_pedidos].index
    
    cur_filtrado = cur[cur['bairro_destino'].isin(bairros_relevantes)]
    
    if len(cur_filtrado) == 0:
        return {"dados": []}
    
    satisfacao = cur_filtrado.groupby('bairro_destino')['satisfacao_nivel'].mean().sort_values(ascending=False).head(top_n)
    
    dados = [
        {"bairro": str(bairro), "satisfacao": float(valor)}
        for bairro, valor in satisfacao.items()
    ]
    
    return {"dados": dados}

@router.get("/distancia-media-por-bairro")
def distancia_media_por_bairro(inicio: Optional[str] = None, fim: Optional[str] = None, top_n: int = 20):
    """
    Retorna a distância média por bairro, ordenada da maior para a menor
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'bairro_destino' not in cur.columns or 'distance_km' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    cur = cur.dropna(subset=['distance_km'])
    
    # Filtrar bairros com mínimo de pedidos (dinâmico baseado no período)
    min_pedidos = _get_min_pedidos(d_inicio, d_fim)
    bairros_relevantes = cur.groupby('bairro_destino').size()
    bairros_relevantes = bairros_relevantes[bairros_relevantes >= min_pedidos].index
    
    cur_filtrado = cur[cur['bairro_destino'].isin(bairros_relevantes)]
    
    if len(cur_filtrado) == 0:
        return {"dados": []}
    
    distancia = cur_filtrado.groupby('bairro_destino')['distance_km'].mean().sort_values(ascending=False).head(top_n)
    
    dados = [
        {"bairro": str(bairro), "distancia_media": float(valor)}
        for bairro, valor in distancia.items()
    ]
    
    return {"dados": dados}

@router.get("/eficiencia-por-bairro")
def eficiencia_por_bairro(inicio: Optional[str] = None, fim: Optional[str] = None, top_n: int = 20):
    """
    Retorna a eficiência (receita/distância) por bairro, ordenada da maior para a menor
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'bairro_destino' not in cur.columns or 'total_brl' not in cur.columns or 'distance_km' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    cur = cur.dropna(subset=['distance_km', 'total_brl'])
    cur = cur[cur['distance_km'] > 0]  # Evitar divisão por zero
    
    # Filtrar bairros com mínimo de pedidos (dinâmico baseado no período)
    min_pedidos = _get_min_pedidos(d_inicio, d_fim)
    bairros_relevantes = cur.groupby('bairro_destino').size()
    bairros_relevantes = bairros_relevantes[bairros_relevantes >= min_pedidos].index
    
    cur_filtrado = cur[cur['bairro_destino'].isin(bairros_relevantes)]
    
    if len(cur_filtrado) == 0:
        return {"dados": []}
    
    # Calcular eficiência: receita total / distância total
    agg = cur_filtrado.groupby('bairro_destino').agg({
        'total_brl': 'sum',
        'distance_km': 'sum'
    })
    
    agg['eficiencia'] = agg['total_brl'] / agg['distance_km']
    eficiencia = agg['eficiencia'].sort_values(ascending=False).head(top_n)
    
    dados = [
        {"bairro": str(bairro), "eficiencia": float(valor)}
        for bairro, valor in eficiencia.items()
    ]
    
    return {"dados": dados}

# ==================== ANÁLISE POR DISTÂNCIA ====================

def _categorizar_distancia(distancia: float) -> str:
    """Categoriza a distância em faixas de ~0.33 km"""
    faixa_tamanho = 1/3  # Aproximadamente 0.333 km
    
    # Calcula qual faixa a distância pertence
    faixa_index = int(distancia / faixa_tamanho)
    
    # Limita a 45 faixas (0 a 15km em divisões de 0.33km)
    if faixa_index >= 45:
        return "15+ km"
    
    inicio = round(faixa_index * faixa_tamanho, 2)
    fim = round((faixa_index + 1) * faixa_tamanho, 2)
    
    return f"{inicio:.2f}-{fim:.2f} km"

@router.get("/pedidos-por-distancia")
def pedidos_por_distancia(inicio: Optional[str] = None, fim: Optional[str] = None):
    """
    Retorna o volume de pedidos por faixa de distância
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'distance_km' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    cur = cur.dropna(subset=['distance_km'])
    
    # Categorizar distâncias
    cur['faixa_distancia'] = cur['distance_km'].apply(_categorizar_distancia)
    
    # Contar pedidos por faixa
    volume = cur.groupby('faixa_distancia').size().sort_index()
    
    dados = [
        {"faixa": faixa, "pedidos": int(qtd)}
        for faixa, qtd in volume.items()
    ]
    
    return {"dados": dados}

@router.get("/satisfacao-por-distancia")
def satisfacao_por_distancia(inicio: Optional[str] = None, fim: Optional[str] = None):
    """
    Retorna a satisfação média por faixa de distância
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'distance_km' not in cur.columns or 'satisfacao_nivel' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    cur = cur.dropna(subset=['distance_km', 'satisfacao_nivel'])
    
    # Categorizar distâncias
    cur['faixa_distancia'] = cur['distance_km'].apply(_categorizar_distancia)
    
    # Calcular satisfação média por faixa
    satisfacao = cur.groupby('faixa_distancia')['satisfacao_nivel'].mean().sort_index()
    
    dados = [
        {"faixa": faixa, "satisfacao": float(valor)}
        for faixa, valor in satisfacao.items()
    ]
    
    return {"dados": dados}

@router.get("/valor-por-distancia")
def valor_por_distancia(inicio: Optional[str] = None, fim: Optional[str] = None):
    """
    Retorna o valor total (receita) por faixa de distância
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'distance_km' not in cur.columns or 'total_brl' not in cur.columns:
        return {"dados": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    cur = cur.dropna(subset=['distance_km', 'total_brl'])
    
    # Categorizar distâncias
    cur['faixa_distancia'] = cur['distance_km'].apply(_categorizar_distancia)
    
    # Calcular valor total por faixa
    valor = cur.groupby('faixa_distancia')['total_brl'].sum().sort_index()
    
    dados = [
        {"faixa": faixa, "valor": float(val)}
        for faixa, val in valor.items()
    ]
    
    return {"dados": dados}

# ==================== ANÁLISE DE PLATAFORMAS POR BAIRRO ====================

@router.get("/lista-bairros")
def lista_bairros(inicio: Optional[str] = None, fim: Optional[str] = None):
    """
    Retorna a lista de bairros ordenada por volume de pedidos
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'bairro_destino' not in cur.columns:
        return {"bairros": []}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    # Contar pedidos por bairro e ordenar
    volume_por_bairro = cur.groupby('bairro_destino').size().sort_values(ascending=False)
    
    bairros = [
        {"bairro": str(bairro), "total_pedidos": int(qtd)}
        for bairro, qtd in volume_por_bairro.items()
    ]
    
    return {"bairros": bairros}

@router.get("/plataformas-por-bairro")
def plataformas_por_bairro(bairro: str, inicio: Optional[str] = None, fim: Optional[str] = None):
    """
    Retorna a distribuição de plataformas para um bairro específico
    """
    df = load_data()
    d_inicio_default, d_fim_default = _default_range()
    d_inicio = _parse_date(inicio, d_inicio_default)
    d_fim = _parse_date(fim, d_fim_default)
    d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    cur = _filter_period(df, d_inicio, d_fim)
    
    if len(cur) == 0 or 'bairro_destino' not in cur.columns or 'platform' not in cur.columns:
        return {"dados": [], "bairro": bairro, "total": 0}
    
    # Filtrar registros com status entregue
    if 'status' in cur.columns:
        cur = cur[cur['status'].str.lower() == 'delivered']
    
    # Filtrar pelo bairro específico
    cur_bairro = cur[cur['bairro_destino'] == bairro]
    
    if len(cur_bairro) == 0:
        return {"dados": [], "bairro": bairro, "total": 0}
    
    # Contar pedidos por plataforma
    plataformas = cur_bairro.groupby('platform').size()
    total_pedidos = int(plataformas.sum())
    
    dados = [
        {
            "plataforma": str(plat),
            "pedidos": int(qtd),
            "percentual": round((int(qtd) / total_pedidos * 100), 2) if total_pedidos > 0 else 0
        }
        for plat, qtd in plataformas.items()
    ]
    
    # Ordenar por quantidade de pedidos
    dados.sort(key=lambda x: x['pedidos'], reverse=True)
    
    return {"dados": dados, "bairro": bairro, "total": total_pedidos}
