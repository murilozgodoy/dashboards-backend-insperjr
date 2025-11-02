from fastapi import APIRouter, Query
from typing import Optional
from datetime import datetime, timedelta
import pandas as pd

from services.data_loader import load_data

router = APIRouter(prefix="/api/temporal", tags=["temporal"])

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

def _criar_segmentacoes_temporais(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Extrair componentes temporais
    df['hora'] = df['order_datetime'].dt.hour
    df['dia_semana'] = df['order_datetime'].dt.day_name()
    df['mes'] = df['order_datetime'].dt.month
    
    # Período do dia
    def periodo_dia(hora):
        if 0 <= hora < 6:
            return 'Madrugada'
        elif 6 <= hora < 12:
            return 'Manhã'
        elif 12 <= hora < 18:
            return 'Tarde'
        else:
            return 'Noite'
    
    df['periodo_dia'] = df['hora'].apply(periodo_dia)
    
    # Intensidade de demanda
    def intensidade_demanda(hora):
        if hora in [12, 13, 14, 19, 20]:
            return 'Pico'
        elif hora in [11, 15, 16, 17, 18, 21]:
            return 'Alta'
        elif hora in [7, 8, 9, 10, 22]:
            return 'Média'
        else:
            return 'Baixa'
    
    df['intensidade_demanda'] = df['hora'].apply(intensidade_demanda)
    
    # Tipo de dia
    def tipo_dia(dia_semana):
        if dia_semana in ['Saturday', 'Sunday']:
            return 'Fim de Semana'
        else:
            return 'Dia Útil'
    
    df['tipo_dia'] = df['dia_semana'].apply(tipo_dia)
    
    # Estação do ano
    def estacao(mes):
        if mes in [12, 1, 2]:
            return 'Verão'
        elif mes in [3, 4, 5]:
            return 'Outono'
        elif mes in [6, 7, 8]:
            return 'Inverno'
        else:
            return 'Primavera'
    
    df['estacao'] = df['mes'].apply(estacao)
    
    return df

@router.get("/periodo-dia")
def get_periodo_dia(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Retorna distribuição de pedidos por período do dia"""
    df = load_data()
    
    if inicio and fim:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
        df = _filter_period(df, d_inicio, d_fim)
    
    df = _criar_segmentacoes_temporais(df)
    
    periodo_counts = df['periodo_dia'].value_counts().to_dict()
    
    # Ordenar por período
    ordem = ['Madrugada', 'Manhã', 'Tarde', 'Noite']
    resultado = [{"periodo": p, "quantidade": int(periodo_counts.get(p, 0))} for p in ordem]
    
    return {"data": resultado}

@router.get("/tipo-dia")
def get_tipo_dia(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Retorna distribuição por tipo de dia (útil vs fim de semana)"""
    df = load_data()
    
    if inicio and fim:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
        df = _filter_period(df, d_inicio, d_fim)
    
    df = _criar_segmentacoes_temporais(df)
    
    tipo_counts = df['tipo_dia'].value_counts().to_dict()
    
    resultado = [
        {"tipo": "Dia Útil", "quantidade": int(tipo_counts.get('Dia Útil', 0))},
        {"tipo": "Fim de Semana", "quantidade": int(tipo_counts.get('Fim de Semana', 0))}
    ]
    
    return {"data": resultado}

@router.get("/heatmap-horario")
def get_heatmap_horario(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Retorna dados para heatmap de pedidos por dia da semana e hora"""
    df = load_data()
    
    if inicio and fim:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
        df = _filter_period(df, d_inicio, d_fim)
    
    df = _criar_segmentacoes_temporais(df)
    
    # Mapear dias da semana
    dias_map = {
        'Monday': 'Segunda',
        'Tuesday': 'Terça',
        'Wednesday': 'Quarta',
        'Thursday': 'Quinta',
        'Friday': 'Sexta',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    
    df['dia_semana_pt'] = df['dia_semana'].map(dias_map)
    
    heatmap_data = df.groupby(['dia_semana_pt', 'hora']).size().reset_index(name='quantidade')
    
    resultado = [
        {
            "dia_semana": row['dia_semana_pt'],
            "hora": int(row['hora']),
            "quantidade": int(row['quantidade'])
        }
        for _, row in heatmap_data.iterrows()
    ]
    
    return {"data": resultado}

@router.get("/evolucao-pedidos")
def get_evolucao_pedidos(
    inicio: Optional[str] = None,
    fim: Optional[str] = None,
    granularidade: Optional[str] = Query(default="dia", regex="^(dia|semana|mes)$")
):
    """Retorna evolução de pedidos e receita ao longo do tempo"""
    df = load_data()
    
    if inicio and fim:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
        df = _filter_period(df, d_inicio, d_fim)
    
    if 'order_datetime' not in df.columns:
        return {"data": []}
    
    df = df.copy()
    
    if granularidade == "dia":
        df['periodo'] = df['order_datetime'].dt.date
    elif granularidade == "semana":
        df['periodo'] = df['order_datetime'].dt.to_period('W').dt.start_time.dt.date
    else:  # mes
        df['periodo'] = df['order_datetime'].dt.to_period('M').dt.start_time.dt.date
    
    if 'total_brl' in df.columns:
        evolucao = df.groupby('periodo').agg({
            'total_brl': ['sum', 'count']
        }).reset_index()
        evolucao.columns = ['periodo', 'receita', 'pedidos']
    else:
        evolucao = df.groupby('periodo').size().reset_index(name='pedidos')
        evolucao['receita'] = 0
    
    resultado = [
        {
            "periodo": str(row['periodo']),
            "receita": float(row['receita']) if 'receita' in row else 0.0,
            "pedidos": int(row['pedidos'])
        }
        for _, row in evolucao.iterrows()
    ]
    
    return {"data": resultado}

@router.get("/horario-pico")
def get_horario_pico(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Retorna distribuição de pedidos por hora do dia"""
    df = load_data()
    
    if inicio and fim:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
        df = _filter_period(df, d_inicio, d_fim)
    
    df = _criar_segmentacoes_temporais(df)
    
    hora_counts = df.groupby('hora').size().reset_index(name='quantidade')
    
    resultado = [
        {"hora": int(row['hora']), "quantidade": int(row['quantidade'])}
        for _, row in hora_counts.iterrows()
    ]
    
    resultado.sort(key=lambda x: x['hora'])
    
    return {"data": resultado}

@router.get("/sazonalidade-semanal")
def get_sazonalidade_semanal(inicio: Optional[str] = None, fim: Optional[str] = None, metric: Optional[str] = Query(default="pedidos", regex="^(pedidos|receita)$")):
    """Retorna distribuição de pedidos ou receita por dia da semana"""
    df = load_data()
    
    if inicio and fim:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
        df = _filter_period(df, d_inicio, d_fim)
    
    df = _criar_segmentacoes_temporais(df)
    
    # Mapear dias da semana
    dias_map = {
        'Monday': 'Segunda',
        'Tuesday': 'Terça',
        'Wednesday': 'Quarta',
        'Thursday': 'Quinta',
        'Friday': 'Sexta',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    
    df['dia_semana_pt'] = df['dia_semana'].map(dias_map)
    
    ordem_dias = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    
    if metric == "receita" and 'total_brl' in df.columns:
        dados = df.groupby('dia_semana_pt')['total_brl'].sum().to_dict()
        resultado = [
            {"dia_semana": dia, "valor": float(dados.get(dia, 0))}
            for dia in ordem_dias
        ]
    else:
        dados = df.groupby('dia_semana_pt').size().to_dict()
        resultado = [
            {"dia_semana": dia, "valor": int(dados.get(dia, 0))}
            for dia in ordem_dias
        ]
    
    return {"data": resultado, "metric": metric}

@router.get("/comparacao-tendencias")
def get_comparacao_tendencias(
    inicio: Optional[str] = None,
    fim: Optional[str] = None,
    granularidade: Optional[str] = Query(default="semana", regex="^(semana|mes)$")
):
    """Retorna crescimento/queda percentual por período comparado ao anterior"""
    df = load_data()
    
    if not inicio or not fim:
        # Se não tiver datas, pega os últimos 2 períodos disponíveis
        d_fim = datetime.now()
        if granularidade == "semana":
            d_inicio = d_fim - timedelta(days=14)
        else:
            d_inicio = d_fim.replace(day=1) - timedelta(days=1)
            d_inicio = d_inicio.replace(day=1)
    else:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    # Filtrar dados
    mask = (df['order_datetime'] >= d_inicio) & (df['order_datetime'] <= d_fim)
    df_filtrado = df.loc[mask].copy()
    
    if 'order_datetime' not in df_filtrado.columns:
        return {"data": []}
    
    # Calcular período médio
    if granularidade == "semana":
        df_filtrado['periodo'] = df_filtrado['order_datetime'].dt.to_period('W').dt.start_time.dt.date
        periodo_duracao = timedelta(days=7)
    else:  # mes
        df_filtrado['periodo'] = df_filtrado['order_datetime'].dt.to_period('M').dt.start_time.dt.date
        periodo_duracao = timedelta(days=30)
    
    # Agrupar por período
    if 'total_brl' in df_filtrado.columns:
        periodos = df_filtrado.groupby('periodo').agg({
            'total_brl': ['sum', 'count']
        }).reset_index()
        periodos.columns = ['periodo', 'receita', 'pedidos']
    else:
        periodos = df_filtrado.groupby('periodo').size().reset_index(name='pedidos')
        periodos['receita'] = 0
    
    periodos = periodos.sort_values('periodo').reset_index(drop=True)
    
    if len(periodos) < 2:
        return {"data": []}
    
    resultado = []
    for i in range(1, len(periodos)):
        periodo_atual = periodos.iloc[i]
        periodo_anterior = periodos.iloc[i-1]
        
        # Calcular variação percentual
        if periodo_anterior['pedidos'] > 0:
            variacao_pedidos = ((periodo_atual['pedidos'] - periodo_anterior['pedidos']) / periodo_anterior['pedidos']) * 100
        else:
            variacao_pedidos = 100.0 if periodo_atual['pedidos'] > 0 else 0.0
        
        if periodo_anterior['receita'] > 0:
            variacao_receita = ((periodo_atual['receita'] - periodo_anterior['receita']) / periodo_anterior['receita']) * 100
        else:
            variacao_receita = 100.0 if periodo_atual['receita'] > 0 else 0.0
        
        resultado.append({
            "periodo": str(periodo_atual['periodo']),
            "pedidos": int(periodo_atual['pedidos']),
            "receita": float(periodo_atual['receita']),
            "variacao_pedidos_pct": round(variacao_pedidos, 2),
            "variacao_receita_pct": round(variacao_receita, 2)
        })
    
    return {"data": resultado, "granularidade": granularidade}

@router.get("/tendencias-diarias")
def get_tendencias_diarias(inicio: Optional[str] = None, fim: Optional[str] = None):
    """Retorna média de pedidos por dia da semana"""
    df = load_data()
    
    if inicio and fim:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
        df = _filter_period(df, d_inicio, d_fim)
    
    df = _criar_segmentacoes_temporais(df)
    
    # Mapear dias da semana
    dias_map = {
        'Monday': 'Segunda',
        'Tuesday': 'Terça',
        'Wednesday': 'Quarta',
        'Thursday': 'Quinta',
        'Friday': 'Sexta',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    
    df['dia_semana_pt'] = df['dia_semana'].map(dias_map)
    
    # Contar ocorrências por dia da semana
    contagem_por_dia = df.groupby('dia_semana_pt').size().to_dict()
    
    # Contar quantas vezes cada dia aparece no período (para calcular média)
    df['data'] = df['order_datetime'].dt.date
    dias_unicos_por_semana = df.groupby('dia_semana_pt')['data'].nunique().to_dict()
    
    ordem_dias = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    
    resultado = []
    for dia in ordem_dias:
        total_pedidos = contagem_por_dia.get(dia, 0)
        num_ocorrencias = dias_unicos_por_semana.get(dia, 1)
        media = total_pedidos / num_ocorrencias if num_ocorrencias > 0 else 0
        
        resultado.append({
            "dia_semana": dia,
            "total_pedidos": int(total_pedidos),
            "media_pedidos": round(media, 2)
        })
    
    return {"data": resultado}

@router.get("/previsto-vs-real")
def get_previsto_vs_real(
    inicio: Optional[str] = None,
    fim: Optional[str] = None
):
    """Retorna comparação entre pedidos previstos (média histórica) e reais - apenas diário
    
    A previsão usa média histórica considerando:
    1. Dia da semana (mais preciso - padrões semanais são consistentes)
    2. Fallback para média geral se não houver dados suficientes para um dia da semana
    """
    df = load_data()
    
    if not inicio or not fim:
        # Se não tiver datas, pega o último mês
        d_fim = datetime.now()
        d_inicio = d_fim.replace(day=1)
    else:
        d_inicio = _parse_date(inicio, datetime.now())
        d_fim = _parse_date(fim, datetime.now())
        d_inicio, d_fim = _normalize_range(d_inicio, d_fim)
    
    # Filtrar dados do período selecionado
    mask_periodo = (df['order_datetime'] >= d_inicio) & (df['order_datetime'] <= d_fim)
    df_periodo = df.loc[mask_periodo].copy()
    
    if 'order_datetime' not in df_periodo.columns or len(df_periodo) == 0:
        return {"data": []}
    
    # Agrupar período selecionado por dia
    df_periodo['periodo'] = df_periodo['order_datetime'].dt.date
    df_periodo['dia_semana'] = df_periodo['order_datetime'].dt.dayofweek  # 0=segunda, 6=domingo
    
    # Pedidos reais por dia
    real = df_periodo.groupby('periodo').size().reset_index(name='pedidos_real')
    
    # Adicionar dia_semana ao real
    periodo_dia_semana = df_periodo.groupby('periodo')['dia_semana'].first().reset_index()
    real = real.merge(periodo_dia_semana, on='periodo', how='left')
    
    # Para calcular média histórica: pegar dados ANTES do período selecionado
    df_historico = df.loc[df['order_datetime'] < d_inicio].copy()
    
    if len(df_historico) > 0:
        # Criar colunas para análise
        df_historico['data'] = df_historico['order_datetime'].dt.date
        df_historico['dia_semana'] = df_historico['order_datetime'].dt.dayofweek  # 0=segunda, 6=domingo
        
        # Contar pedidos por dia no histórico
        pedidos_por_dia = df_historico.groupby(['dia_semana', 'data']).size().reset_index(name='pedidos')
        
        # Calcular a média por dia da semana
        media_por_dia_semana = pedidos_por_dia.groupby('dia_semana')['pedidos'].mean().reset_index(name='media_pedidos')
        media_por_dia_semana_dict = media_por_dia_semana.set_index('dia_semana')['media_pedidos'].to_dict()
        
        # Calcular média geral como fallback (caso não tenha dados suficientes para um dia da semana)
        media_geral = pedidos_por_dia['pedidos'].mean()
        
        # Mapear dia_semana para os períodos reais (com fallback para média geral)
        real['pedidos_previsto'] = real['dia_semana'].map(
            lambda x: round(media_por_dia_semana_dict.get(int(x), media_geral), 2) if pd.notna(x) else round(media_geral, 2)
        )
    else:
        real['pedidos_previsto'] = 0.0
    
    # Formatar resultado
    resultado = []
    for _, row in real.iterrows():
        resultado.append({
            "periodo": str(row['periodo']),
            "pedidos_real": int(row['pedidos_real']),
            "pedidos_previsto": float(row['pedidos_previsto'])
        })
    
    resultado.sort(key=lambda x: x['periodo'])
    
    return {"data": resultado}

