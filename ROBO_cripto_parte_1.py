import os
import time
import sqlite3
import logging
import threading
from datetime import datetime
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass

# Carrega variáveis de ambiente
load_dotenv()

# ===================================================================
# CONFIGURAÇÕES
# ===================================================================

class Config:
    # ========== TIMEFRAMES ==========
    INTERVALO_1D = Client.KLINE_INTERVAL_1DAY      # 6 meses
    INTERVALO_4H = Client.KLINE_INTERVAL_4HOUR     # Direção do ativo
    INTERVALO_1H = Client.KLINE_INTERVAL_1HOUR     # Padrões gráficos
    INTERVALO_15M = Client.KLINE_INTERVAL_15MINUTE # Ponto de entrada
    
    # ========== QUANTIDADE DE VELAS ==========
    LIMITE_1D = 180      # 180 velas = 6 meses
    LIMITE_4H = 450      # 450 velas = 75 dias
    LIMITE_1H = 200      # 200 velas = 8.3 dias
    LIMITE_15M = 100     # 100 velas = 25 horas
    
    # ========== PARÂMETROS TÉCNICOS ==========
    PERIODO_RSI = 14
    PERIODO_BOLLINGER = 20
    MEDIA_RAPIDA = 12
    MEDIA_LENTA = 50
    MEDIA_TENDENCIA = 200
    
    RSI_SOBREVENDA = 30
    RSI_SOBRECOMPRA = 70
    RSI_NEUTRO_ALTA = 45
    
    # ========== PESOS PARA SCORE ==========
    PESO_TENDENCIA_MACRO = 0.30    # 1D
    PESO_DIRECAO_4H = 0.30         # 4H
    PESO_PADROES = 0.20            # Padrões gráficos
    PESO_ENTRADA_15M = 0.20        # 15M
    
    # Pesos para combinação da análise 1D
    PESO_TRADICIONAL_1D = 0.60     # Análise tradicional (médias)
    PESO_ESTRUTURA_1D = 0.40       # Análise de estrutura (topos/fundos)
    
    SCORE_MINIMO_COMPRA = 70
    
    # ========== STOP E CAPITAL ==========
    STOP_LOSS = 0.03
    STOP_LOSS_DINAMICO = 0.02
    INTERVALO_ATUALIZACAO = 120     # 2 minutos
    SALDO_MINIMO = 0.00001
    TAXA_BINANCE = 0.001
    BATCH_SIZE = 5
    
    # ========== PARES PARA TRADING ==========
    PARES_TRADING = [
        {'par': 'BTCBRL', 'ativo': 'BTC', 'capital': 200.00},
        {'par': 'ETHBRL', 'ativo': 'ETH', 'capital': 200.00},
        {'par': 'SOLBRL', 'ativo': 'SOL', 'capital': 200.00},
        {'par': 'BNBBRL', 'ativo': 'BNB', 'capital': 200.00},
        {'par': 'LTCBRL', 'ativo': 'LTC', 'capital': 200.00},
        {'par': 'DOGEBRL', 'ativo': 'DOGE', 'capital': 200.00},
        {'par': 'XRPBRL', 'ativo': 'XRP', 'capital': 200.00},
        {'par': 'NEARBRL', 'ativo': 'NEAR', 'capital': 200.00},
        {'par': 'SANDBRL', 'ativo': 'SAND', 'capital': 200.00},
        {'par': 'ATOMBRL', 'ativo': 'ATOM', 'capital': 200.00},
    ]

# ===================================================================
# CLASSES DE DADOS
# ===================================================================

@dataclass
class Estado:
    par: str
    ativo: str
    capital_inicial: float
    ativo_comprado: float = 0.0
    capital_operacional: float = 0.0
    preco_maximo: float = 0.0
    posicao_aberta: bool = False
    preco_compra: float = 0.0
    ultima_atualizacao: Optional[str] = None
    contador_venda: int = 0
    ultimo_fundo_local: float = 0.0
    ultimo_topo_local: float = 0.0

    def __post_init__(self):
        self.capital_operacional = self.capital_inicial


class GerenciadorEstados:
    def __init__(self):
        self.estados: Dict[str, Estado] = {
            config_par['par']: Estado(
                config_par['par'],
                config_par['ativo'],
                config_par['capital']
            ) for config_par in Config.PARES_TRADING
        }


# ===================================================================
# ANALISADOR DE PADRÕES GRÁFICOS
# ===================================================================

class AnalisadorGrafico:
    """Analisa padrões gráficos em diferentes timeframes"""
    
    @staticmethod
    def encontrar_topos_fundos(df: pd.DataFrame, janela: int = 5) -> Tuple[List, List]:
        """Encontra topos e fundos locais no gráfico"""
        highs = df['high'].astype(float)
        lows = df['low'].astype(float)
        
        topos = []
        fundos = []
        
        for i in range(janela, len(df) - janela):
            if highs.iloc[i] == max(highs.iloc[i-janela:i+janela+1]):
                topos.append({
                    'index': i,
                    'preco': highs.iloc[i],
                    'timestamp': df.iloc[i]['timestamp']
                })
            
            if lows.iloc[i] == min(lows.iloc[i-janela:i+janela+1]):
                fundos.append({
                    'index': i,
                    'preco': lows.iloc[i],
                    'timestamp': df.iloc[i]['timestamp']
                })
        
        return topos, fundos
    
    @staticmethod
    def analisar_estrutura_mercado(df: pd.DataFrame, periodo_analise: int = 90) -> Dict:
        """
        Analisa a estrutura de mercado baseada em topos e fundos (Price Action)
        
        Conceito:
        - Tendência de ALTA: Topos e Fundos cada vez mais altos
        - Tendência de BAIXA: Topos e Fundos cada vez mais baixos
        """
        if df is None or len(df) < periodo_analise:
            return {'tendencia_estrutural': 'NEUTRA', 'forca_tendencia': 50, 'erro': True}
        
        # Pega os últimos 'periodo_analise' dias
        df_analise = df.tail(periodo_analise)
        fechamentos = df_analise['close'].astype(float)
        
        # Encontra topos e fundos significativos
        topos, fundos = AnalisadorGrafico.encontrar_topos_fundos(df_analise, janela=5)
        
        if len(topos) < 2 or len(fundos) < 2:
            return {'tendencia_estrutural': 'NEUTRA', 'forca_tendencia': 50, 
                    'topos': topos, 'fundos': fundos, 'erro': True}
        
        # Pega os últimos 5 topos e fundos para análise
        ultimos_topos = topos[-5:] if len(topos) >= 5 else topos
        ultimos_fundos = fundos[-5:] if len(fundos) >= 5 else fundos
        
        # ========== ANÁLISE DA TENDÊNCIA POR TOPOS ==========
        topos_altos = True
        topos_baixos = True
        
        for i in range(1, len(ultimos_topos)):
            if ultimos_topos[i]['preco'] <= ultimos_topos[i-1]['preco']:
                topos_altos = False
            if ultimos_topos[i]['preco'] >= ultimos_topos[i-1]['preco']:
                topos_baixos = False
        
        # ========== ANÁLISE DA TENDÊNCIA POR FUNDOS ==========
        fundos_altos = True
        fundos_baixos = True
        
        for i in range(1, len(ultimos_fundos)):
            if ultimos_fundos[i]['preco'] <= ultimos_fundos[i-1]['preco']:
                fundos_altos = False
            if ultimos_fundos[i]['preco'] >= ultimos_fundos[i-1]['preco']:
                fundos_baixos = False
        
        # ========== DETERMINA A TENDÊNCIA ESTRUTURAL ==========
        tendencia_estrutural = "NEUTRA"
        forca_tendencia = 50
        score_tendencia = 50
        
        # Tendência de ALTA: Topos e Fundos mais altos
        if topos_altos and fundos_altos:
            tendencia_estrutural = "ALTA"
            score_tendencia = 75
            forca_tendencia = 80
        # Tendência de ALTA (apenas topos mais altos)
        elif topos_altos:
            tendencia_estrutural = "ALTA_FRACA"
            score_tendencia = 65
            forca_tendencia = 60
        # Tendência de BAIXA: Topos e Fundos mais baixos
        elif topos_baixos and fundos_baixos:
            tendencia_estrutural = "BAIXA"
            score_tendencia = 30
            forca_tendencia = 80
        # Tendência de BAIXA (apenas fundos mais baixos)
        elif fundos_baixos:
            tendencia_estrutural = "BAIXA_FRACA"
            score_tendencia = 35
            forca_tendencia = 60
        else:
            tendencia_estrutural = "NEUTRA"
            score_tendencia = 50
            forca_tendencia = 30
        
        # ========== VERIFICA QUEBRA DE ESTRUTURA ==========
        preco_atual = fechamentos.iloc[-1]
        ultimo_topo = ultimos_topos[-1]['preco'] if ultimos_topos else 0
        ultimo_fundo = ultimos_fundos[-1]['preco'] if ultimos_fundos else 0
        
        quebra_alta = preco_atual < ultimo_fundo if ultimo_fundo > 0 else False
        quebra_baixa = preco_atual > ultimo_topo if ultimo_topo > 0 else False
        estrutura_quebrada = quebra_alta or quebra_baixa
        
        return {
            'tendencia_estrutural': tendencia_estrutural,
            'forca_tendencia': forca_tendencia,
            'score_tendencia': score_tendencia,
            'topos': ultimos_topos,
            'fundos': ultimos_fundos,
            'topos_altos': topos_altos,
            'fundos_altos': fundos_altos,
            'estrutura_quebrada': estrutura_quebrada,
            'ultimo_topo': ultimo_topo,
            'ultimo_fundo': ultimo_fundo,
            'quebra_alta': quebra_alta,
            'quebra_baixa': quebra_baixa,
            'preco_atual': preco_atual,
            'erro': False
        }
    
    @staticmethod
    def encontrar_topos_fundos_historicos(df: pd.DataFrame) -> Dict:
        """Encontra topos e fundos históricos"""
        fechamentos = df['close'].astype(float)
        
        max_geral = fechamentos.max()
        min_geral = fechamentos.min()
        max_90d = fechamentos.tail(90).max()
        min_90d = fechamentos.tail(90).min()
        max_30d = fechamentos.tail(30).max()
        min_30d = fechamentos.tail(30).min()
        
        topos_locais, fundos_locais = AnalisadorGrafico.encontrar_topos_fundos(df, janela=7)
        
        topos_significativos = sorted(topos_locais, key=lambda x: x['preco'], reverse=True)[:5]
        fundos_significativos = sorted(fundos_locais, key=lambda x: x['preco'])[:5]
        
        return {
            'maximo_historico': max_geral,
            'minimo_historico': min_geral,
            'maximo_90d': max_90d,
            'minimo_90d': min_90d,
            'maximo_30d': max_30d,
            'minimo_30d': min_30d,
            'topos_significativos': topos_significativos,
            'fundos_significativos': fundos_significativos
        }
    
    @staticmethod
    def calcular_suporte_resistencia(df: pd.DataFrame) -> Dict:
        """Calcula níveis de suporte e resistência"""
        fechamentos = df['close'].astype(float)
        
        max_90d = fechamentos.tail(90).max()
        min_90d = fechamentos.tail(90).min()
        max_30d = fechamentos.tail(30).max()
        min_30d = fechamentos.tail(30).min()
        
        media_20 = fechamentos.rolling(20).mean().iloc[-1]
        media_50 = fechamentos.rolling(50).mean().iloc[-1]
        media_200 = fechamentos.rolling(200).mean().iloc[-1]
        
        preco_atual = fechamentos.iloc[-1]
        
        nivel_psicologico_top = round(preco_atual / 100) * 100 + 100
        nivel_psicologico_bottom = round(preco_atual / 100) * 100
        
        return {
            'resistencia_principal': max_90d,
            'suporte_principal': min_90d,
            'resistencia_recente': max_30d,
            'suporte_recente': min_30d,
            'media_20': media_20,
            'media_50': media_50,
            'media_200': media_200,
            'nivel_psicologico_top': nivel_psicologico_top,
            'nivel_psicologico_bottom': nivel_psicologico_bottom,
            'distancia_resistencia': ((max_90d - preco_atual) / preco_atual) * 100,
            'distancia_suporte': ((preco_atual - min_90d) / preco_atual) * 100
        }
    
    @staticmethod
    def identificar_padroes_candle(df: pd.DataFrame) -> List:
        """Identifica padrões de candles importantes"""
        padroes = []
        
        ultima = df.iloc[-1]
        penultima = df.iloc[-2] if len(df) > 1 else None
        
        open_price = float(ultima['open'])
        close_price = float(ultima['close'])
        high_price = float(ultima['high'])
        low_price = float(ultima['low'])
        
        corpo = abs(close_price - open_price)
        sombra_superior = high_price - max(open_price, close_price)
        sombra_inferior = min(open_price, close_price) - low_price
        tamanho_total = high_price - low_price
        
        if tamanho_total == 0:
            return padroes
        
        if corpo < tamanho_total * 0.1:
            padroes.append({'tipo': 'DOJI', 'forca': 0.3, 'direcao': 'NEUTRA'})
        elif sombra_inferior > corpo * 2 and sombra_superior < corpo:
            padroes.append({'tipo': 'MARTELO', 'forca': 0.6, 'direcao': 'ALTA'})
        elif sombra_superior > corpo * 2 and sombra_inferior < corpo:
            padroes.append({'tipo': 'ESTRELA_CADENTE', 'forca': 0.6, 'direcao': 'BAIXA'})
        
        if penultima is not None:
            prev_close = float(penultima['close'])
            prev_open = float(penultima['open'])
            prev_baixa = prev_close < prev_open
            
            if prev_baixa and close_price > open_price:
                if open_price <= prev_close and close_price >= prev_open:
                    padroes.append({'tipo': 'ENGULFING_ALTA', 'forca': 0.8, 'direcao': 'ALTA'})
        
        return padroes
    
    @staticmethod
    def identificar_ltb_lta(df: pd.DataFrame) -> Dict:
        """Identifica Linhas de Tendência de Alta (LTA) e Baixa (LTB)"""
        fechamentos = df['close'].astype(float)
        preco_atual = fechamentos.iloc[-1]
        
        topos, fundos = AnalisadorGrafico.encontrar_topos_fundos(df, janela=3)
        
        lta_valida = False
        ltb_valida = False
        lta_preco = 0
        ltb_preco = 0
        
        if len(fundos) >= 2:
            fundos_recentes = fundos[-3:]
            if len(fundos_recentes) >= 2:
                if fundos_recentes[-2]['preco'] < fundos_recentes[-1]['preco']:
                    lta_valida = True
                    lta_preco = fundos_recentes[-1]['preco']
        
        if len(topos) >= 2:
            topos_recentes = topos[-3:]
            if len(topos_recentes) >= 2:
                if topos_recentes[-2]['preco'] > topos_recentes[-1]['preco']:
                    ltb_valida = True
                    ltb_preco = topos_recentes[-1]['preco']
        
        return {
            'lta_valida': lta_valida,
            'ltb_valida': ltb_valida,
            'lta_preco': lta_preco,
            'ltb_preco': ltb_preco,
            'preco_acima_lta': preco_atual > lta_preco if lta_valida else False,
            'preco_abaixo_ltb': preco_atual < ltb_preco if ltb_valida else False
        }
    
    @staticmethod
    def identificar_figuras_graficas(df: pd.DataFrame) -> List:
        """Identifica figuras gráficas"""
        if len(df) < 50:
            return []
        
        fechamentos = df['close'].astype(float)
        highs = df['high'].astype(float)
        lows = df['low'].astype(float)
        
        figuras = []
        ultimas_20 = df.tail(20)
        
        # Bandeira
        if len(fechamentos) >= 50:
            preco_inicio_bandeira = fechamentos.iloc[-50]
            preco_fim_bandeira = fechamentos.iloc[-1]
            movimento_total = (preco_fim_bandeira - preco_inicio_bandeira) / preco_inicio_bandeira * 100
            
            desvio_20 = fechamentos.tail(20).std()
            desvio_antes = fechamentos.tail(50).head(30).std()
            
            if abs(movimento_total) > 5 and desvio_20 < desvio_antes * 0.6:
                figuras.append({
                    'tipo': 'BANDEIRA',
                    'direcao': 'ALTA' if movimento_total > 0 else 'BAIXA',
                    'forca': 0.7,
                    'preco_rompimento': fechamentos.iloc[-2]
                })
        
        # Triângulos
        if len(ultimas_20) >= 20:
            ultimos_20_highs = highs.tail(20)
            ultimos_20_lows = lows.tail(20)
            
            topo_plano = (ultimos_20_highs.max() - ultimos_20_highs.min()) < (ultimos_20_highs.mean() * 0.02)
            fundos_ascendentes = ultimos_20_lows.iloc[-1] > ultimos_20_lows.iloc[0]
            
            if topo_plano and fundos_ascendentes:
                figuras.append({
                    'tipo': 'TRIANGULO_ASCENDENTE',
                    'direcao': 'ALTA',
                    'forca': 0.75,
                    'resistencia': ultimos_20_highs.max(),
                    'preco_atual': fechamentos.iloc[-1]
                })
            
            fundo_plano = (ultimos_20_lows.max() - ultimos_20_lows.min()) < (ultimos_20_lows.mean() * 0.02)
            topos_descendentes = ultimos_20_highs.iloc[-1] < ultimos_20_highs.iloc[0]
            
            if fundo_plano and topos_descendentes:
                figuras.append({
                    'tipo': 'TRIANGULO_DESCENDENTE',
                    'direcao': 'BAIXA',
                    'forca': 0.75,
                    'suporte': ultimos_20_lows.min()
                })
        
        # Fundo Duplo
        topos, fundos = AnalisadorGrafico.encontrar_topos_fundos(df, janela=5)
        
        if len(fundos) >= 4:
            fundo1 = fundos[-3] if len(fundos) >= 3 else None
            fundo2 = fundos[-1] if len(fundos) >= 1 else None
            topo_meio = topos[-2] if len(topos) >= 2 else None
            
            if fundo1 and fundo2 and topo_meio:
                diferenca = abs(fundo1['preco'] - fundo2['preco']) / fundo1['preco'] * 100
                if diferenca < 3 and topo_meio['preco'] > fundo1['preco'] * 1.03:
                    figuras.append({
                        'tipo': 'FUNDO_DUPLO',
                        'direcao': 'ALTA',
                        'forca': 0.8,
                        'preco_rompimento': topo_meio['preco'],
                        'preco_atual': fechamentos.iloc[-1]
                    })
        
        # Topo Duplo
        if len(topos) >= 4:
            topo1 = topos[-3] if len(topos) >= 3 else None
            topo2 = topos[-1] if len(topos) >= 1 else None
            fundo_meio = fundos[-2] if len(fundos) >= 2 else None
            
            if topo1 and topo2 and fundo_meio:
                diferenca = abs(topo1['preco'] - topo2['preco']) / topo1['preco'] * 100
                if diferenca < 3 and fundo_meio['preco'] < topo1['preco'] * 0.97:
                    figuras.append({
                        'tipo': 'TOPO_DUPLO',
                        'direcao': 'BAIXA',
                        'forca': 0.8,
                        'preco_rompimento': fundo_meio['preco']
                    })
        
        return figuras
    
    @staticmethod
    def calcular_metade_movimento(df: pd.DataFrame) -> Dict:
        """Calcula a metade do movimento"""
        if len(df) < 50:
            return {'metade_alta': 0, 'metade_baixa': 0, 'movimento_total': 0}
        
        fechamentos = df['close'].astype(float)
        minimo_periodo = fechamentos.tail(50).min()
        maximo_periodo = fechamentos.tail(50).max()
        
        movimento_total = maximo_periodo - minimo_periodo
        metade_movimento = minimo_periodo + (movimento_total / 2)
        
        return {
            'minimo_periodo': minimo_periodo,
            'maximo_periodo': maximo_periodo,
            'movimento_total': movimento_total,
            'metade_movimento': metade_movimento,
            'preco_atual_na_metade': fechamentos.iloc[-1] >= metade_movimento
        }


# ===================================================================
# ANALISADOR DE TENDÊNCIA (MULTI-TIMEFRAME)
# ===================================================================

class AnalisadorTendencia:
    """Analisa tendência em múltiplos timeframes"""
    
    def __init__(self, cliente, logger):
        self.cliente = cliente
        self.logger = logger
        self.analisador = AnalisadorGrafico()
        self.cache_dados = {}
        self.cache_timestamp = {}
    
    def buscar_dados(self, par: str, intervalo, limite: int) -> Optional[pd.DataFrame]:
        """Busca dados com cache"""
        chave = f"{par}_{intervalo}"
        agora = time.time()
        
        if chave in self.cache_dados and (agora - self.cache_timestamp.get(chave, 0)) < 60:
            return self.cache_dados[chave]
        
        try:
            klines = self.cliente.get_klines(
                symbol=par,
                interval=intervalo,
                limit=limite
            )
            
            if not klines:
                return None
            
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            self.cache_dados[chave] = df
            self.cache_timestamp[chave] = agora
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao buscar dados {par} {intervalo}: {e}")
            return None
    
    def analisar_estrutura_mercado_1d(self, df: pd.DataFrame) -> Dict:
        """Análise de ESTRUTURA DE MERCADO no timeframe 1D"""
        if df is None or len(df) < 90:
            return {'score': 50, 'tendencia_estrutural': 'NEUTRA', 'erro': True}
        
        estrutura = self.analisador.analisar_estrutura_mercado(df, periodo_analise=90)
        
        if estrutura.get('erro', False):
            return {'score': 50, 'tendencia_estrutural': 'NEUTRA', 'erro': True}
        
        # Mapeia para formato compatível
        tendencia_map = {
            'ALTA': 'ALTA_FORTE',
            'ALTA_FRACA': 'ALTA',
            'NEUTRA': 'NEUTRA',
            'BAIXA_FRACA': 'BAIXA',
            'BAIXA': 'BAIXA_FORTE'
        }
        
        # Log da estrutura
        self.logger.info(f"\n   📊 ESTRUTURA DE MERCADO (Market Structure):")
        self.logger.info(f'      Topos: {[f"{t["preco"]:.2f}" for t in estrutura["topos"][-5:]]}')
        self.logger.info(f'      Fundos: {[f"{f["preco"]:.2f}" for f in estrutura["fundos"][-5:]]}')
        self.logger.info(f"      Topos mais altos: {'SIM' if estrutura['topos_altos'] else 'NAO'}")
        self.logger.info(f"      Fundos mais altos: {'SIM' if estrutura['fundos_altos'] else 'NAO'}")
        self.logger.info(f"      Estrutura: {estrutura['tendencia_estrutural']} (forca: {estrutura['forca_tendencia']:.0f}%)")
        
        if estrutura['estrutura_quebrada']:
            self.logger.info(f"      ⚠️ ESTRUTURA QUEBRADA! Preco rompeu {'fundo' if estrutura['quebra_alta'] else 'topo'} anterior")
        
        return {
            'score': estrutura['score_tendencia'],
            'tendencia_estrutural': estrutura['tendencia_estrutural'],
            'tendencia_macro': tendencia_map.get(estrutura['tendencia_estrutural'], 'NEUTRA'),
            'forca_tendencia': estrutura['forca_tendencia'],
            'estrutura_quebrada': estrutura['estrutura_quebrada'],
            'ultimo_topo': estrutura['ultimo_topo'],
            'ultimo_fundo': estrutura['ultimo_fundo'],
            'topos': estrutura['topos'],
            'fundos': estrutura['fundos'],
            'topos_altos': estrutura['topos_altos'],
            'fundos_altos': estrutura['fundos_altos'],
            'erro': False
        }
    
    def analisar_tendencia_macro_1d(self, df: pd.DataFrame) -> Dict:
        """Análise de 6 meses (1D) - Análise tradicional com médias"""
        if df is None or len(df) < 100:
            return {'score': 50, 'tendencia': 'NEUTRA', 'erro': True}
        
        fechamentos = df['close'].astype(float)
        preco_atual = fechamentos.iloc[-1]
        
        media_50 = fechamentos.rolling(50).mean().iloc[-1] if len(fechamentos) >= 50 else preco_atual
        media_100 = fechamentos.rolling(100).mean().iloc[-1] if len(fechamentos) >= 100 else preco_atual
        media_200 = fechamentos.rolling(200).mean().iloc[-1] if len(fechamentos) >= 200 else preco_atual
        
        # RSI calculado apenas para visualização
        delta = fechamentos.diff()
        ganhos = delta.where(delta > 0, 0)
        perdas = -delta.where(delta < 0, 0)
        media_ganhos = ganhos.rolling(Config.PERIODO_RSI).mean()
        media_perdas = perdas.rolling(Config.PERIODO_RSI).mean()
        rs = media_ganhos / media_perdas
        rsi = 100 - (100 / (1 + rs))
        rsi_atual = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
        
        if len(fechamentos) >= 60:
            ultimos_30 = fechamentos.tail(30).mean()
            anteriores_30 = fechamentos.tail(60).head(30).mean()
            tendencia_30d = (ultimos_30 / anteriores_30 - 1) * 100 if anteriores_30 > 0 else 0
        else:
            tendencia_30d = 0
        
        # Score SEM RSI
        score = 50
        
        if preco_atual > media_200:
            score += 20
        if preco_atual > media_100:
            score += 10
        if preco_atual > media_50:
            score += 10
        if tendencia_30d > 5:
            score += 5
        elif tendencia_30d < -5:
            score -= 10
        
        topos_fundos = self.analisador.encontrar_topos_fundos_historicos(df)
        score = min(100, max(0, score))
        
        if score >= 70:
            tendencia = "ALTA_FORTE"
        elif score >= 55:
            tendencia = "ALTA"
        elif score <= 30:
            tendencia = "BAIXA_FORTE"
        elif score <= 45:
            tendencia = "BAIXA"
        else:
            tendencia = "NEUTRA"
        
        return {
            'score': score,
            'tendencia': tendencia,
            'preco_atual': preco_atual,
            'media_200': media_200,
            'media_100': media_100,
            'media_50': media_50,
            'rsi': rsi_atual,
            'tendencia_30d': tendencia_30d,
            'topos_fundos': topos_fundos,
            'erro': False
        }
    
    def analisar_direcao_4h(self, df: pd.DataFrame) -> Dict:
        """Análise de direção em 4H - SEM RSI na decisão"""
        if df is None or len(df) < 50:
            return {'score': 50, 'direcao': 'NEUTRA', 'erro': True}
        
        fechamentos = df['close'].astype(float)
        preco_atual = fechamentos.iloc[-1]
        
        media_20 = fechamentos.rolling(20).mean().iloc[-1]
        media_50 = fechamentos.rolling(50).mean().iloc[-1]
        media_200 = fechamentos.rolling(200).mean().iloc[-1] if len(fechamentos) >= 200 else media_50
        
        # MACD
        ema_12 = fechamentos.ewm(span=12, adjust=False).mean()
        ema_26 = fechamentos.ewm(span=26, adjust=False).mean()
        macd = ema_12 - ema_26
        sinal = macd.ewm(span=9, adjust=False).mean()
        macd_atual = macd.iloc[-1]
        sinal_atual = sinal.iloc[-1]
        
        # RSI apenas para visualização
        delta = fechamentos.diff()
        ganhos = delta.where(delta > 0, 0)
        perdas = -delta.where(delta < 0, 0)
        media_ganhos = ganhos.rolling(Config.PERIODO_RSI).mean()
        media_perdas = perdas.rolling(Config.PERIODO_RSI).mean()
        rs = media_ganhos / media_perdas
        rsi = 100 - (100 / (1 + rs))
        rsi_atual = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
        
        suporte_resistencia = self.analisador.calcular_suporte_resistencia(df)
        ltb_lta = self.analisador.identificar_ltb_lta(df)
        
        # Score SEM RSI
        score = 50
        
        if preco_atual > media_50:
            score += 15
        if preco_atual > media_20:
            score += 10
        if macd_atual > sinal_atual:
            score += 15
        if suporte_resistencia['distancia_resistencia'] < 15:
            score -= 5
        
        if ltb_lta['lta_valida'] and ltb_lta['preco_acima_lta']:
            score += 10
        
        score = min(100, max(0, score))
        
        if score >= 70:
            direcao = "ALTA_FORTE"
        elif score >= 55:
            direcao = "ALTA"
        elif score <= 30:
            direcao = "BAIXA_FORTE"
        elif score <= 45:
            direcao = "BAIXA"
        else:
            direcao = "NEUTRA"
        
        return {
            'score': score,
            'direcao': direcao,
            'preco_atual': preco_atual,
            'media_50': media_50,
            'media_20': media_20,
            'rsi': rsi_atual,
            'macd_hist': macd_atual - sinal_atual,
            'suporte': suporte_resistencia['suporte_recente'],
            'resistencia': suporte_resistencia['resistencia_recente'],
            'ltb_lta': ltb_lta,
            'erro': False
        }
    
    def analisar_padroes_1h(self, df: pd.DataFrame, tendencia_macro: Dict, direcao_4h: Dict) -> Dict:
        """Análise de padrões gráficos em 1H"""
        if df is None or len(df) < 100:
            return {'score': 0, 'padroes': [], 'sinal_compra': False, 'motivo_compra': ''}
        
        figuras = self.analisador.identificar_figuras_graficas(df)
        ltb_lta = self.analisador.identificar_ltb_lta(df)
        sr = self.analisador.calcular_suporte_resistencia(df)
        padroes_candle = self.analisador.identificar_padroes_candle(df)
        metade_movimento = self.analisador.calcular_metade_movimento(df)
        
        score = 0
        sinal_compra = False
        motivo_compra = ""
        
        tendencia_favoravel = (
            tendencia_macro.get('tendencia', 'NEUTRA') in ['ALTA', 'ALTA_FORTE'] and
            direcao_4h.get('direcao', 'NEUTRA') in ['ALTA', 'ALTA_FORTE']
        )
        
        for figura in figuras:
            if figura['direcao'] == 'ALTA' and tendencia_favoravel:
                if figura['tipo'] == 'BANDEIRA':
                    if figura.get('preco_rompimento') and df['close'].astype(float).iloc[-1] > figura['preco_rompimento']:
                        score += 30
                        motivo_compra = "Rompeu Bandeira a favor da tendencia"
                        sinal_compra = True
                
                elif figura['tipo'] == 'TRIANGULO_ASCENDENTE':
                    resistencia = figura.get('resistencia', 0)
                    preco_atual = figura.get('preco_atual', 0)
                    if resistencia > 0 and preco_atual >= resistencia * 0.99:
                        score += 35
                        motivo_compra = "Rompeu Triangulo Ascendente"
                        sinal_compra = True
                
                elif figura['tipo'] == 'FUNDO_DUPLO':
                    preco_rompimento = figura.get('preco_rompimento', 0)
                    preco_atual = figura.get('preco_atual', 0)
                    if preco_rompimento > 0 and preco_atual >= preco_rompimento:
                        score += 40
                        motivo_compra = "Fundo Duplo confirmado - rompeu topo da figura"
                        sinal_compra = True
        
        # Vela forte no suporte
        if len(df) > 0:
            ultima_vela = df.iloc[-1]
            corpo = abs(float(ultima_vela['close']) - float(ultima_vela['open']))
            tamanho_total = float(ultima_vela['high']) - float(ultima_vela['low'])
            
            if tamanho_total > 0:
                vela_forte = corpo > tamanho_total * 0.6
                vela_alta = float(ultima_vela['close']) > float(ultima_vela['open'])
                preco_atual = float(ultima_vela['close'])
                preco_no_suporte = preco_atual <= sr['suporte_recente'] * 1.02
                
                if vela_forte and vela_alta and preco_no_suporte and tendencia_favoravel:
                    score += 25
                    motivo_compra = "Vela forte de alta no suporte"
                    sinal_compra = True
        
        if metade_movimento['preco_atual_na_metade'] and tendencia_favoravel:
            score += 10
        
        return {
            'score': min(100, score),
            'padroes': figuras,
            'padroes_candle': padroes_candle,
            'sinal_compra': sinal_compra,
            'motivo_compra': motivo_compra,
            'ltb_lta': ltb_lta,
            'sr': sr,
            'metade_movimento': metade_movimento
        }
    
    def analisar_entrada_15m(self, df: pd.DataFrame, tendencia_geral: str, padroes_1h: Dict) -> Dict:
        """Análise de entrada no gráfico de 15 minutos - SEM RSI na decisao"""
        if df is None or len(df) < 30:
            return {'sinal_entrada': False, 'score': 0, 'motivo': ''}
        
        fechamentos = df['close'].astype(float)
        volumes = df['volume'].astype(float)
        preco_atual = fechamentos.iloc[-1]
        
        # RSI apenas para visualização
        delta = fechamentos.diff()
        ganhos = delta.where(delta > 0, 0)
        perdas = -delta.where(delta < 0, 0)
        media_ganhos = ganhos.rolling(Config.PERIODO_RSI).mean()
        media_perdas = perdas.rolling(Config.PERIODO_RSI).mean()
        rs = media_ganhos / media_perdas
        rsi = 100 - (100 / (1 + rs))
        rsi_atual = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
        
        # Bandas de Bollinger
        media = fechamentos.rolling(Config.PERIODO_BOLLINGER).mean()
        desvio = fechamentos.rolling(Config.PERIODO_BOLLINGER).std()
        banda_inferior = media - (desvio * 2)
        
        # Volume
        volume_medio_20 = volumes.rolling(20).mean().iloc[-1] if len(volumes) >= 20 else volumes.mean()
        volume_alto = volumes.iloc[-1] > volume_medio_20 * 1.2 if volume_medio_20 > 0 else False
        
        # Padrões de candle
        ultima = df.iloc[-1]
        open_price = float(ultima['open'])
        close_price = float(ultima['close'])
        high_price = float(ultima['high'])
        low_price = float(ultima['low'])
        
        vela_alta = close_price > open_price
        corpo = abs(close_price - open_price)
        tamanho_total = high_price - low_price
        vela_forte = corpo > tamanho_total * 0.6 if tamanho_total > 0 else False
        
        # Condições de entrada - SEM RSI
        score = 0
        sinal_entrada = False
        motivo = ""
        
        # Preco abaixo da banda inferior
        if len(banda_inferior) > 0 and preco_atual <= banda_inferior.iloc[-1]:
            score += 40
            sinal_entrada = True
            motivo = "Preco abaixo da banda inferior de Bollinger (15m)"
        
        # Vela forte de alta com volume
        elif vela_alta and vela_forte and volume_alto:
            score += 35
            sinal_entrada = True
            motivo = "Vela forte de alta com volume"
        
        # Volume alto
        elif volume_alto:
            score += 25
            sinal_entrada = True
            motivo = "Volume alto indicando possivel movimento"
        
        return {
            'sinal_entrada': sinal_entrada,
            'score': min(100, score),
            'preco_atual': preco_atual,
            'rsi': rsi_atual,
            'volume_confirmado': volume_alto,
            'banda_inferior': banda_inferior.iloc[-1] if len(banda_inferior) > 0 else 0,
            'motivo': motivo
        }


# ===================================================================
# BOT PRINCIPAL
# ===================================================================

class BotTrading:
    def __init__(self):
        self.running = False
        self.thread = None
        self.thread_local = threading.local()

        self.setup_logging()
        
        self.cliente = Client(
            os.getenv('KEY_BINANCE'),
            os.getenv('SECRET_BINANCE')
        )
        
        if not self.verificar_tempo():
            raise Exception("Erro: Sincronizacao de tempo necessaria")
        if not self.verificar_chaves_api():
            raise Exception("Erro: Chaves API invalidas")
        
        self.analisador = AnalisadorTendencia(self.cliente, self.logger)
        self.gerenciador = GerenciadorEstados()
        self.db = self.inicializar_banco()
        self.carregar_estados()
        self.verificar_saldos_iniciais()
        
        self.cache_symbol_info = {}
        self.ultima_analise = {}

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('trading_bot.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def verificar_tempo(self):
        try:
            server_time = self.cliente.get_server_time()
            local_time = int(time.time() * 1000)
            return abs(local_time - server_time['serverTime']) <= 10000
        except Exception as e:
            self.logger.error(f'Erro ao verificar tempo: {e}')
            return False

    def verificar_chaves_api(self):
        try:
            info = self.cliente.get_account()
            return info.get('canTrade', False)
        except Exception as e:
            self.logger.error(f'Erro ao verificar chaves API: {e}')
            return False

    def inicializar_banco(self):
        try:
            if not os.path.exists('data'):
                os.makedirs('data')
                
            conn = sqlite3.connect('data/trades.db', timeout=60)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    timestamp TEXT,
                    price REAL,
                    position TEXT,
                    profit REAL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS estado (
                    par TEXT PRIMARY KEY,
                    ativo TEXT NOT NULL,
                    posicao INTEGER NOT NULL,
                    preco_compra REAL NOT NULL,
                    ativo_comprado REAL NOT NULL,
                    capital_operacional REAL NOT NULL,
                    preco_maximo REAL NOT NULL,
                    ultima_atualizacao TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS operacoes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    par TEXT NOT NULL,
                    ativo TEXT NOT NULL,
                    tipo TEXT NOT NULL,
                    preco REAL NOT NULL,
                    quantidade REAL NOT NULL,
                    valor_total REAL NOT NULL,
                    data_hora TEXT NOT NULL,
                    motivo TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS analises (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    par TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    score_total REAL,
                    tendencia_macro TEXT,
                    direcao_4h TEXT,
                    padrao_detectado TEXT,
                    sinal_compra INTEGER
                )
            ''')
            
            conn.commit()
            return conn
            
        except Exception as e:
            self.logger.error(f'Erro ao inicializar banco: {e}')
            raise

    def get_db(self):
        if not hasattr(self.thread_local, "connection"):
            self.thread_local.connection = sqlite3.connect('data/trades.db')
        return self.thread_local.connection

    def save_trade(self, price, position, profit):
        try:
            conn = self.get_db()
            c = conn.cursor()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            c.execute('INSERT INTO trades VALUES (?, ?, ?, ?)',
                     (timestamp, price, position, profit))
            conn.commit()
        except Exception as e:
            self.logger.error(f'Erro ao salvar trade: {e}')

    def salvar_analise(self, par: str, analise: Dict):
        try:
            conn = self.get_db()
            c = conn.cursor()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            c.execute('''
                INSERT INTO analises (
                    par, timestamp, score_total, tendencia_macro, 
                    direcao_4h, padrao_detectado, sinal_compra
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                par, timestamp,
                analise.get('score_final', 0),
                analise.get('tendencia_macro', 'NEUTRA'),
                analise.get('direcao_4h', 'NEUTRA'),
                analise.get('motivo_compra', ''),
                1 if analise.get('sinal_compra', False) else 0
            ))
            conn.commit()
        except Exception as e:
            self.logger.error(f'Erro ao salvar analise: {e}')

    def salvar_estado(self, estado: Estado):
        try:
            cursor = self.db.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO estado (
                    par, ativo, posicao, preco_compra, ativo_comprado,
                    capital_operacional, preco_maximo, ultima_atualizacao
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                estado.par,
                estado.ativo,
                1 if estado.posicao_aberta else 0,
                estado.preco_compra,
                estado.ativo_comprado,
                estado.capital_operacional,
                estado.preco_maximo,
                datetime.now().isoformat()
            ))
            self.db.commit()
        except Exception as e:
            self.logger.error(f'Erro ao salvar estado: {e}')
            self.db.rollback()

    def registrar_operacao(self, estado: Estado, tipo: str, preco: float, quantidade: float, valor_total: float, motivo: str = None):
        try:
            cursor = self.db.cursor()
            cursor.execute('''
                INSERT INTO operacoes (
                    par, ativo, tipo, preco, quantidade, valor_total, data_hora, motivo
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                estado.par,
                estado.ativo,
                tipo,
                preco,
                quantidade,
                valor_total,
                datetime.now().isoformat(),
                motivo
            ))
            self.db.commit()
        except Exception as e:
            self.logger.error(f'Erro ao registrar operacao: {e}')
            self.db.rollback()

    def carregar_estados(self):
        try:
            cursor = self.db.cursor()
            for par in self.gerenciador.estados.keys():
                cursor.execute('SELECT * FROM estado WHERE par = ?', (par,))
                row = cursor.fetchone()
                if row:
                    estado = self.gerenciador.estados[par]
                    estado.ativo = row[1]
                    estado.posicao_aberta = bool(row[2])
                    estado.preco_compra = row[3]
                    estado.ativo_comprado = row[4]
                    estado.capital_operacional = max(row[5], 0)
                    estado.preco_maximo = row[6]
                    estado.ultima_atualizacao = row[7]
                else:
                    self.salvar_estado(self.gerenciador.estados[par])
                    
        except Exception as e:
            self.logger.error(f'Erro ao carregar estados: {e}')

    def verificar_saldos_iniciais(self):
        for estado in self.gerenciador.estados.values():
            try:
                if estado.posicao_aberta:
                    balance = self.cliente.get_asset_balance(asset=estado.ativo)
                    saldo_real = float(balance['free'])
                    
                    if saldo_real < Config.SALDO_MINIMO:
                        estado.posicao_aberta = False
                        estado.ativo_comprado = 0
                        estado.preco_compra = 0
                        estado.preco_maximo = 0
                    else:
                        estado.ativo_comprado = saldo_real
                    
                    self.salvar_estado(estado)
                    
                    open_orders = self.cliente.get_open_orders(symbol=estado.par)
                    for order in open_orders:
                        self.cliente.cancel_order(
                            symbol=estado.par,
                            orderId=order['orderId']
                        )
                        
            except Exception as e:
                self.logger.error(f'Erro ao verificar saldo de {estado.par}: {e}')

    def verificar_venda(self, estado: Estado, df_1h: pd.DataFrame) -> bool:
        try:
            if not estado.posicao_aberta:
                return False
                
            preco_atual = float(self.cliente.get_symbol_ticker(symbol=estado.par)['price'])
            
            if preco_atual > estado.preco_maximo:
                estado.preco_maximo = preco_atual
                self.salvar_estado(estado)
            
            lucro_atual = ((preco_atual / estado.preco_compra) - 1) * 100
            
            # ========== STOP LOSS DINÂMICO POR ATIVO ==========
            # Stops específicos para cada ativo (mais volátil = stop maior)
            stops_por_ativo = {
                'BTCBRL': 0.02,     # 2% - Bitcoin mais estável
                'ETHBRL': 0.025,    # 2.5%
                'SOLBRL': 0.03,     # 3%
                'BNBBRL': 0.025,    # 2.5%
                'LTCBRL': 0.03,     # 3%
                'DOGEBRL': 0.045,   # 4.5% - Dogecoin muito volátil
                'XRPBRL': 0.035,    # 3.5%
                'NEARBRL': 0.04,    # 4%
                'SANDBRL': 0.05,    # 5% - SAND muito volátil
                'ATOMBRL': 0.04,    # 4%
            }
            
            # Pega o stop específico do ativo ou usa 3% como padrão
            stop_percent = stops_por_ativo.get(estado.par, 0.03)
            stop_loss = estado.preco_maximo * (1 - stop_percent)
            
            if preco_atual <= stop_loss:
                self.logger.info(f"\n[STOP LOSS] {stop_percent*100:.0f}% atingido para {estado.par}")
                self.logger.info(f"   Preco maximo: {estado.preco_maximo:.2f}")
                self.logger.info(f"   Stop Loss: {stop_loss:.2f}")
                self.logger.info(f"   Lucro/Prejuizo: {lucro_atual:.2f}%")
                return True
            
            # ========== PROTEÇÃO DE LUCRO ==========
            if lucro_atual >= 8.0:
                queda_do_maximo = ((estado.preco_maximo - preco_atual) / estado.preco_maximo) * 100
                if queda_do_maximo >= 1.0:
                    self.logger.info(f"\n[PROTECAO LUCRO] para {estado.par}")
                    self.logger.info(f"   Lucro atual: {lucro_atual:.2f}%")
                    self.logger.info(f"   Queda do maximo: {queda_do_maximo:.2f}%")
                    return True
            
            # ========== QUEBRA DE ESTRUTURA ==========
            if len(df_1h) >= 50:
                fechamentos = df_1h['close'].astype(float)
                fundo_recente = fechamentos.tail(30).min()
                if preco_atual <= fundo_recente * 0.99:
                    self.logger.info(f"\n[VENDA] Rompimento de fundo anterior!")
                    self.logger.info(f"   Preco rompeu suporte: {preco_atual:.2f} <= {fundo_recente:.2f}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Erro ao verificar venda para {estado.par}: {e}")
            return False

    def comprar(self, estado: Estado, motivo: str = None):
        try:
            if estado.posicao_aberta:
                return
                
            account_info = self.cliente.get_account()
            brl_balance = next((b for b in account_info['balances'] if b['asset'] == 'BRL'), None)
            
            if not brl_balance or float(brl_balance['free']) < estado.capital_operacional:
                self.logger.warning(f"Saldo insuficiente para comprar {estado.par}")
                return
            
            ticker = float(self.cliente.get_symbol_ticker(symbol=estado.par)['price'])
            quantidade = estado.capital_operacional / ticker
            
            info = self.cache_symbol_info.get(estado.par)
            if not info:
                info = self.cliente.get_symbol_info(estado.par)
                self.cache_symbol_info[estado.par] = info
                
            step_size = float(next(f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'))
            quantidade = float(("{:." + str(len(str(step_size).split('.')[-1])) + "f}").format(quantidade - (quantidade % step_size)))
            
            if quantidade <= 0:
                return
            
            order = self.cliente.order_market_buy(symbol=estado.par, quantity=quantidade)
            
            if order['status'] == 'FILLED':
                estado.ativo_comprado = quantidade
                estado.preco_compra = ticker
                estado.preco_maximo = ticker
                estado.posicao_aberta = True
                
                self.registrar_operacao(estado, 'COMPRA', ticker, quantidade, estado.capital_operacional, motivo)
                
                self.logger.info(f"\n[COMPRA] {estado.par}")
                self.logger.info(f"   Quantidade: {quantidade:.8f} {estado.ativo}")
                self.logger.info(f"   Preco: {ticker:.2f} BRL")
                self.logger.info(f"   Motivo: {motivo}")
                
                self.salvar_estado(estado)
                
        except Exception as e:
            self.logger.error(f"Erro na compra para {estado.par}: {e}")

    def vender(self, estado: Estado, motivo: str = None):
        try:
            if not estado.posicao_aberta:
                return
            
            balance = self.cliente.get_asset_balance(asset=estado.ativo)
            saldo_real = float(balance['free'])
            
            if saldo_real < Config.SALDO_MINIMO:
                estado.posicao_aberta = False
                estado.ativo_comprado = 0
                self.salvar_estado(estado)
                return
            
            info = self.cache_symbol_info.get(estado.par)
            if not info:
                info = self.cliente.get_symbol_info(estado.par)
                self.cache_symbol_info[estado.par] = info
                
            step_size = float(next(f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'))
            quantidade = float(("{:." + str(len(str(step_size).split('.')[-1])) + "f}").format(saldo_real - (saldo_real % step_size)))
            
            if quantidade <= 0:
                return
            
            order = self.cliente.order_market_sell(symbol=estado.par, quantity=quantidade)
            
            if order['status'] == 'FILLED':
                valor_venda = float(order['cummulativeQuoteQty'])
                preco_medio = valor_venda / quantidade
                
                valor_compra = estado.ativo_comprado * estado.preco_compra
                lucro = valor_venda - valor_compra
                lucro_percentual = ((valor_venda / valor_compra) - 1) * 100
                
                estado.capital_operacional = max(0, estado.capital_operacional + lucro)
                
                self.registrar_operacao(estado, 'VENDA', preco_medio, quantidade, valor_venda, motivo)
                
                self.logger.info(f"\n[VENDA] {estado.par}")
                self.logger.info(f"   Quantidade: {quantidade:.8f} {estado.ativo}")
                self.logger.info(f"   Preco Medio: {preco_medio:.2f} BRL")
                self.logger.info(f"   Lucro/Prejuizo: {lucro_percentual:.2f}%")
                self.logger.info(f"   Motivo: {motivo}")
                
                estado.posicao_aberta = False
                estado.ativo_comprado = 0
                estado.preco_compra = 0
                estado.preco_maximo = 0
                
                self.salvar_estado(estado)
                
        except Exception as e:
            self.logger.error(f"Erro na venda para {estado.par}: {e}")

    def executar_analise_completa(self, estado: Estado) -> Dict:
        """Executa analise completa multi-timeframe com Estrutura de Mercado"""
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"[ANALISE] {estado.par}")
        self.logger.info(f"{'='*80}")
        
        # ========== 1. ANÁLISE MACRO (1D) ==========
        self.logger.info(f"\n[1D] 6 meses - Analise Tradicional")
        df_1d = self.analisador.buscar_dados(estado.par, Config.INTERVALO_1D, Config.LIMITE_1D)
        analise_tradicional = self.analisador.analisar_tendencia_macro_1d(df_1d)
        
        self.logger.info(f"   Tendencia: {analise_tradicional['tendencia']} (score: {analise_tradicional['score']:.1f})")
        self.logger.info(f"   RSI (info): {analise_tradicional['rsi']:.1f}")
        
        # ========== 1.1 ANÁLISE DE ESTRUTURA DE MERCADO (1D) ==========
        self.logger.info(f"\n[1D] ESTRUTURA DE MERCADO (Topos e Fundos)")
        analise_estrutura = self.analisador.analisar_estrutura_mercado_1d(df_1d)
        
        score_estrutura = 50
        tendencia_estrutural = "NEUTRA"
        topos_lista = ""
        fundos_lista = ""
        topos_altos = False
        fundos_altos = False
        forca_tendencia = 0
        estrutura_quebrada = False
        
        if not analise_estrutura.get('erro', False):
            score_estrutura = analise_estrutura['score']
            tendencia_estrutural = analise_estrutura['tendencia_macro']
            forca_tendencia = analise_estrutura.get('forca_tendencia', 0)
            estrutura_quebrada = analise_estrutura.get('estrutura_quebrada', False)
            topos_altos = analise_estrutura.get('topos_altos', False)
            fundos_altos = analise_estrutura.get('fundos_altos', False)
            
            # Formata listas de topos e fundos para salvar
            if analise_estrutura.get('topos'):
                topos_lista = ', '.join([f"{t['preco']:.2f}" for t in analise_estrutura['topos'][-5:]])
            if analise_estrutura.get('fundos'):
                fundos_lista = ', '.join([f"{f['preco']:.2f}" for f in analise_estrutura['fundos'][-5:]])
            
            self.logger.info(f"   Tendencia Estrutural: {analise_estrutura['tendencia_estrutural']} (score: {score_estrutura:.1f})")
            self.logger.info(f"   Forca da Estrutura: {forca_tendencia:.0f}%")
            self.logger.info(f"   Ultimo Topo: {analise_estrutura['ultimo_topo']:.2f} | Ultimo Fundo: {analise_estrutura['ultimo_fundo']:.2f}")
        
        # ========== COMBINA AS ANÁLISES 1D ==========
        score_combinado_1d = (
            analise_tradicional['score'] * Config.PESO_TRADICIONAL_1D +
            score_estrutura * Config.PESO_ESTRUTURA_1D
        )
        
        # A tendência combinada prioriza a estrutura se ela for forte (>70%)
        if not analise_estrutura.get('erro', False) and forca_tendencia >= 70:
            tendencia_combinada = tendencia_estrutural
        else:
            tendencia_combinada = analise_tradicional['tendencia']
        
        self.logger.info(f"\n   Score Combinado 1D: {score_combinado_1d:.1f}")
        self.logger.info(f"   ({Config.PESO_TRADICIONAL_1D:.0%} tradicional + {Config.PESO_ESTRUTURA_1D:.0%} estrutura)")
        
        # Topos e fundos históricos
        topos_fundos = analise_tradicional.get('topos_fundos', {})
        self.logger.info(f"   Maximo 90d: {topos_fundos.get('maximo_90d', 0):.2f} | Minimo 90d: {topos_fundos.get('minimo_90d', 0):.2f}")
        
        # ========== 2. ANÁLISE DE DIREÇÃO (4H) ==========
        self.logger.info(f"\n[4H] Direcao do Ativo")
        df_4h = self.analisador.buscar_dados(estado.par, Config.INTERVALO_4H, Config.LIMITE_4H)
        analise_4h = self.analisador.analisar_direcao_4h(df_4h)
        
        self.logger.info(f"   Direcao: {analise_4h['direcao']} (score: {analise_4h['score']:.1f})")
        self.logger.info(f"   RSI (info): {analise_4h['rsi']:.1f}")
        self.logger.info(f"   Suporte: {analise_4h['suporte']:.2f} | Resistencia: {analise_4h['resistencia']:.2f}")
        
        # ========== 3. ANÁLISE DE PADRÕES (1H) ==========
        self.logger.info(f"\n[1H] Padroes Graficos")
        df_1h = self.analisador.buscar_dados(estado.par, Config.INTERVALO_1H, Config.LIMITE_1H)
        analise_padroes = self.analisador.analisar_padroes_1h(df_1h, {'tendencia': tendencia_combinada}, analise_4h)
        
        self.logger.info(f"   Score Padroes: {analise_padroes['score']:.1f}")
        
        if analise_padroes['padroes']:
            for padrao in analise_padroes['padroes']:
                self.logger.info(f"   - Figura detectada: {padrao['tipo']} ({padrao['direcao']})")
        
        # ========== 4. ANÁLISE DE ENTRADA (15M) ==========
        self.logger.info(f"\n[15M] Ponto de Entrada")
        df_15m = self.analisador.buscar_dados(estado.par, Config.INTERVALO_15M, Config.LIMITE_15M)
        analise_entrada = self.analisador.analisar_entrada_15m(df_15m, tendencia_combinada, analise_padroes)
        
        self.logger.info(f"   Score Entrada: {analise_entrada['score']:.1f}")
        self.logger.info(f"   RSI (info): {analise_entrada['rsi']:.1f}")
        
        # ========== 5. CÁLCULO DO SCORE FINAL ==========
        score_final = (
            score_combinado_1d * Config.PESO_TENDENCIA_MACRO +
            analise_4h['score'] * Config.PESO_DIRECAO_4H +
            analise_padroes['score'] * Config.PESO_PADROES +
            analise_entrada['score'] * Config.PESO_ENTRADA_15M
        )
        
        sinal_compra = (
            tendencia_combinada in ['ALTA', 'ALTA_FORTE'] and
            analise_4h['direcao'] in ['ALTA', 'ALTA_FORTE'] and
            (analise_padroes['sinal_compra'] or analise_entrada['sinal_entrada']) and
            score_final >= Config.SCORE_MINIMO_COMPRA
        )
        
        motivo_compra = ""
        if sinal_compra:
            if analise_padroes['sinal_compra']:
                motivo_compra = analise_padroes['motivo_compra']
            elif analise_entrada['sinal_entrada']:
                motivo_compra = analise_entrada['motivo']
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"[RESULTADO] Score Final: {score_final:.1f}/100 (minimo: {Config.SCORE_MINIMO_COMPRA})")
        self.logger.info(f"   1D Combinado: {score_combinado_1d:.1f} (peso {Config.PESO_TENDENCIA_MACRO:.0%})")
        self.logger.info(f"   4H: {analise_4h['direcao']} (score: {analise_4h['score']:.1f}, peso {Config.PESO_DIRECAO_4H:.0%})")
        self.logger.info(f"   1H Padroes: {analise_padroes['score']:.1f} (peso {Config.PESO_PADROES:.0%})")
        self.logger.info(f"   15M Entrada: {analise_entrada['score']:.1f} (peso {Config.PESO_ENTRADA_15M:.0%})")
        
        if sinal_compra:
            self.logger.info(f"\n   SINAL DE COMPRA CONFIRMADO!")
            self.logger.info(f"   Motivo: {motivo_compra}")
        else:
            self.logger.info(f"\n   Sem sinal de compra")
            if tendencia_combinada not in ['ALTA', 'ALTA_FORTE']:
                self.logger.info(f"   - Tendencia macro desfavoravel: {tendencia_combinada}")
            if analise_4h['direcao'] not in ['ALTA', 'ALTA_FORTE']:
                self.logger.info(f"   - Direcao 4H desfavoravel: {analise_4h['direcao']}")
            if score_final < Config.SCORE_MINIMO_COMPRA:
                self.logger.info(f"   - Score final abaixo do minimo")
        
        # ========== 6. SALVAR ESTRUTURA DE MERCADO NO BANCO ==========
        try:
            # Usar get_db() que é thread-safe
            conn = self.get_db()
            cursor = conn.cursor()
            
            # Criar tabela de estrutura se não existir
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS estrutura_mercado (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    par TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    topos TEXT,
                    fundos TEXT,
                    topos_altos INTEGER,
                    fundos_altos INTEGER,
                    tendencia_estrutural TEXT,
                    forca_tendencia REAL,
                    estrutura_quebrada INTEGER,
                    score_estrutura REAL
                )
            ''')
            
            # Inserir dados
            cursor.execute('''
                INSERT INTO estrutura_mercado (
                    par, timestamp, topos, fundos, topos_altos, 
                    fundos_altos, tendencia_estrutural, forca_tendencia, 
                    estrutura_quebrada, score_estrutura
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                estado.par,
                datetime.now().isoformat(),
                topos_lista,
                fundos_lista,
                1 if topos_altos else 0,
                1 if fundos_altos else 0,
                tendencia_estrutural,
                forca_tendencia,
                1 if estrutura_quebrada else 0,
                score_estrutura
            ))
            conn.commit()
            
            self.logger.info(f"✅ Estrutura de mercado salva para {estado.par}")
            
        except Exception as e:
            self.logger.error(f"Erro ao salvar estrutura de mercado: {e}")
        
        return {
            'score_final': score_final,
            'sinal_compra': sinal_compra,
            'motivo_compra': motivo_compra,
            'tendencia_macro': tendencia_combinada,
            'direcao_4h': analise_4h['direcao'],
            'score_padroes': analise_padroes['score'],
            'score_entrada': analise_entrada['score'],
            'score_estrutura': score_estrutura,
            'tendencia_estrutural': tendencia_estrutural,
            'estrutura_quebrada': estrutura_quebrada,
            'forca_tendencia': forca_tendencia,
            'topos': topos_lista,
            'fundos': fundos_lista,
            'topos_altos': topos_altos,
            'fundos_altos': fundos_altos,
            'topos_fundos': topos_fundos,
            'padroes_detectados': [p['tipo'] for p in analise_padroes['padroes']]
        }
    
    def executar_estrategia(self, estado: Estado):
        try:
            if estado.posicao_aberta:
                df_1h = self.analisador.buscar_dados(estado.par, Config.INTERVALO_1H, Config.LIMITE_1H)
                if self.verificar_venda(estado, df_1h):
                    self.vender(estado, "Stop Loss ou Protecao de Lucro")
                return
            
            analise = self.executar_analise_completa(estado)
            self.salvar_analise(estado.par, analise)
            
            if analise['sinal_compra']:
                self.comprar(estado, analise['motivo_compra'])
            
        except Exception as e:
            self.logger.error(f'Erro na estrategia para {estado.par}: {e}')

    def run(self):
        self.logger.info("Bot trading iniciado com analise multi-timeframe!")
        self.logger.info("   Timeframes: 1D (macro) -> 4H (direcao) -> 1H (padroes) -> 15M (entrada)")
        
        while self.running:
            try:
                estados = list(self.gerenciador.estados.values())
                for i in range(0, len(estados), Config.BATCH_SIZE):
                    batch = estados[i:i + Config.BATCH_SIZE]
                    for estado in batch:
                        self.executar_estrategia(estado)
                
                time.sleep(Config.INTERVALO_ATUALIZACAO)
                
            except Exception as e:
                self.logger.error(f'Erro durante execucao: {e}')
                time.sleep(30)

    def start(self):
        if not self.running:
            self.logger.info("Iniciando bot trading...")
            self.running = True
            self.thread = threading.Thread(target=self.run)
            self.thread.start()
            return True
        return False

    def stop(self):
        if self.running:
            self.logger.info("Parando bot trading...")
            self.running = False
            if self.thread:
                self.thread.join()
                self.thread = None
            return True
        return False

    def __del__(self):
        if hasattr(self, 'db'):
            self.db.close()


# ===================================================================
# EXECUÇÃO PRINCIPAL
# ===================================================================

if __name__ == '__main__':
    bot = BotTrading()
    try:
        bot.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        bot.logger.info('\nEncerrando bot de trading...')
        bot.stop()
        if hasattr(bot, 'db'):
            bot.db.close()
    except Exception as e:
        bot.logger.error(f'Erro fatal: {e}')
        if hasattr(bot, 'db'):
            bot.db.close()