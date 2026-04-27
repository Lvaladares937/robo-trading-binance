from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import sqlite3
import json
from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'trades.db')

# Configuração do cliente Binance
KEY_BINANCE = os.getenv('KEY_BINANCE')
SECRET_BINANCE = os.getenv('SECRET_BINANCE')

if not KEY_BINANCE or not SECRET_BINANCE:
    print("ERRO: Chaves API não encontradas no arquivo .env")
    print("Verifique se o arquivo .env existe e contém KEY_BINANCE e SECRET_BINANCE")
    cliente = None
else:
    try:
        cliente = Client(KEY_BINANCE, SECRET_BINANCE)
        print("✅ Conectado à Binance com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao conectar à Binance: {e}")
        cliente = None

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/api/grafico/<par>')
def get_grafico_dados(par):
    """Retorna dados para o gráfico do ativo"""
    
    if cliente is None:
        return jsonify({'erro': 'API não conectada', 'dados': [], 'indicadores': {}}), 200
    
    intervalo = request.args.get('intervalo', '1h')
    limit = request.args.get('limit', 100, type=int)
    
    interval_map = {
        '15m': Client.KLINE_INTERVAL_15MINUTE,
        '1h': Client.KLINE_INTERVAL_1HOUR,
        '4h': Client.KLINE_INTERVAL_4HOUR,
        '1d': Client.KLINE_INTERVAL_1DAY
    }
    
    try:
        # Buscar dados da Binance
        klines = cliente.get_klines(
            symbol=par,
            interval=interval_map.get(intervalo, Client.KLINE_INTERVAL_1HOUR),
            limit=limit
        )
        
        if not klines:
            return jsonify({'erro': f'Sem dados para {par}', 'dados': [], 'indicadores': {}}), 200
        
        dados = []
        for k in klines:
            dados.append({
                'time': k[0],
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5])
            })
        
        # Calcular indicadores
        df = pd.DataFrame(dados)
        
        # Converter para float
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['open'] = df['open'].astype(float)
        
        # Médias Móveis - tratar NaN
        df['ma7'] = df['close'].rolling(window=7).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma50'] = df['close'].rolling(window=50).mean()
        df['ma200'] = df['close'].rolling(window=200).mean()
        
        # Bandas de Bollinger
        df['bb_middle'] = df['close'].rolling(window=20).mean()
        df['bb_std'] = df['close'].rolling(window=20).std()
        df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * 2)
        df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * 2)
        
        # RSI
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Substituir NaN por None para JSON
        def clean_series(series):
            return [None if pd.isna(x) else float(x) for x in series.tolist()]
        
        # Suporte e Resistência (níveis de 30 dias)
        ultimos_30 = df.tail(30)
        suporte = float(ultimos_30['low'].min()) if not ultimos_30.empty else 0
        resistencia = float(ultimos_30['high'].max()) if not ultimos_30.empty else 0
        
        # Preço atual
        preco_atual = float(df['close'].iloc[-1]) if not df.empty else 0
        
        return jsonify({
            'dados': dados,
            'indicadores': {
                'ma7': clean_series(df['ma7']) if 'ma7' in df else [],
                'ma20': clean_series(df['ma20']) if 'ma20' in df else [],
                'ma50': clean_series(df['ma50']) if 'ma50' in df else [],
                'ma200': clean_series(df['ma200']) if 'ma200' in df else [],
                'bb_upper': clean_series(df['bb_upper']) if 'bb_upper' in df else [],
                'bb_lower': clean_series(df['bb_lower']) if 'bb_lower' in df else [],
                'rsi': clean_series(df['rsi']) if 'rsi' in df else []
            },
            'suporte': suporte,
            'resistencia': resistencia,
            'preco_atual': preco_atual
        })
        
    except BinanceAPIException as e:
        print(f"Erro Binance API: {e}")
        return jsonify({'erro': f'Erro na API: {str(e)}', 'dados': [], 'indicadores': {}}), 200
    except Exception as e:
        print(f"Erro geral: {e}")
        return jsonify({'erro': str(e), 'dados': [], 'indicadores': {}}), 200

@app.route('/api/estados')
def get_estados():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM estado')
    estados = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(estados)

@app.route('/api/operacoes')
def get_operacoes():
    limit = request.args.get('limit', 50, type=int)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM operacoes ORDER BY id DESC LIMIT ?', (limit,))
    operacoes = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(operacoes)

@app.route('/api/analises')
def get_analises():
    limit = request.args.get('limit', 50, type=int)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM analises ORDER BY id DESC LIMIT ?', (limit,))
    analises = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(analises)

@app.route('/api/resumo')
def get_resumo():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) as total, tipo FROM operacoes GROUP BY tipo')
    operacoes = {row['tipo']: row['total'] for row in cursor.fetchall()}
    
    ultimas_24h = (datetime.now() - timedelta(hours=24)).isoformat()
    cursor.execute('SELECT COUNT(*) as total FROM operacoes WHERE data_hora > ?', (ultimas_24h,))
    operacoes_24h = cursor.fetchone()['total']
    
    cursor.execute('SELECT SUM(capital_operacional) as total FROM estado')
    capital_total = cursor.fetchone()['total'] or 0
    
    cursor.execute('SELECT COUNT(*) as total FROM estado WHERE posicao = 1')
    posicoes_abertas = cursor.fetchone()['total']
    
    conn.close()
    
    return jsonify({
        'total_compras': operacoes.get('COMPRA', 0),
        'total_vendas': operacoes.get('VENDA', 0),
        'operacoes_24h': operacoes_24h,
        'capital_total': capital_total,
        'posicoes_abertas': posicoes_abertas,
        'ultima_atualizacao': datetime.now().isoformat()
    })

@app.route('/api/performance')
def get_performance():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            par,
            COUNT(CASE WHEN tipo = 'COMPRA' THEN 1 END) as compras,
            COUNT(CASE WHEN tipo = 'VENDA' THEN 1 END) as vendas,
            SUM(CASE WHEN tipo = 'COMPRA' THEN valor_total ELSE 0 END) as total_comprado,
            SUM(CASE WHEN tipo = 'VENDA' THEN valor_total ELSE 0 END) as total_vendido
        FROM operacoes
        GROUP BY par
    ''')
    
    performance = []
    for row in cursor.fetchall():
        lucro = (row['total_vendido'] or 0) - (row['total_comprado'] or 0)
        performance.append({
            'par': row['par'],
            'compras': row['compras'],
            'vendas': row['vendas'],
            'total_comprado': row['total_comprado'] or 0,
            'total_vendido': row['total_vendido'] or 0,
            'lucro': lucro,
            'lucro_percentual': (lucro / (row['total_comprado'] or 1)) * 100 if row['total_comprado'] else 0
        })
    
    conn.close()
    return jsonify(performance)

if __name__ == '__main__':
    print("=" * 50)
    print("🚀 Dashboard do Robô Trader")
    print("=" * 50)
    print(f"📁 Banco de dados: {DB_PATH}")
    
    if cliente:
        print("✅ Binance API: Conectada")
    else:
        print("❌ Binance API: Não conectada - verifique o arquivo .env")
    
    print(f"🌐 Acesse: http://localhost:5000")
    print("=" * 50)
    
    app.run(host='0.0.0.0', port=5000, debug=True)