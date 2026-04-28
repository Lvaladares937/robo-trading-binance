import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join('data', 'trades.db')

# Dados REAIS baseados nos seus logs
# (Peguei dos logs que você mostrou anteriormente)
dados_estrutura = [
    # (par, topos, fundos, topos_altos, fundos_altos, tendencia, forca, quebrada)
    ('BTCBRL', '371246, 386610, 398250, 373682', '317250, 339187, 323825, 338378, 355121', 0, 0, 'NEUTRA', 30, 1),
    ('ETHBRL', '11004.79, 11478.06, 12487.30, 11250.33, 12289.07', '9247.06, 9860.00, 9324.31, 10174.39, 11251.12', 0, 0, 'NEUTRA', 30, 0),
    ('SOLBRL', '490.60, 511.00, 488.50, 446.80, 451.90', '357.80, 399.10, 385.00, 424.20, 396.70', 0, 0, 'NEUTRA', 30, 0),
    ('BNBBRL', '3356.00, 3485.00, 3641.00, 3209.00, 3248.00', '2999.00, 2987.00, 3216.00, 2948.00, 2964.00', 0, 0, 'NEUTRA', 30, 0),
    ('LTCBRL', '300.50, 300.90, 309.80, 286.40, 285.00', '239.90, 260.00, 274.30, 266.10, 264.90', 0, 0, 'NEUTRA', 30, 0),
    ('DOGEBRL', '0.54, 0.55, 0.51, 0.49, 0.51', '0.42, 0.45, 0.46, 0.46, 0.45', 0, 0, 'NEUTRA', 30, 0),
    ('XRPBRL', '7.63, 7.67, 8.40, 7.16, 7.50', '5.92, 6.54, 6.62, 6.64', 0, 1, 'NEUTRA', 30, 0),
    ('NEARBRL', '7.47, 7.47, 7.90, 7.17, 7.16', '4.87, 4.95, 6.25, 5.95, 6.63', 0, 0, 'NEUTRA', 30, 0),
    ('SANDBRL', '2.23, 2.33, 2.22, 2.10, 2.02', '2.39, 1.65, 1.89, 1.97, 1.92', 0, 0, 'NEUTRA', 30, 1),
    ('ATOMBRL', '88.30, 63.70, 62.76, 68.16, 71.90', '49.52, 44.30, 42.90, 45.00, 45.00', 0, 0, 'NEUTRA', 30, 0),
]

def popular_tabela():
    if not os.path.exists(DB_PATH):
        print(f"❌ Banco de dados não encontrado: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Verificar se a tabela existe, se não, criar
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
    
    # Limpar dados antigos
    cursor.execute("DELETE FROM estrutura_mercado")
    
    # Inserir novos dados
    for dados in dados_estrutura:
        cursor.execute('''
            INSERT INTO estrutura_mercado (
                par, timestamp, topos, fundos, topos_altos, 
                fundos_altos, tendencia_estrutural, forca_tendencia, 
                estrutura_quebrada, score_estrutura
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            dados[0],                      # par
            datetime.now().isoformat(),    # timestamp
            dados[1],                      # topos
            dados[2],                      # fundos
            dados[3],                      # topos_altos
            dados[4],                      # fundos_altos
            dados[5],                      # tendencia_estrutural
            dados[6],                      # forca_tendencia
            dados[7],                      # estrutura_quebrada
            50                             # score_estrutura (padrão)
        ))
    
    conn.commit()
    
    # Verificar quantos foram inseridos
    cursor.execute("SELECT COUNT(*) FROM estrutura_mercado")
    count = cursor.fetchone()[0]
    
    print(f"✅ {count} registros inseridos na tabela estrutura_mercado!")
    
    # Mostrar os registros
    cursor.execute("SELECT par, topos, fundos, tendencia_estrutural FROM estrutura_mercado")
    for row in cursor.fetchall():
        print(f"   📊 {row[0]}: {row[3]} | Topos: {row[1][:30]}...")
    
    conn.close()

if __name__ == "__main__":
    popular_tabela()