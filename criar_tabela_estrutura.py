# Crie um arquivo criar_tabela_estrutura.py
import sqlite3
import os

DB_PATH = os.path.join('data', 'trades.db')

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

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

conn.commit()
print("✅ Tabela 'estrutura_mercado' criada com sucesso!")

# Verificar
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("Tabelas existentes:", [row[0] for row in cursor.fetchall()])

conn.close()