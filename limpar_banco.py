import sqlite3
import os

DB_PATH = os.path.join('data', 'trades.db')

# Lista de pares que você QUER manter (iguais ao seu Config)
PARES_ATUAIS = [
    'BTCBRL', 'ETHBRL', 'SOLBRL', 'BNBBRL', 'LTCBRL',
    'DOGEBRL', 'XRPBRL', 'NEARBRL', 'SANDBRL', 'ATOMBRL'
]

def limpar_estados_antigos():
    if not os.path.exists(DB_PATH):
        print(f"Banco de dados não encontrado: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Ver pares existentes
    cursor.execute("SELECT par, capital_operacional FROM estado")
    pares_existentes = cursor.fetchall()
    
    print("=" * 50)
    print("Pares ANTES da limpeza:")
    print("=" * 50)
    for par, capital in pares_existentes:
        print(f"  {par}: R$ {capital:.2f}")
    
    # Remover pares que não estão na lista atual
    print("\n" + "=" * 50)
    print("Removendo pares antigos...")
    print("=" * 50)
    for par, _ in pares_existentes:
        if par not in PARES_ATUAIS:
            print(f"  ❌ Removendo: {par}")
            cursor.execute("DELETE FROM estado WHERE par = ?", (par,))
            cursor.execute("DELETE FROM analises WHERE par = ?", (par,))
    
    # Atualizar capital dos pares atuais para R$ 200
    print("\n" + "=" * 50)
    print("Atualizando capitais...")
    print("=" * 50)
    for par in PARES_ATUAIS:
        cursor.execute("""
            UPDATE estado 
            SET capital_operacional = 200.00
            WHERE par = ?
        """, (par,))
        print(f"  ✅ {par}: R$ 200.00")
    
    conn.commit()
    
    # Verificar resultado
    cursor.execute("SELECT par, capital_operacional FROM estado ORDER BY par")
    resultados = cursor.fetchall()
    
    print("\n" + "=" * 50)
    print("Pares DEPOIS da limpeza:")
    print("=" * 50)
    total = 0
    for par, capital in resultados:
        print(f"  {par}: R$ {capital:.2f}")
        total += capital
    
    print(f"\n  TOTAL: R$ {total:.2f}")
    
    conn.close()
    print("\n✅ Limpeza concluída!")

if __name__ == "__main__":
    limpar_estados_antigos()