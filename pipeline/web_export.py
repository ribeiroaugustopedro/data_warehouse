import duckdb
import os

# Configuração de caminhos relativos ao diretório pipeline
DB_PATH = os.path.join('..', 'duckdb', 'warehouse.db')
OUTPUT_DIR = os.path.join('..', '..', 'portfolio_website', 'public', 'data')

def export_to_web():
    if not os.path.exists(DB_PATH):
        print(f"Erro: Arquivo {DB_PATH} não encontrado.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Conectando ao banco em {DB_PATH}...")
    con = duckdb.connect(DB_PATH)
    
    print("Exportando tabelas para o site (Formato Parquet)...")
    
    # Exporta Golden Zone para o site
    tables = [('gold', 'providers'), ('gold', 'users')]
    
    for schema, table in tables:
        output_path = os.path.join(OUTPUT_DIR, f"{table}.parquet")
        print(f" -> Exportando {schema}.{table} para {output_path}...")
        con.execute(f"COPY (SELECT * FROM {schema}.{table}) TO '{output_path}' (FORMAT PARQUET)")
    
    con.close()
    print("Pipeline de exportação concluído com sucesso!")

if __name__ == "__main__":
    export_to_web()
