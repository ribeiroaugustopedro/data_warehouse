# Warehouse - Analytics Engineering Infrastructure

Repositório central de **Engenharia de Dados & ELT** para processamento e modelagem da rede de saúde. Este warehouse utiliza o **DuckDB** como motor de banco de dados e o **dbt (data build tool)** para transformações analíticas escaláveis.

---

## 🏗️ Arquitetura de Camadas (Medallion)
- **Bronze (Extract)**: Ingestão de dados brutos de beneficiários e prestadores (CSV/Excel/SaaS APIs).
- **Silver (Transform)**: Limpeza de dados, padronização de geocodificação e anonimização de informações sensíveis.
- **Gold (Load)**: Visualizações otimizadas ("Materialized Views") que alimentam as aplicações de front-end, como o **GeoMap SaaS**.

## 🛠️ Tech Stack
- **Banco de Dados**: DuckDB (In-process OLAP database).
- **Modelagem**: dbt-duckdb.
- **Linguagem**: Python & SQL.
- **Geração de Mock Data**: Faker & Custom Python scripts.

## 🚀 Como Executar o Processamento
1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Execute o dbt para materializar as tabelas Gold:
   ```bash
   cd dbt
   dbt run
   ```

## 📂 Estrutura de Pastas
- `/data`: Arquivos raw e landing zone.
- `/duckdb`: Database persistido (`warehouse.db`).
- `/dbt`: Modelos SQL e arquivos de configuração do dbt.
- `/generator`: Scripts Python para simular dados de alta fidelidade.

---
> [!NOTE]
> Este warehouse foi projetado para portabilidade total e performance analítica extrema em bases de dados residenciais e corporativas.
