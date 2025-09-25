# Projeto ETL Neotass com Airflow e Docker Compose

Este projeto realiza o processo ETL (Extract, Transform, Load) de dados de oportunidades e vendas (sellout) utilizando Apache Airflow, pandas e SQLAlchemy, armazenando os dados em um data warehouse SQLite.

## Estrutura do Projeto

- **airflow/dags/dag_etl_neotass.py**: DAG principal do Airflow que executa as etapas de ETL.
- **airflow/dags/data_warehouse/models.py**: Modelos ORM SQLAlchemy para as tabelas do data warehouse.
- **airflow/dags/data_warehouse/**: Diretório onde os arquivos CSV gerados pelo ETL são salvos.
- **database/**: Diretório esperado para os arquivos de entrada (`registros_oportunidades.json` e `sellout.parquet`).

## Pré-requisitos

- Docker (recomendado para rodar o Airflow)
- Python 3.8+
- Apache Airflow
- pandas, pyarrow, SQLAlchemy

## Guia Passo a Passo

### 1. Clonar o repositório

```bash
git clone https://github.com/jonathanbrito48/teste_neotass.git
cd teste_neotass
```

### 2. Configurar o ambiente

Recomenda-se usar Docker Compose para rodar o Airflow. Certifique-se de que o arquivo `.env` está presente em `airflow/.env` com o conteúdo:

```
AIRFLOW_UID=1000
```

#### **(Opcional) Ambiente Virtual Python**

Se preferir rodar localmente (fora do Docker), recomenda-se criar um ambiente virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Preparar os arquivos de entrada

Coloque os arquivos:
- `registros_oportunidades.json`
- `sellout.parquet`

no diretório `/opt/airflow/database` dentro do container ou ajuste os caminhos no código conforme necessário.

### 4. Instalar dependências

Instale as dependências usando o arquivo `requirements.txt`:

```bash
pip install -r requirements.txt
```

### 5. Inicializar o Airflow com Docker Compose

Se estiver usando Docker Compose, siga os passos abaixo:

1. **Construa as imagens e inicialize os serviços necessários:**

   ```bash
   cd airflow
   docker-compose up -d airflow-init
   ```

2. **Inicie os containers do banco de dados e do scheduler/webserver:**

   ```bash
   docker-compose up -d --build
   ```

3. **(Opcional) Para visualizar os logs ou depurar:**

   ```bash
   docker-compose logs -f
   ```

Acesse o Airflow Web UI em `http://localhost:8080`.

**Login padrão do Airflow:**
- Usuário: `airflow`
- Senha: `airflow`

Caso tenha alterado o usuário/senha no seu `docker-compose` ou variáveis de ambiente, utilize as credenciais configuradas.

### 6. Executar a DAG

No Airflow Web UI, habilite e execute a DAG chamada `dag_etl_neotass`.

### 7. Verificar os resultados

Os arquivos CSV gerados estarão em `airflow/dags/data_warehouse/`:
- `dim_parceiro.csv`
- `dim_produto.csv`
- `fato_registro_oportunidade.csv`
- `fato_sellout.csv`

**Banco de Dados Integrado:**  
Os dados transformados são carregados em um banco de dados SQLite localizado em:

```
airflow/dags/data_warehouse/data_warehouse.db
```

Esse arquivo representa o Data Warehouse (DW) do projeto.

### 8. Validar e Visualizar o Data Warehouse

Você pode validar e explorar os dados carregados no banco SQLite utilizando ferramentas como:

- **DBeaver**: Ferramenta gratuita e multiplataforma para explorar bancos de dados. Basta abrir o arquivo `data_warehouse.db` como um banco SQLite.
- **VS Code**: Com a extensão "SQLite" instalada, você pode abrir e consultar o banco diretamente pelo editor.
- **CLI**: Usando o comando abaixo para acessar o banco via terminal:
  ```bash
  sqlite3 airflow/dags/data_warehouse/data_warehouse.db
  ```
  E então executar comandos SQL, por exemplo:
  ```sql
  SELECT * FROM dim_parceiro LIMIT 10;
  ```

## Observações

- O projeto utiliza chunking para inserção eficiente dos dados no banco.
- Os modelos ORM estão definidos em `models.py` e refletem as dimensões e fatos do data warehouse.
- Ajuste os caminhos dos arquivos conforme o ambiente de execução (local ou container).

## Dúvidas

Em caso de dúvidas, consulte os arquivos do projeto ou abra uma issue.