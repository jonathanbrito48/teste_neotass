from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_etl_neotass',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def extract():
        import pandas as pd
        import pyarrow.parquet as pq

        data_oportunidades = pd.read_json('/opt/airflow/database/registros_oportunidades.json')

        data_sellout = pq.read_table('/opt/airflow/database/sellout.parquet')
        data_sellout = data_sellout.to_pandas()
        return data_oportunidades, data_sellout


    def transform():
        import pandas as pd
        from datetime import datetime as dt

        data_oportunidades, data_sellout = extract()
        
        #Dim Cliente
        dim_parceiro = data_oportunidades[['Nome Fantasia','CNPJ Parceiro','Telefone Parceiro']].copy()
        dim_parceiro['id_parceiro'] = dim_parceiro['CNPJ Parceiro'].str.replace(r'\D', '', regex=True)
        dim_parceiro.drop_duplicates(subset=['id_parceiro'], keep='first', inplace=True)
        dim_parceiro.rename(columns={
            'Nome Fantasia': 'nome_fantasia',
            'CNPJ Parceiro': 'cnpj_parceiro',
            'Telefone Parceiro': 'telefone_parceiro'
        }, inplace=True)
        dim_parceiro.reset_index(drop=True, inplace=True)

        #Dim Produto
        dim_produto = data_oportunidades[['Nome_Produto','Categoria produto','Valor_Unitario']].copy()
        dim_produto = dim_produto.drop_duplicates().reset_index(drop=True)
        dim_produto['id_produto'] = dim_produto.index + 1
        dim_produto['id_produto'] = dim_produto['id_produto'].astype(str)
        dim_produto.rename(columns={'Nome_Produto':'nome_produto',
                                    'Categoria produto':'categoria_produto',
                                    'Valor_Unitario':'valor_unitario'}, inplace=True)
        dim_produto = dim_produto[['id_produto','nome_produto','categoria_produto','valor_unitario']]

        # Fato Oportunidade
        fato_registro_oportunidade = data_oportunidades.copy()
        fato_registro_oportunidade = fato_registro_oportunidade.merge(dim_produto, how='left', left_on='Nome_Produto', right_on='nome_produto')
        fato_registro_oportunidade.reset_index(drop=True, inplace=True)
        fato_registro_oportunidade['id_oportunidade'] = fato_registro_oportunidade.index + 1
        fato_registro_oportunidade['id_oportunidade'] = fato_registro_oportunidade['id_oportunidade'].astype(str)
        fato_registro_oportunidade.rename(columns={'Quantidade':'quantidade',
                                                    'Status':'status'}, inplace=True)
        fato_registro_oportunidade['id_parceiro'] = fato_registro_oportunidade['CNPJ Parceiro'].str.replace(r'\D', '', regex=True)
        fato_registro_oportunidade['valor_total'] = fato_registro_oportunidade['quantidade'] * fato_registro_oportunidade['Valor_Unitario']
        fato_registro_oportunidade['data_registro'] = pd.DataFrame([dt.fromtimestamp(int(tempo) / 1000) for tempo in fato_registro_oportunidade['Data de Registro']], columns=['data_registro'])
        fato_registro_oportunidade = fato_registro_oportunidade[['id_oportunidade','id_parceiro', 'id_produto', 'data_registro', 'quantidade','valor_total','status']]

        # Fato Sellout
        data_sellout_merge = data_sellout.merge(dim_produto, how='left', left_on='Nome_Produto', right_on='nome_produto')
        data_sellout_merge['id_parceiro'] = data_sellout_merge['CNpj Parceiro'].str.replace(r'\D', '', regex=True)
        data_sellout_merge['valor_total'] = data_sellout_merge['Quantidade'] * data_sellout_merge['Valor_Unitario']
        data_sellout_merge.reset_index(drop=True, inplace=True)
        data_sellout_merge['id_sellout'] = data_sellout_merge.index + 1
        data_sellout_merge['id_sellout'] = data_sellout_merge['id_sellout'].astype(str)
        data_sellout_final = data_sellout_merge[['id_sellout','id_parceiro','id_produto','Data_Fatura','NF','Quantidade','valor_total']]
        data_sellout_final.rename(columns={'Data_Fatura':'data_fatura', 
                                            'NF':'nf', 
                                            'Quantidade':'quantidade'}, inplace=True)
        
        import os

        if not os.path.exists('/opt/airflow/dags/data_warehouse'):
            os.makedirs('/opt/airflow/dags/data_warehouse')
        else:
            print("Directory already exists")

        dim_parceiro.to_csv('/opt/airflow/dags/data_warehouse/dim_parceiro.csv', index=False)
        dim_produto.to_csv('/opt/airflow/dags/data_warehouse/dim_produto.csv', index=False)
        fato_registro_oportunidade.to_csv('/opt/airflow/dags/data_warehouse/fato_registro_oportunidade.csv', index=False)
        data_sellout_final.to_csv('/opt/airflow/dags/data_warehouse/fato_sellout.csv', index=False)



    def load():
        from data_warehouse.models import engine, SessionLocal, Base
        import pandas as pd
        from sqlalchemy.exc import SQLAlchemyError
        from sqlalchemy import text
        import os

        # Garante que o diretório existe
        db_dir = '/opt/airflow/dags/data_warehouse'
        os.makedirs(db_dir, exist_ok=True)
        # Garante que o banco e as tabelas existem
        Base.metadata.create_all(engine)

        dim_parceiro_df = pd.read_csv('/opt/airflow/dags/data_warehouse/dim_parceiro.csv')
        dim_produto_df = pd.read_csv('/opt/airflow/dags/data_warehouse/dim_produto.csv')
        fato_registro_oportunidade_df = pd.read_csv('/opt/airflow/dags/data_warehouse/fato_registro_oportunidade.csv')
        fato_sellout_df = pd.read_csv('/opt/airflow/dags/data_warehouse/fato_sellout.csv')

        chunk_size = 1000

        session = SessionLocal()

        try:
            dados_chunk = []

            for dado in dim_parceiro_df.to_dict(orient='records'):
                dados_chunk.append(dado)
                if len(dados_chunk) >= chunk_size:
                    session.execute(text("INSERT INTO dim_parceiro (id_parceiro, nome_fantasia, telefone_parceiro, cnpj_parceiro) VALUES (:id_parceiro, :nome_fantasia, :telefone_parceiro, :cnpj_parceiro)"), dados_chunk)
                    session.commit()
                    dados_chunk = []
                    print(f"Inserted {chunk_size} rows into dim_parceiro")

            if dados_chunk:
                session.execute(text("INSERT INTO dim_parceiro (id_parceiro, nome_fantasia, telefone_parceiro, cnpj_parceiro) VALUES (:id_parceiro, :nome_fantasia, :telefone_parceiro, :cnpj_parceiro)"), dados_chunk)
                session.commit()
                print(f"Inserted {len(dados_chunk)} rows into dim_parceiro")

        except Exception as e:
            session.rollback()
            print(f"Erro na inserção: {e}")

        finally:
            os.remove('/opt/airflow/dags/data_warehouse/dim_parceiro.csv')
            session.close()


        try:
            dados_chunk = []

            for dado in dim_produto_df.to_dict(orient='records'):
                dados_chunk.append(dado)
                if len(dados_chunk) >= chunk_size:
                    session.execute(text("INSERT INTO dim_produto (id_produto, nome_produto, categoria_produto, valor_unitario) VALUES (:id_produto, :nome_produto, :categoria_produto, :valor_unitario)"), dados_chunk)
                    session.commit()
                    dados_chunk = []
                    print(f"Inserted {chunk_size} rows into dim_produto")

            if dados_chunk:
                session.execute(text("INSERT INTO dim_produto (id_produto, nome_produto, categoria_produto, valor_unitario) VALUES (:id_produto, :nome_produto, :categoria_produto, :valor_unitario)"), dados_chunk)
                session.commit()
                print(f"Inserted {len(dados_chunk)} rows into dim_produto")

        except Exception as e:
            session.rollback()
            print(f"Erro na inserção: {e}")

        finally:
            os.remove('/opt/airflow/dags/data_warehouse/dim_produto.csv')
            session.close()

        try:
            dados_chunk = []

            for dado in fato_registro_oportunidade_df.to_dict(orient='records'):
                dados_chunk.append(dado)
                if len(dados_chunk) >= chunk_size:
                    session.execute(text("INSERT INTO fato_registro_oportunidade (id_oportunidade, id_parceiro, id_produto, data_registro, quantidade, valor_total, status) VALUES (:id_oportunidade, :id_parceiro, :id_produto, :data_registro, :quantidade, :valor_total, :status)"), dados_chunk)
                    session.commit()
                    dados_chunk = []
                    print(f"Inserted {chunk_size} rows into fato_registro_oportunidade")

            if dados_chunk:
                session.execute(text("INSERT INTO fato_registro_oportunidade (id_oportunidade, id_parceiro, id_produto, data_registro, quantidade, valor_total, status) VALUES (:id_oportunidade, :id_parceiro, :id_produto, :data_registro, :quantidade, :valor_total, :status)"), dados_chunk)
                session.commit()
                print(f"Inserted {len(dados_chunk)} rows into fato_registro_oportunidade")
        
        except Exception as e:
            session.rollback()
            print(f"Erro na inserção: {e}")

        finally:
            os.remove('/opt/airflow/dags/data_warehouse/fato_registro_oportunidade.csv')
            session.close()

        try:
            dados_chunk = []

            for dado in fato_sellout_df.to_dict(orient='records'):
                dados_chunk.append(dado)
                if len(dados_chunk) >= chunk_size:
                    session.execute(text("INSERT INTO fato_sellout (id_sellout, id_parceiro, id_produto, data_fatura, nf, quantidade, valor_total) VALUES (:id_sellout, :id_parceiro, :id_produto, :data_fatura, :nf, :quantidade, :valor_total)"), dados_chunk)
                    session.commit()
                    dados_chunk = []
                    print(f"Inserted {chunk_size} rows into fato_sellout")

            if dados_chunk:
                session.execute(text("INSERT INTO fato_sellout (id_sellout, id_parceiro, id_produto, data_fatura, nf, quantidade, valor_total) VALUES (:id_sellout, :id_parceiro, :id_produto, :data_fatura, :nf, :quantidade, :valor_total)"), dados_chunk)
                session.commit()
                print(f"Inserted {len(dados_chunk)} rows into fato_sellout")

        except Exception as e:
            session.rollback()
            print(f"Erro na inserção: {e}")
        finally:
            os.remove('/opt/airflow/dags/data_warehouse/fato_sellout.csv')
            session.close()

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    transform_task >> load_task