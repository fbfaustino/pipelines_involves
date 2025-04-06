import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("ACCOUNT"),
    "user": os.getenv("USER"),
    "password": os.getenv("PASSWORD"),
    "database": os.getenv("DATABASE"),
    "schema": os.getenv("SCHEMA"),
    "warehouse": os.getenv("WAREHOUSE"),
    "role": os.getenv("ROLE")
}

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        role=SNOWFLAKE_CONFIG["role"],
        autocommit=True
    )

def remove_constant_columns(csv_path):
    df = pd.read_csv(csv_path, sep=',')
    df = df.dropna(axis=1, how='all')  # Remove colunas totalmente nulas
    df = df.loc[:, df.nunique(dropna=False) > 1]  # Mantém apenas colunas com mais de um valor único
    df = df.replace(["", "NAN", "NaN", "nan", "NULL", "null", pd.NA, pd.NaT], None)
    return df

def create_dimensions_and_fact(df):
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    tabelas_sql = {
        "dim_pdv": """
            CREATE OR REPLACE TABLE {schema}.dim_pdv (
                ID_PDV STRING, NOME_PDV STRING, NOME_FANTASIA STRING, RAZAO_SOCIAL STRING, 
                BANDEIRA STRING, REDE STRING, TIPO_PDV STRING, PERFIL_PDV STRING, CANAL_PDV STRING, 
                REGIONAL STRING, MACRO_REGIONAL STRING, CNPJ STRING
            )
        """,
        "dim_pergunta": """
            CREATE OR REPLACE TABLE {schema}.dim_pergunta (
                ID_PERGUNTA STRING, NOME_CAMPO STRING, ROTULO_WEB STRING, FL_TIPO STRING, 
                TIPO_CAMPO STRING, FL_REFERENCIA STRING, OBJETIVO_CAMPO STRING, OBRIGATORIO STRING
            )
        """,
        "dim_colaborador": """
            CREATE OR REPLACE TABLE {schema}.dim_colaborador (
                ID_COLABORADOR STRING, NOME_COLABORADOR STRING, USUARIO STRING, 
                REGIONAIS STRING, MOVEL STRING, DATA_ADMISSAO STRING
            )
        """,
        "dim_produto": """
            CREATE OR REPLACE TABLE {schema}.dim_produto (
                ID_PRODUTO STRING, PRODUTO STRING, LINHA_PRODUTO STRING, MARCA STRING, 
                CATEGORIA_PRODUTO STRING, SUPER_CATEGORIA STRING, CODIGO_BARRAS STRING
            )
        """,
        "fato_coleta": """
            CREATE OR REPLACE TABLE {schema}.fato_coleta (
                ID_COLETA STRING, DATA STRING, ID_PDV STRING, ID_PERGUNTA STRING, 
                ID_COLABORADOR STRING, ID_PRODUTO STRING, 
                RESPOSTA STRING -- Alterado para garantir que sempre será tratado como string
            )
        """
    }

    for tabela, query in tabelas_sql.items():
        cursor.execute(query.format(schema=SNOWFLAKE_CONFIG["schema"]))

    def insert_into_snowflake(table_name, df_table):
        if df_table.empty:
            print(f"Nenhum dado para inserir em {table_name}")
            return
        
        df_table = df_table.replace(["", "NAN", "NaN", "nan", "NULL", "null", pd.NA, pd.NaT], None)
        
        if table_name == "fato_coleta" and 'RESPOSTA' in df_table.columns:
            df_table['RESPOSTA'] = df_table['RESPOSTA'].astype(str)
        
        if table_name != "fato_coleta":
            df_table = df_table.dropna(how='all')
        
        if df_table.empty:
            print(f"Nenhum dado válido para inserir em {table_name} após limpeza")
            return
            
        placeholders = ", ".join(["%s"] * len(df_table.columns))
        columns = ", ".join(df_table.columns)
        sql_insert = f"INSERT INTO {SNOWFLAKE_CONFIG['schema']}.{table_name} ({columns}) VALUES ({placeholders})"
        
        data_tuples = [tuple(None if pd.isna(x) else str(x) if isinstance(x, (int, float, bool)) else x for x in row) 
                      for row in df_table.to_numpy()]

        try:
            cursor.executemany(sql_insert, data_tuples)
            print(f"Dados inseridos na tabela {table_name} - {len(data_tuples)} registros")
        except Exception as e:
            print(f"Erro ao inserir dados na tabela {table_name}: {e}")
            print("Exemplo de registros problemáticos:", data_tuples[:3])
            raise

    tabelas_df = {
        "dim_pdv": df[['ID_PDV', 'NOME_PDV', 'NOME_FANTASIA', 'RAZAO_SOCIAL', 'BANDEIRA', 'REDE', 'TIPO_PDV', 
                       'PERFIL_PDV', 'CANAL_PDV', 'REGIONAL', 'MACRO_REGIONAL', 'CNPJ']].drop_duplicates(),
        "dim_pergunta": df[['ID_PERGUNTA', 'NOME_CAMPO', 'ROTULO_WEB', 'FL_TIPO', 'TIPO_CAMPO', 'FL_REFERENCIA', 
                            'OBJETIVO_CAMPO', 'OBRIGATORIO']].drop_duplicates(),
        "dim_colaborador": df[['ID_COLABORADOR', 'NOME_COLABORADOR', 'USUARIO', 'REGIONAIS', 'MOVEL', 'DATA_ADMISSAO']].drop_duplicates(),
        "dim_produto": df[['ID_PRODUTO', 'PRODUTO', 'LINHA_PRODUTO', 'MARCA', 'CATEGORIA_PRODUTO', 
                           'SUPER_CATEGORIA', 'CODIGO_BARRAS']].dropna(subset=['ID_PRODUTO']).drop_duplicates(),
        "fato_coleta": df[['ID_COLETA', 'DATA', 'ID_PDV', 'ID_PERGUNTA', 'ID_COLABORADOR', 'ID_PRODUTO', 'RESPOSTA']].drop_duplicates()
    }

    for tabela, df_tabela in tabelas_df.items():
        insert_into_snowflake(tabela, df_tabela)

    cursor.close()
    conn.close()
    print("🚀 Tabelas e dados carregados no Snowflake com sucesso!")

csv_path = "C:/pipelines/involves/files/full_base_involve.csv"
df_limpo = remove_constant_columns(csv_path)
create_dimensions_and_fact(df_limpo)
