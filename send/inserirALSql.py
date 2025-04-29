import pandas as pd
import pyodbc

# Configuração do SQL Server
SERVER = "CGUAL42872042\\SQLEXPRESS01"
DATABASE = "gastosFederais"
TABLE_NAME = "recursosAL"

def conectar_sql():
    conn_str = f"DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    return pyodbc.connect(conn_str)

def criar_tabela():
    conn = conectar_sql()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TABLE_NAME}')
        BEGIN
            CREATE TABLE {TABLE_NAME} (
                pt_funcao_id VARCHAR(50),
                ano INT,
                fonte_mae_id__descricao_fonte_mae VARCHAR(255),
                nome_favorecido VARCHAR(255),
                valor_liquidado DECIMAL(18,2),
                valor_empenhado DECIMAL(18,2),
                fonte_id VARCHAR(50),
                descricao_ug VARCHAR(255),
                pt_funcao_id__descricao_funcao VARCHAR(255),
                programa_id__programa_descricao VARCHAR(255),
                projeto_atividade_id VARCHAR(50),
                natureza VARCHAR(50),
                descricao_natureza3 VARCHAR(255),
                descricao_natureza2 VARCHAR(255),
                fonte_mae_id VARCHAR(50),
                descricao_natureza6 VARCHAR(255),
                descricao_natureza5 VARCHAR(255),
                codigo_favorecido VARCHAR(50),
                mes INT,
                programa_id VARCHAR(50),
                projeto_atividade_id__projeto_descricao VARCHAR(255),
                descricao_subtitulo VARCHAR(255),
                descricao_natureza VARCHAR(255),
                sub_funcao_id__descricao_sub_funcao VARCHAR(255),
                valor_pago DECIMAL(18,2),
                fonte_id__descricao_fonte VARCHAR(500),
                orgao_descricao VARCHAR(255)
            )
        END
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Tabela '{TABLE_NAME}' verificada/criada com sucesso!")

def carregar_csv(caminho_csv):
    df = pd.read_csv(caminho_csv, sep=",", dtype=str, encoding="utf-8")

    # print(df.columns)
    # print(df.head(10))
    # print(df[["valor_liquidado", "valor_empenhado", "valor_pago"]].head(10))



    df["pt_funcao_id"] = df["pt_funcao_id"].astype(str)
    df["fonte_mae_id"] = df["fonte_mae_id"].astype(str)
    df["projeto_atividade_id"] = df["projeto_atividade_id"].astype(str)
    df["fonte_id"] = df["fonte_id"].astype(str)
    df["programa_id"] = df["programa_id"].astype(str)
    df["codigo_favorecido"] = df["codigo_favorecido"].astype(str)
    df["natureza"] = df["natureza"].astype(str)


 
    colunas_monetarias = ["valor_liquidado", "valor_empenhado", "valor_pago"]
    for coluna in colunas_monetarias:
        df[coluna] = df[coluna].str.replace(r"\.", "", regex=True).str.replace(",", ".", regex=True)
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce").fillna(0)

    print(df.head())
    print(df["valor_empenhado"].head())
    print(df["valor_liquidado"].head())
    print(df["valor_pago"].head())

    print(df.dtypes)
    
    df = df.fillna(0)  
    return df


def inserir_dados_sql(df):
    conn = conectar_sql()
    cursor = conn.cursor()


    for index, row in df.iterrows():
        valores = tuple(row.fillna("").to_dict().values()) 

        placeholders = ", ".join(["?"] * len(row)) 
        query = f"INSERT INTO {TABLE_NAME} VALUES ({placeholders})"

        cursor.execute(query, valores)

    conn.commit()
    cursor.close()
    conn.close()
    print("Dados inseridos com sucesso!")

caminho_arquivo ="dados aguia/recursos recebidos al/dados.csv"

criar_tabela()
df = carregar_csv(caminho_arquivo)
print(df.head())  
inserir_dados_sql(df)
