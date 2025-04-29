import pandas as pd
import pyodbc

# Configuração do SQL Server
SERVER = "CGUAL42872042\\SQLEXPRESS01"
DATABASE = "gastosFederais"
TABLE_NAME = "recursosFederais"

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
                codigoPessoa VARCHAR(50),
                nomePessoa VARCHAR(255),
                tipoPessoa VARCHAR(255),
                municipioPessoa VARCHAR(255),
                siglaUFPessoa VARCHAR(2),
                codigoUG VARCHAR(50),
                nomeUG VARCHAR(255),
                codigoOrgao VARCHAR(50),
                nomeOrgao VARCHAR(255),
                codigoOrgaoSuperior VARCHAR(50),
                nomeOrgaoSuperior VARCHAR(255),
                valor VARCHAR(50),
                Ano INT,
                Mes INT
            )
        END
    """)
        
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Tabela '{TABLE_NAME}' verificada/criada com sucesso!")

def carregar_csv(caminho_csv):
    df = pd.read_csv(caminho_csv, sep=",", dtype=str, encoding="utf-8")
    print(df['valor'])

    colunas_monetarias = ["valor"]
    for coluna in colunas_monetarias:
        # Primeiro, removemos os pontos de milhar
        df[coluna] = df[coluna].str.replace(r"\.(?=\d{3})", "", regex=True)  # Remove apenas os pontos de milhar
        # Depois, substituímos as vírgulas por ponto
        df[coluna] = df[coluna].str.replace(",", ".", regex=True)
        # Convertemos a coluna para numérico, tratando erros e substituindo valores inválidos por 0
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce").fillna(0)

    # Verificando os valores corrigidos
    print(df['valor'])

    return df

def apagar_dados_sql():
    conn = conectar_sql()
    cursor = conn.cursor()

    # Comando para apagar todos os dados da tabela
    cursor.execute(f"DELETE FROM {TABLE_NAME}")  # Ou você pode usar TRUNCATE se preferir
    conn.commit()
    cursor.close()
    conn.close()
    print("Dados apagados com sucesso!")

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

# Caminho do arquivo CSV
caminho_arquivo = "dados aguia/recursos recebidos federal normalizados/recursosRecebidosFederal_corrigido.csv"

# Criação da tabela (caso não exista)
criar_tabela()

# Carregar o arquivo CSV
df = carregar_csv(caminho_arquivo)
print(df.head())

# Apagar os dados existentes na tabela
apagar_dados_sql()

# Inserir os novos dados
inserir_dados_sql(df)
