import os
import pandas as pd

def juntar_csvs(pasta, arquivo_saida):
    """
    Junta todos os arquivos CSV em uma pasta em um único arquivo CSV.
    """
    arquivos_csv = [f for f in os.listdir(pasta) if f.endswith('.csv')]
    df_list = []

    for arquivo in arquivos_csv:
        caminho_arquivo = os.path.join(pasta, arquivo)
        print(f"Lendo arquivo: {caminho_arquivo}")
        try:
            df = pd.read_csv(caminho_arquivo, encoding='utf-8')
            df_list.append(df)
        except Exception as e:
            print(f"Erro ao ler o arquivo {caminho_arquivo}: {e}")

    if df_list:
        try:
            df_uniao = pd.concat(df_list, ignore_index=True)
            df_uniao.to_csv(arquivo_saida, index=False, encoding='utf-8')
            print(f"Todos os arquivos CSV foram unidos em {arquivo_saida}")
        except Exception as e:
            print(f"Erro ao salvar o arquivo CSV unido: {e}")
    else:
        print("Nenhum arquivo CSV válido encontrado.")

if __name__ == "__main__":
    pasta = "dados aguia/recursos recebidos federal normalizados"  # Pasta onde estão os arquivos CSV
    arquivo_saida = "recursosRecebidosFederal.csv"  # Nome do arquivo CSV de saída
    juntar_csvs(pasta, arquivo_saida)