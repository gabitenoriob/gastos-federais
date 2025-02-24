import requests
import csv
import os
import time
import schedule
import threading
from datetime import datetime

# Chave da API
API_KEY = "243834a56e5469793b50dd147cd802ff"

# URL base da API
BASE_URL = "https://api.portaldatransparencia.gov.br"

# Endpoints com parâmetros
ENDPOINTS = {
    "recursos_recebidos": {
        "endpoint": "/api-de-dados/despesas/recursos-recebidos",
        "params": {
            "uf": "AL",
            "pagina": 1,
        },
    }
}

# Função para consultar a API
def consultar_api(endpoint, params=None):
    """
    Consulta um endpoint da API do Portal da Transparência.
    """
    url = f"{BASE_URL}{endpoint}"
    headers = {"chave-api-dados": API_KEY}

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Erro ao acessar a API: {e}")
        return None

# Função para salvar dados em CSV
def salvar_em_csv(dados, nome_arquivo):
    """
    Salva os dados em um arquivo CSV.
    """
    if not dados:
        print(f"Nenhum dado para salvar em {nome_arquivo}.")
        return

    os.makedirs("resultados", exist_ok=True)
    caminho_arquivo = os.path.join("resultados", nome_arquivo)

    try:
        with open(caminho_arquivo, mode="w", newline="", encoding="utf-8") as arquivo_csv:
            writer = csv.DictWriter(arquivo_csv, fieldnames=dados[0].keys())
            writer.writeheader()
            writer.writerows(dados)
        print(f"Dados salvos em {caminho_arquivo}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo CSV: {e}")

# Função para processar múltiplas páginas
def processar_paginacao(endpoint, params, nome_arquivo):
    """
    Processa os dados de múltiplas páginas e salva em um único CSV.
    """
    mes = time.localtime().tm_mon
    ano = time.localtime().tm_year

    params["mesAnoInicio"] =f"{mes:02d}/{ano}"
    params["mesAnoFim"] = f"{mes:02d}/{ano}"

    dados_acumulados = []
    pagina_atual = 1

    while True:
        print(f"Consultando página {pagina_atual} do endpoint {endpoint}...")
        params["pagina"] = pagina_atual
        dados = consultar_api(endpoint, params)

        if dados and isinstance(dados, list):  # Se houver dados válidos
            dados_acumulados.extend(dados)
            pagina_atual += 1  # Passa para a próxima página
        else:
            print(f"Fim dos dados no endpoint {endpoint}.")
            break  # Sai do loop quando não houver mais dados

    # Salvar os dados acumulados
    salvar_em_csv(dados_acumulados, nome_arquivo)

# Função principal para executar as consultas
def executar_consultas():
    """
    Executa consultas para os endpoints especificados e salva os resultados em CSV.
    """
    for nome, detalhes in ENDPOINTS.items():
        print(f"Iniciando consulta para {nome}...")
        processar_paginacao(detalhes["endpoint"], detalhes["params"], "dados/favorecidos2025.csv")

# Função para agendar a execução mensal
def agendar_execucao():
    schedule.every().day.at("07:00").do(verificar_e_executar)
    while True:
        schedule.run_pending()
        time.sleep(1)

# Função para verificar se é o primeiro dia do mês
def verificar_e_executar():
    if datetime.now().day == 1:
        executar_consultas()

# Executar as consultas agendadas
if __name__ == "__main__":
    # Executar em uma thread separada para não bloquear o script principal
    threading.Thread(target=agendar_execucao).start()
