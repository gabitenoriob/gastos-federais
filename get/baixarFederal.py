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

# Endpoint e parâmetros
ENDPOINT = "/api-de-dados/despesas/recursos-recebidos"
UF = "AL"
OUTPUT_DIR = "dados aguia/recursos recebidos federal normalizados"
OUTPUT_FILE = f"{OUTPUT_DIR}/recursosRecebidosFederal.csv"


def consultar_api(mes, ano, pagina=1):
    """Consulta a API para um determinado mês e ano."""
    params = {
        "mesAnoInicio": f"{mes:02d}/{ano}",
        "mesAnoFim": f"{mes:02d}/{ano}",
        "uf": UF,
        "pagina": pagina,
    }
    headers = {"chave-api-dados": API_KEY}

    try:
        response = requests.get(f"{BASE_URL}{ENDPOINT}", headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Erro ao acessar a API: {e}")
        return None


def processar_paginacao(mes, ano):
    """Processa todas as páginas disponíveis para o mês e ano fornecidos."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)  # Cria o diretório se não existir
    dados_acumulados = []
    pagina_atual = 1

    while True:
        print(f"📥 Baixando página {pagina_atual} de {mes:02d}/{ano}...")
        dados = consultar_api(mes, ano, pagina_atual)

        if dados and isinstance(dados, list) and len(dados) > 0:
            dados_acumulados.extend(dados)
            pagina_atual += 1
            time.sleep(1)  # Aguarda 1 segundo entre requisições para evitar bloqueios da API
        else:
            print(f"✅ Fim dos dados para {mes:02d}/{ano}. Última página baixada: {pagina_atual - 1}.")
            break  # Adicionado para sair do loop corretamente

    salvar_em_csv(dados_acumulados, OUTPUT_FILE)


def salvar_em_csv(dados, arquivo):
    """Salva os dados no arquivo CSV."""
    if not dados:
        print(f"⚠️ Nenhum dado para salvar em {arquivo}.")
        return

    cabecalhos = list(dados[0].keys())
    existe_arquivo = os.path.isfile(arquivo)

    with open(arquivo, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cabecalhos)
        if not existe_arquivo:
            writer.writeheader()
        writer.writerows(dados)

    print(f"✅ Dados salvos em {arquivo}")


def verificar_e_executar():
    """Verifica se é o dia e executa a coleta de dados do mês anterior."""
    hoje = datetime.today()
    if hoje.day == 1:
        mes_anterior = 12 if hoje.month == 1 else hoje.month - 1
        ano_anterior = hoje.year - 1 if hoje.month == 1 else hoje.year
        print(f"📅 Executando coleta para {mes_anterior:02d}/{ano_anterior}.")
        processar_paginacao(mes_anterior, ano_anterior)
        print("🛑 Coleta concluída. Encerrando o programa.")
        os._exit(0)  # Encerra o programa


def agendar_execucao():
    """Agendar a execução da coleta de dados mensalmente"""
    schedule.every().day.at("08:32").do(verificar_e_executar)
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    threading.Thread(target=agendar_execucao, daemon=True).start()
    while True:
        time.sleep(1)
