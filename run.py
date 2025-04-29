import schedule
import time
import subprocess
from datetime import datetime

def rodar_script(script):
    """Executa um script Python e exibe a saída no terminal."""
    try:
        print(f"Executando {script}...")
        subprocess.run(["python", script], check=True)
        print(f"Finalizado {script} ✅")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script}: {e}")

def baixar_dados():
    """Executa os scripts de download no dia 1."""
    rodar_script("get/baixarFederal.py")
    rodar_script("get/baixarListasAl.py")
    rodar_script("get/baixarDespesasAl.py")

def processar_dados():
    rodar_script("send/inserirALSql.py")           # Envia os dados locais para SQL
    rodar_script("utils/corrigirColunasFavorecidos.py")           # Corrige os dados federais
    rodar_script("send/inserirFederalSql.py")      # Envia os dados federais corrigidos para SQL

def verificar_execucao():
    """Verifica a data e agenda a execução correta."""
    hoje = datetime.today().day
    if hoje == 1:
        baixar_dados()
    if hoje == 1:
        processar_dados()

# Agendar para rodar de cada dia
schedule.every().day.at("08:30").do(verificar_execucao)

print("Agendador iniciado. O run.py está rodando continuamente...")

while True:
    schedule.run_pending()
    time.sleep(60)  # Verifica a cada 60 segundos
