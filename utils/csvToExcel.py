import pandas as pd

# Carregar o arquivo CSV
csv_file = 'dados aguia/recursos recebidos federal normalizados/recursosRecebidosFederal_corrigido.csv'

# Ler o CSV em um DataFrame
df = pd.read_csv(csv_file)

# Salvar o DataFrame como um arquivo Excel
excel_file = 'recursosRecebidosFederal.xlsx'
df.to_excel(excel_file, index=False)

print(f"Arquivo Excel '{excel_file}' gerado com sucesso!")
