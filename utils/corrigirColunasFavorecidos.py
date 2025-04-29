import pandas as pd

def split_month_year(df, column_name='anoMes'):
    """
    Splits a 'anoMes' column (e.g., '202405') into separate 'Ano' and 'Mes' columns.
    
    Args:
      df: pandas DataFrame containing the 'anoMes' column.
      column_name: The name of the column to split.
      
    Returns:
      A new DataFrame with the added 'Ano' and 'Mes' columns.
      Returns the original DataFrame if the column is not found or if errors occur.
    """
    try:
        # Verifica se a coluna existe no DataFrame
        if column_name in df.columns:
            # Garantir que a coluna seja do tipo string para facilitar a manipulação
            df[column_name] = df[column_name].astype(str)
            # Separar a coluna 'anoMes' em 'Ano' e 'Mes'
            df['Ano'] = df[column_name].str.slice(0, 4)  # Os primeiros 4 caracteres correspondem ao ano
            df['Mes'] = df[column_name].str.slice(4, 6)  # Os próximos 2 caracteres correspondem ao mês

            # Converter para int
            df['Ano'] = df['Ano'].astype(int)
            df['Mes'] = df['Mes'].astype(int)

            return df
        else:
            print(f"Error: Column '{column_name}' not found in DataFrame.")
            print(f"Available columns: {df.columns.tolist()}")
            return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return df


# Ajuste para carregar o CSV corretamente com vírgula como delimitador
df = pd.read_csv('dados aguia/recursos recebidos federal normalizados/recursosRecebidosFederal.csv', sep=',', quotechar='"', encoding='utf-8')

# Print the columns to verify the exact names
print(f"Available columns: {df.columns.tolist()}")

# Call the function with the corrected column names
df = split_month_year(df, column_name='anoMes')

# If the column is found, drop it
if 'anoMes' in df.columns:
    df = df.drop(columns=['anoMes'])

# Print the DataFrame's columns after modification
print(f"Columns after modification: {df.columns.tolist()}")

# Save the DataFrame back to CSV
df.to_csv('dados aguia/recursos recebidos federal normalizados/recursosRecebidosFederal_corrigido.csv', index=False)
