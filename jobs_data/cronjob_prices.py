# Second job to run
# This job will be responsible for read the CSV file with the companies, get the prices from yfinance and save it to a new CSV file.
# Yahoo Finance API

import yfinance as yf
import pandas as pd
import os
from datetime import datetime


# Função para buscar os preços das ações e retornar uma tabela com codCVM, data e preço de fechamento ajustado
def fetch_stock_prices(codes, start_date, end_date, cod_cvm):
    try:
        # Baixando os dados de preços ajustados para as ações usando Yahoo Finance
        data = yf.download(codes, start=start_date, end=end_date, progress=False)

        # Verificando se os dados não estão vazios
        if not data.empty:
            # Selecionando apenas a coluna de preço ajustado de fechamento
            adj_close = data['Adj Close']
            # Criando um DataFrame com codCVM, data e preço de fechamento ajustado
            price_data = adj_close.reset_index()
            price_data['codCVM'] = cod_cvm
            price_data = price_data[['codCVM', 'Date', 'Adj Close']]
            price_data.rename(columns={'Adj Close': 'preco_fechamento'}, inplace=True)

            return price_data
        return None
    except Exception as e:
        print(f"Erro ao buscar preço para {codes}: {e}")
        return None


# Função para verificar a última data já salva no CSV
def get_last_saved_date(csv_file):
    if os.path.exists(csv_file):
        # Carregar o CSV existente
        existing_data = pd.read_csv(csv_file)
        # Substituir " 00:00:00" na coluna 'Date' antes de converter para datetime
        existing_data['Date'] = existing_data['Date'].str.replace(' 00:00:00', '', regex=False)
        # Verificar a data mais recente no arquivo
        existing_data['Date'] = pd.to_datetime(existing_data['Date'])
        last_date = existing_data['Date'].max()
        return last_date
    return None  # Se o arquivo não existir, retornamos None


if __name__ == '__main__':
    # Carregar o CSV com os dados das empresas
    companies = pd.read_csv('D:/GitHub/QUANT/jobs_data/data/companies_final.csv')

    # Só pega as empresas que têm códigos de ações
    companies = companies[companies['codes'].notnull()]
    companies = companies.head(25)

    # Definir o caminho do CSV de preços já salvos
    csv_file = 'D:/GitHub/QUANT/jobs_data/data/companies_prices_table.csv'

    # Verificar a última data registrada no CSV
    last_saved_date = get_last_saved_date(csv_file)

    # Definir as datas de início e fim para o período de interesse
    start_date = '2024-01-01'  # Default start date
    end_date = datetime.today().strftime('%Y-%m-%d')

    # Se já houver um CSV com dados, buscar preços a partir da última data salva
    if last_saved_date:
        start_date = last_saved_date.strftime('%Y-%m-%d')  # Iniciar a busca a partir da última data salva

    # Adicionar ".SA" a cada código de ação
    companies['codes'] = companies['codes'].apply(lambda code: code + '.SA')

    # Criar uma lista de DataFrames para armazenar os resultados
    all_prices = []

    # Iterar sobre as empresas e buscar os preços das ações
    for _, row in companies.iterrows():
        cod_cvm = row['COD_CVM']

        # Buscar os preços para os códigos dessa empresa
        stock_prices = fetch_stock_prices(row['codes'], start_date, end_date, cod_cvm)

        if stock_prices is not None:
            all_prices.append(stock_prices)

    # Concatenar todos os DataFrames individuais em um único DataFrame
    if all_prices:
        prices_df = pd.concat(all_prices, ignore_index=True)

        # Se o arquivo CSV já existir, carregar os dados antigos e concatenar com os novos
        if os.path.exists(csv_file):
            existing_data = pd.read_csv(csv_file)
            updated_data = pd.concat([existing_data, prices_df], ignore_index=True)
            updated_data.drop_duplicates(subset=['codCVM', 'Date'], keep='last', inplace=True)  # Remover duplicatas
        else:
            updated_data = prices_df

        # Salvar o DataFrame final atualizado no CSV
        updated_data.to_csv(csv_file, index=False)
        print(f"Preços de ações atualizados e salvos em {csv_file}")
    else:
        print("Nenhum dado novo para salvar.")
