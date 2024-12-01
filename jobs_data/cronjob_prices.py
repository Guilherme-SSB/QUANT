# Second job to run
# This job will be responsible for read the CSV file with the companies, get the prices from yfinance and save it to a new CSV file.
# Yahoo Finance API

import yfinance as yf
import pandas as pd


# Função para buscar o preço da ação
def fetch_stock_price(codes):
    try:
        # Baixando os dados de preços para as ações usando Yahoo Finance
        # Considerando que os códigos de ações são passados como uma lista ou uma string
        data = yf.download(codes, period="1d", interval="1d")

        # Retornando o preço de fechamento, pode adicionar mais campos se necessário
        return data['Close'].iloc[-1]  # Pega o último preço de fechamento
    except Exception as e:
        print(f"Erro ao buscar preço para {codes}: {e}")
        return None



if __name__ == '__main__':
    companies = pd.read_csv('D:\GitHub\QUANT\jobs_data\data\companies.csv')

    # Criando uma nova coluna com os preços das ações usando o código das ações
    companies['stock_price'] = companies['codes'].apply(lambda x: fetch_stock_price(x))

    # Agora cria um novo DataFrame com a chave forte 'COD_CVM'
    prices_df = companies[['COD_CVM', 'stock_price']]

    # Exibindo o novo DataFrame com os preços
    print(prices_df)