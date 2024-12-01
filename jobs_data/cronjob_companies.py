# First job to run
# This job will be responsible for update the companies jobs_data and save it to a CSV file.
# https://www.b3.com.br/pt_br/produtos-e-servicos/negociacao/renda-variavel/empresas-listadas.htm

import json
import pandas as pd

from jobs_data.auxiliar import *

URL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/'
URL_AUX = {
    "language":"pt-br",
    "pageNumber":1,
    "pageSize":120
}

URL_DETAILED = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/'
URL_DETAILED_AUX = {
    "codeCVM": "10456",
    "language":"pt-br"
}

def get_total_pages() -> int:
    first_config = convert_to_base64(json.dumps(URL_AUX))

    url = URL + first_config

    response = make_request('GET', url)

    total_companies = response['page']['totalRecords']

    return total_companies // URL_AUX['pageSize']


def get_urls():
    total_pages = get_total_pages()

    urls = []
    for i in range(1, total_pages+2):
        config = URL_AUX
        config['pageNumber'] = i
        config = convert_to_base64(json.dumps(config))
        url = URL + config
        urls.append(url)

    return urls


def format_companies(companies: pd.DataFrame) -> pd.DataFrame:
    companies = companies[[col for col in companies.columns if not col.endswith('_drop')]]
    companies.drop(columns=['additional_info'], inplace=True)

    # Drop columns
    drop_columns = ['industryClassification', 'industryClassificationEng', 'hasQuotation', 'code', 'describleCategoryBVMF', 'dateQuotation']
    companies.drop(columns=drop_columns, inplace=True)

    # Rename columns
    companies.rename(columns={
        'codeCVM': 'COD_CVM',
        'issuingCompany': 'SIGLA',
        'companyName': 'RAZAO_SOCIAL',
        'tradingName': 'NOME_FANTASIA',
        'cnpj': 'CNPJ',
        'marketIndicator': 'MKT_INDICATOR',
        'typeBDR': 'TYPE_BDR',
        'dateListing': 'DT_LISTAGEM',
        'status': 'STATUS',
        'segment': 'SEGMENTO',
        'segmentEng': 'SEGMENTO_ENG',
        'type': 'TIPO',
        'market': 'MERCADO',
        'activity': 'ATIVIDADE',
        'website': 'WEBSITE',
        'institutionCommon': 'INSTITUITION_COMMON',
        'institutionPreferred': 'INSTITUITION_PREFERRED',
        'lastDate': 'ULTIMA_ATUALIZACAO',
        'hasEmissions': 'TEM_EMISSOES',
        'hasBDR': 'TEM_BDR',
    }, inplace=True)

    # Ajusta formato da data
    companies['DT_LISTAGEM'] = companies['DT_LISTAGEM'].replace('31/12/9999', '01/01/2100')
    companies['DT_LISTAGEM'] = pd.to_datetime(companies['DT_LISTAGEM'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

    # Criar novas colunas a partir da lista de dicionários
    companies['codes'] = companies['otherCodes'].apply(
        lambda x: [item['code'] for item in x] if isinstance(x, list) else []
    )
    companies['isins'] = companies['otherCodes'].apply(
        lambda x: [item['isin'] for item in x] if isinstance(x, list) else []
    )

    companies.drop(columns=['otherCodes'], inplace=True)

    return companies


def fetch_company_info(cod_cvm: str):
    config = URL_DETAILED_AUX
    config['codeCVM'] = cod_cvm
    config = convert_to_base64(json.dumps(config))
    url = URL_DETAILED + config

    res = make_request('GET', url)

    return res


def get_companies():
    try:
        companies = pd.read_csv(r'D:\GitHub\QUANT\jobs_data\data\companies.csv')
    except:
        urls = get_urls()
        companies = []

        for url in urls:
            response = make_request('GET', url)
            companies += response['results']

        companies = pd.DataFrame(companies)

        companies['additional_info'] = parallel_apply(companies, fetch_company_info, column='codeCVM', max_workers=32)
        additional_df = pd.json_normalize(companies['additional_info'])
        companies = companies.merge(additional_df, on='codeCVM', how='left', suffixes=('', '_drop'))

        companies = format_companies(companies)

        companies.to_csv('D:\GitHub\QUANT\jobs_data\data\companies.csv', index=False)

    return companies


if __name__ == '__main__':
    a = get_companies()
    print(a)

