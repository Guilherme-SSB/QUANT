
import base64
import concurrent.futures

import requests
from tqdm import tqdm


def make_request(method, url, **kwargs):
    response = requests.request(method, url, **kwargs)

    if response.status_code not in [200, 201]:
        raise Exception(f'Erro ao fazer requisição {response.status_code}: {response.text}')
    return response.json()


def convert_to_base64(string: str) -> str:
    return base64.b64encode(string.encode('utf-8')).decode('utf-8')


def parallel_apply(df, func, column, max_workers=4, **kwargs):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Cria um iterador com tqdm para exibir progresso
        futures = {executor.submit(func, row, **kwargs): row for row in df[column]}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing"):
            results.append(future.result())
    return results


