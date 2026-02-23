import os
import requests
from dotenv import load_dotenv

def carregar_variaveis_env():
    load_dotenv()
    return {
        'access_token': os.getenv('ACCESS_TOKEN'),
        'client_id': os.getenv('CLIENT_ID'),
        'client_secret': os.getenv('CLIENT_SECRET'),
        'refresh_token': os.getenv('REFRESH_TOKEN')
    }

def atualizar_token_env(novo_access_token):
    with open('.env', 'r') as arquivo:
        variaveis = arquivo.readlines()

    with open('.env', 'w') as arquivo:
        for linha in variaveis:
            if linha.startswith('ACCESS_TOKEN='):
                arquivo.write(f"ACCESS_TOKEN={novo_access_token}\n")
            else:
                arquivo.write(linha)

def obter_novo_token(variaveis):
    url = "https://api.example.com/oauth/token"
    parametros = {
        'grant_type': 'refresh_token',
        'client_id': variaveis['client_id'],
        'client_secret': variaveis['client_secret'],
        'refresh_token': variaveis['refresh_token']
    }

    try:
        resposta = requests.post(url, data=parametros)
        resposta.raise_for_status()
        novo_token = resposta.json().get('access_token')

        if novo_token:
            atualizar_token_env(novo_token)
            print("Token atualizado com sucesso.")
        else:
            raise Exception("Token de acesso não encontrado na resposta.")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao atualizar token: {e}")

def main():
    variaveis = carregar_variaveis_env()
    obter_novo_token(variaveis)

if __name__ == "__main__":
    main()
