from dotenv import load_dotenv
import os
import requests
import pyodbc
from datetime import datetime, timedelta
import json

# Carregar variáveis de ambiente
load_dotenv()
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')

# Configuração de conexão com Azure SQL
CONNECTION_STRING = os.getenv("DB_CONNECTION")

def connect_to_database():
    """Estabelece conexão com o banco de dados Azure SQL."""
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        return conn
    except pyodbc.Error as e:
        raise Exception(f"Erro ao conectar ao banco de dados: {str(e)}")

def create_table_if_not_exists(cursor):
    """Cria a tabela no banco de dados se ela não existir."""
    try:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Facebook_Ads')
            CREATE TABLE Facebook_Ads (
                id INT IDENTITY(1,1) PRIMARY KEY,
                account_name VARCHAR(255),
                date_start DATE,
                campaign_id VARCHAR(100),
                account_id VARCHAR(100),
                campaign_name VARCHAR(255),
                objective VARCHAR(100),
                adset_id VARCHAR(100),
                adset_name VARCHAR(255),
                ad_id VARCHAR(100),
                ad_name VARCHAR(255),
                metric_type VARCHAR(100),
                metric_value INT,
                insert_date DATETIME DEFAULT GETDATE()
            )
        """)
        cursor.commit()
    except pyodbc.Error as e:
        raise Exception(f"Erro ao criar tabela: {str(e)}")

def check_duplicate_record(cursor, date_start, campaign_id, account_id, adset_id, ad_id, metric_type):
    """Verifica se já existe um registro com os mesmos valores no banco de dados."""
    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM Facebook_Ads
            WHERE date_start = ?
            AND campaign_id = ?
            AND account_id = ?
            AND adset_id = ?
            AND ad_id = ?
            AND metric_type = ?
        """, (date_start, campaign_id, account_id, adset_id, ad_id, metric_type))
        return cursor.fetchone()[0] > 0
    except pyodbc.Error:
        return False

def insert_into_database(cursor, data):
    """Insere dados no banco de dados Azure SQL."""
    try:
        is_duplicate = check_duplicate_record(
            cursor,
            data[1],   # date_start
            data[2],   # campaign_id
            data[3],   # account_id
            data[6],   # adset_id
            data[8],   # ad_id
            data[10]   # metric_type
        )

        if is_duplicate:
            # Apagar o registro duplicado
            cursor.execute("""
                DELETE FROM Facebook_Ads
                WHERE date_start = ?
                AND campaign_id = ?
                AND account_id = ?
                AND adset_id = ?
                AND ad_id = ?
                AND metric_type = ?
            """, (data[1], data[2], data[3], data[6], data[8], data[10]))
            cursor.commit()

            # Re-inserir o registro atualizado
            cursor.execute("""
                INSERT INTO Facebook_Ads
                (account_name, date_start, campaign_id, account_id, campaign_name,
                 objective, adset_id, adset_name, ad_id, ad_name, metric_type, metric_value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data)
            cursor.commit()
            return "overwritten"  # Indica que o registro foi sobrescrito

        # Inserir o registro diretamente se não houver duplicado
        cursor.execute("""
            INSERT INTO Facebook_Ads
            (account_name, date_start, campaign_id, account_id, campaign_name,
             objective, adset_id, adset_name, ad_id, ad_name, metric_type, metric_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        cursor.commit()
        return "added"  # Indica que o registro foi adicionado sem sobrescrever
    except pyodbc.Error as e:
        print(f"Erro ao processar registro: {e}")
        return "error"

def get_account_name(account_id):
    """Obtém o nome da conta de anúncios."""
    url = f"https://graph.facebook.com/v21.0/{account_id}"
    params = {'access_token': ACCESS_TOKEN, 'fields': 'name'}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('name', 'Unknown Account')
    except requests.RequestException:
        return "Unknown Account"

def get_daily_conversion_data(account_id, start_date, end_date):
    """Obtém dados de conversões diárias."""
    url = f"https://graph.facebook.com/v21.0/{account_id}/insights"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'date_start,campaign_id,account_id,campaign_name,objective,adset_id,adset_name,ad_id,ad_name,actions',
        'level': 'ad',
        'time_range': json.dumps({'since': start_date, 'until': end_date}),
        'time_increment': 1,
    }

    try:
        while url:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            yield from data.get('data', [])
            url = data.get('paging', {}).get('next')
            params = {}
    except requests.RequestException:
        yield from []

def process_daily_conversion_data(data, account_name, cursor):
    """Processa os dados retornados e salva no banco de dados."""
    added_count = 0
    overwritten_count = 0
    error_count = 0

    for record in data:
        try:
            base_data = [
                account_name,
                record.get('date_start'),
                record.get('campaign_id'),
                record.get('account_id'),
                record.get('campaign_name'),
                record.get('objective'),
                record.get('adset_id'),
                record.get('adset_name'),
                record.get('ad_id'),
                record.get('ad_name'),
            ]

            actions = record.get('actions', [])
            for action in actions:
                metric = action.get('action_type')
                result = int(action.get('value', 0))
                row = base_data + [metric, result]
                result_type = insert_into_database(cursor, row)

                if result_type == "added":
                    added_count += 1
                elif result_type == "overwritten":
                    overwritten_count += 1
                elif result_type == "error":
                    error_count += 1
        except Exception:
            error_count += 1

    return added_count, overwritten_count, error_count

def main():
    """Função principal que executa o fluxo completo do script."""
    ad_account_id = "ad_account_id" ## Insert your account id here
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    connection_error = None

    try:
        print("Conectando ao banco de dados...")
        conn = connect_to_database()
        cursor = conn.cursor()

        print("Verificando/criando tabela no banco de dados...")
        create_table_if_not_exists(cursor)

        if not ACCESS_TOKEN:
            raise ValueError("TOKEN de acesso não encontrado no arquivo .env")

        print("Obtendo informações da conta...")
        account_name = get_account_name(ad_account_id)

        current_start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        total_added = 0
        total_overwritten = 0
        total_errors = 0

        while current_start <= end:
            current_end = current_start + timedelta(days=6)
            if current_end > end:
                current_end = end

            print(f"Processando período: {current_start.strftime('%Y-%m-%d')} a {current_end.strftime('%Y-%m-%d')}")
            data = get_daily_conversion_data(ad_account_id, current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d'))
            added, overwritten, errors = process_daily_conversion_data(data, account_name, cursor)
            total_added += added
            total_overwritten += overwritten
            total_errors += errors

            print(f"✓ Registros adicionados: {added}")
            print(f"✓ Registros sobrescritos: {overwritten}")

            current_start = current_end + timedelta(days=1)

        print("\n✅ Processamento concluído!")
        print(f"✓ Registros adicionados (sem sobrescrever): {total_added}")
        print(f"✓ Registros duplicados e sobrescritos: {total_overwritten}")
        print(f"ℹ Erros ao processar registros: {total_errors}")

    except Exception as e:
        connection_error = str(e)
        print(f"❌ Erro crítico durante a execução: {connection_error}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Conexão com o banco de dados fechada")
        print(f"\n🚨 Detalhes finais de execução:")
        print(f"   - Conexão com banco: {'Erro' if connection_error else 'Sucesso'}")
        print(f"   - Total de erros no processamento: {total_errors}")

if __name__ == "__main__":
    main()
