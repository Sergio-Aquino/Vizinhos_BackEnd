import boto3
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    try:
        cpf = event.get('queryStringParameters', {}).get('cpf')
        if not cpf:
            return {
                'statusCode': 400,
                'body': json.dumps({'erro': 'Parâmetro "cpf" é obrigatório.'})
            }
        
        user_table = dynamodb.Table(os.environ['USER_TABLE'])

        print(f"Buscando dados para o CPF: {cpf} na tabela {user_table}")

        response = user_table.get_item(
            Key={'cpf': cpf},
            ProjectionExpression='cpf, Plano_Vendedor, Data_Expiracao_Plano_Vendedor'
        )

        if 'Item' not in response:
            print(f"Nenhum item encontrado para o CPF: {cpf}")
            return {
                'statusCode': 404,
                'body': json.dumps({'erro': f'Vendedor com CPF {cpf} não encontrado na tabela {user_table}'})
            }

        item = response['Item']
        print(f"Item encontrado: {item}")

        data_expiracao = datetime.strptime(item['Data_Expiracao_Plano_Vendedor'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo('America/Sao_Paulo'))
        agora = datetime.now(ZoneInfo('America/Sao_Paulo'))
        tempo_restante = data_expiracao - agora

        if tempo_restante.total_seconds() < 0:
            mensagem = "Plano expirado"
            dias_restantes = 0
            segundos_totais = 0
        else:
            dias_restantes = tempo_restante.days
            segundos_restantes_no_dia = tempo_restante.seconds
            horas, rem = divmod(segundos_restantes_no_dia, 3600)
            minutos, segundos = divmod(rem, 60)
            mensagem = f"{dias_restantes} dias, {horas:02d}:{minutos:02d}:{segundos:02d} restantes"
            segundos_totais = int(tempo_restante.total_seconds())

        resultado = {
            'cpf_consultado': cpf,
            'data_expiracao': data_expiracao,
            'tempo_restante_formatado': mensagem,
            'dias_restantes': dias_restantes,
            'total_segundos_restantes': segundos_totais
        }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(resultado, default=str)
        }

    except KeyError as e:
        print(f"Erro de chave: {e}")
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'erro': f'Parâmetro ausente ou inválido: {e}'})
        }
    except dynamodb.meta.client.exceptions.ResourceNotFoundException as e:
        error_message = f"Erro Boto3/AWS: Tabela DynamoDB '{user_table}' não encontrada ou inacessível. Verifique o nome e a região. Detalhe: {e}"
        print(error_message)
        return {
            'statusCode': 404,
            'body': json.dumps({'erro': error_message})
        }
    except boto3.exceptions.Boto3Error as e:
        print(f"Erro Boto3/AWS: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'erro': f'Erro ao acessar o DynamoDB: {e}'})
        }
    except Exception as e:
        print(str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'erro': f'{str(e)}'})
        }

if __name__ == "__main__":
    os.environ['USER_TABLE'] = ''
    test_event = {
        'queryStringParameters': {
            'cpf': ''
        }
    }
    print(lambda_handler(test_event, None))