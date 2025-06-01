import json
import boto3
import os
from datetime import datetime, timedelta
import mercadopago
from zoneinfo import ZoneInfo

dynamodb = boto3.resource('dynamodb')

PLANOS = {
    "one_month": 30,
    "three_month": 90,
    "six_month": 180
}

MERCADO_PAGO_STATUS_MAP = {
    "pending": "Aguardando Pagamento",
    "approved": "Pago",
    "authorized": "Autorizado",
    "in_process": "Em análise",
    "in_mediation": "Em disputa",
    "rejected": "Rejeitado",
    "cancelled": "Cancelado",
    "refunded": "Reembolsado",
    "charged_back": "Chargeback"
}


def map_status_pagamento(status: str) -> str:
    return MERCADO_PAGO_STATUS_MAP.get(status, "Status desconhecido")

def is_subscription_active(user: dict):
    subscription_update_date = user.get('Data_Atualizacao_Plano_Vendedor')
    if not subscription_update_date:
        print("Data de atualização do plano do vendedor não encontrada.")
        raise KeyError("Data de atualização do plano do vendedor não encontrada.")
    
    subscription_plan = user.get('Plano_Vendedor')
    if not subscription_plan:
        raise KeyError("Plano de vendedor não encontrado no usuário")
    
    days = PLANOS.get(subscription_plan)
    if days is None:
        print(f"Plano de vendedor inválido: {subscription_plan}")
        raise ValueError(f"Plano de vendedor inválido: {subscription_plan}")
    
    expiration_date = datetime.strptime(subscription_update_date, "%Y-%m-%d %H:%M:%S") + timedelta(days=days)
    expiration_date = expiration_date.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))

    user['Data_Expiracao_Plano_Vendedor'] = expiration_date.strftime("%Y-%m-%d %H:%M:%S")

    if not datetime.now(ZoneInfo("America/Sao_Paulo")) <= expiration_date:
        print(f"Plano do vendedor expirado. Data de expiração: {expiration_date}")
        user['Status_Plano_Vendedor'] = "Expirado - Aguardando renovação"

def refresh_status_plano_vendedor(user: dict) -> None:
    id_Pagamento = user.get('id_Pagamento_Plano_Vendedor')
    if not id_Pagamento:
        raise ValueError("id_Pagamento_Plano_Vendedor não encontrado no usuário")
    
    access_token = os.environ["ACCESS_TOKEN"]
    sdk = mercadopago.SDK(access_token)

    payment = sdk.payment().get(id_Pagamento)
    user['Status_Plano_Vendedor'] = map_status_pagamento(payment['response']['status'])

    print(f"Status do plano do vendedor {user.get('email')} atualizado para: {user['Status_Plano_Vendedor']}")

def lambda_handler(event:any, context:any):
    try:
        email = event.get('queryStringParameters', {}).get('email')
        if not email:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Email não pode ser vazio"}, default=str)
            }
        if not isinstance(email, str):
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Email inválido"}, default=str)
            }
        
        table_user = dynamodb.Table(os.environ['USER_TABLE'])

        response_user = table_user.query(
            IndexName='email-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('email').eq(email)
        )

        if 'Items' not in response_user or not response_user['Items']:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Usuário não encontrado: " + {email}}, default=str)
            }
        
        user = response_user['Items'][0]

        if user.get('Usuario_Tipo') not in ['seller', 'customer_seller']:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Usuário não é um vendedor: " + email}, default=str)
            }
        
        if user.get('Plano_Vendedor') not in PLANOS:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Plano de vendedor inválido: " + email}, default=str)
            }
        
        refresh_status_plano_vendedor(user)
        is_subscription_active(user)
        
        table_user.put_item(Item=user)

        return {
            "statusCode": 200,
            "body": json.dumps({"status_plano": user.get('Status_Plano_Vendedor'), "data_validade": f"{user.get('Data_Expiracao_Plano_Vendedor')}"}, default=str)
        }
    except KeyError as ke:
        print(ke)
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(ke)}, default=str)
        }
    except ValueError as ve:
        print(f"Erro de valor: {ve}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(ve)}, default=str)
        }
    except Exception as ex:
        print(f"Erro ao atualiar o status do plano do vendedor: {ex}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao atualiar o status do plano do vendedor: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    sample_event = {
        'body': json.dumps({"email": "sergioadm120@gmail.com"})
    }
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['ACCESS_TOKEN'] = "APP_USR-1356231261866648-051013-96ea67cf7f09765fd6a88e99c93bdd9f-2430273423"

    print(lambda_handler(sample_event, None))