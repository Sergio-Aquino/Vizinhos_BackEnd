import json
import boto3
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import decimal

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Tipo não serializável: {type(obj)}")

def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        
        if 'id_Pedido' not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'ID do pedido não fornecido'}, default=decimal_default)
            }
        
        id_pedido = body['id_Pedido']
        
        dynamodb = boto3.resource('dynamodb')
        table_order = dynamodb.Table(os.environ['TABLE_ORDER'])
        
        response_order = table_order.get_item(
            Key={'id_Pedido': id_pedido}
        )
        
        if 'Item' not in response_order:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'Pedido {id_pedido} não encontrado'}, default=decimal_default)
            }
        
        order = response_order['Item']
        
        if 'id_Pagamento' not in order:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Pedido não possui ID de pagamento'}, default=decimal_default)
            }
        
        if 'fk_id_Endereco' not in order:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Pedido não possui ID da loja'}, default=decimal_default)
            }
        
        id_loja = order['fk_id_Endereco']
        
        table_store = dynamodb.Table(os.environ['STORE_ADDRESS_TABLE'])
        response_store = table_store.get_item(
            Key={'id_Endereco': id_loja}
        )
        
        if 'Item' not in response_store:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'Loja com ID {id_loja} não encontrada'}, default=decimal_default)
            }
        
        store = response_store['Item']
        
        if 'access_token' not in store:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Loja não possui token de acesso do Mercado Pago'}, default=decimal_default)
            }
        
        access_token = store['access_token']
        payment_id = order['id_Pagamento']
        
        cancelable_statuses = ["in_process", "pending", "authorized"]
        
        payment_status = get_payment_status(payment_id, access_token)
        
        if payment_status and payment_status not in cancelable_statuses:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Não é possível cancelar o pagamento com status {payment_status}. Apenas pagamentos com status {", ".join(cancelable_statuses)} podem ser cancelados.'
                }, default=decimal_default)
            }
        
        cancel_result = cancel_payment(payment_id, access_token)
        
        if cancel_result.get('status') == 'cancelled':
            hora_atual = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
            
            table_order.update_item(
                Key={'id_Pedido': id_pedido},
                UpdateExpression="set status_pedido = :status, hora_atualizacao = :hora",
                ExpressionAttributeValues={
                    ':status': 'Cancelado',
                    ':hora': hora_atual
                }
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Pedido cancelado com sucesso',
                    'id_Pedido': id_pedido,
                    'payment_id': payment_id,
                    'status': cancel_result.get('status'),
                    'status_detail': cancel_result.get('status_detail')
                }, default=decimal_default)
            }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Falha ao cancelar o pagamento',
                    'id_Pedido': id_pedido,
                    'payment_id': payment_id,
                    'error': cancel_result.get('error', 'Erro desconhecido')
                }, default=decimal_default)
            }
            
    except Exception as ex:
        print(f"Erro ao cancelar pedido: {ex}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Erro ao cancelar pedido: {str(ex)}'}, default=decimal_default)
        }

def get_payment_status(payment_id, access_token):
    try:
        url = f"https://api.mercadopago.com/v1/payments/{payment_id}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            payment_data = response.json()
            return payment_data.get('status')
        else:
            print(f"Erro ao consultar status do pagamento: {response.status_code} - {response.text}")
            return None
    except Exception as ex:
        print(f"Erro ao consultar status do pagamento: {ex}")
        return None

def cancel_payment(payment_id, access_token):
    try:
        url = f"https://api.mercadopago.com/v1/payments/{payment_id}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "status": "cancelled"
        }
        
        response = requests.put(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao cancelar pagamento: {response.status_code} - {response.text}")
            return {"error": f"Erro {response.status_code}: {response.text}"}
    except Exception as ex:
        print(f"Erro ao cancelar pagamento: {ex}")
        return {"error": str(ex)}
