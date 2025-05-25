# -*- coding: utf-8 -*-
import json
import os
import requests
import boto3
from datetime import datetime
from zoneinfo import ZoneInfo
import decimal
import uuid

session = requests.Session()
dynamodb = boto3.resource("dynamodb")

TABLE_ORDER_NAME = os.environ.get("TABLE_ORDER")
STORE_ADDRESS_TABLE_NAME = os.environ.get("STORE_ADDRESS_TABLE")

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Tipo não serializável: {type(obj)}")

def get_payment_status(payment_id, access_token):
    try:
        payment_id_str = str(payment_id)
        url = f"https://api.mercadopago.com/v1/payments/{payment_id_str}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        print(f"Consultando status do pagamento {payment_id_str}...")
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        payment_data = response.json()
        status = payment_data.get("status")
        print(f"Status do pagamento {payment_id_str}: {status}")
        return status

    except requests.exceptions.HTTPError as http_err:
        error_message = f"Erro HTTP ao consultar status do pagamento {payment_id}: {http_err.response.status_code} - {http_err.response.text}"
        print(error_message)
        return None
    except requests.exceptions.RequestException as req_ex:
        error_message = f"Erro de conexão/requisição ao consultar status do pagamento: {req_ex}"
        print(error_message)
        return None
    except Exception as ex:
        error_message = f"Erro inesperado na função get_payment_status: {ex}"
        print(error_message)
        return None

def refund_total_payment(payment_id, access_token):
    try:
        payment_id_str = str(payment_id)
        url = f"https://api.mercadopago.com/v1/payments/{payment_id_str}/refunds"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "X-Idempotency-Key": str(uuid.uuid4()) 
        }
        payload = None
        print(f"Iniciando reembolso TOTAL para o pagamento {payment_id_str}...")
        response = session.post(url, headers=headers, json=payload, timeout=15)

        if response.status_code == 423:
            error_message = f"Erro 423: Recurso bloqueado. Tentativa de reembolso recente para {payment_id_str}. {response.text}"
            print(error_message)
            try:
                return {"error_locked": True, "details": response.json()}
            except json.JSONDecodeError:
                return {"error_locked": True, "details": error_message}

        if response.status_code in [200, 201]:
            print(f"Reembolso para {payment_id_str} processado com sucesso (Status: {response.status_code}).")
            return response.json()
        
        response.raise_for_status()
        error_message = f"Erro inesperado após POST reembolso (Status {response.status_code}): {response.text}"
        print(error_message)
        return {"error": error_message}

    except requests.exceptions.HTTPError as http_err:
        error_message = f"Erro HTTP ao solicitar reembolso total: {http_err.response.status_code} - {http_err.response.text}"
        print(error_message)
        try:
            return {"error": error_message, "details": http_err.response.json()}
        except json.JSONDecodeError:
            return {"error": error_message}
    except requests.exceptions.RequestException as req_ex:
        error_message = f"Erro de conexão/requisição ao solicitar reembolso: {req_ex}"
        print(error_message)
        return {"error": error_message}
    except Exception as ex:
        error_message = f"Erro inesperado na função refund_total_payment: {ex}"
        print(error_message)
        return {"error": error_message}

def lambda_handler(event, context):
    payment_id_for_logging = None
    id_pedido_for_logging = None
    try:
        if not TABLE_ORDER_NAME or not STORE_ADDRESS_TABLE_NAME:
             print("CRITICAL: Variáveis de ambiente TABLE_ORDER ou STORE_ADDRESS_TABLE não configuradas.")
             return {
                "statusCode": 500,
                "body": json.dumps({"message": "Erro de configuração interna do servidor."})
            }
        
        table_order = dynamodb.Table(TABLE_ORDER_NAME)
        table_store = dynamodb.Table(STORE_ADDRESS_TABLE_NAME)

        try:
            body = json.loads(event.get("body", "{}"))
        except json.JSONDecodeError:
            return {"statusCode": 400, "body": json.dumps({"message": "Corpo da requisição inválido (não é JSON válido)"})}

        id_pedido = body.get("id_Pedido")
        id_pedido_for_logging = id_pedido
        if not id_pedido: return {"statusCode": 400, "body": json.dumps({"message": "O parâmetro \"id_Pedido\" é obrigatório."})}

        try:
            response_order = table_order.get_item(Key={"id_Pedido": id_pedido})
        except Exception as db_ex:
             print(f"Erro ao acessar DynamoDB (Order Table): {db_ex}")
             return {"statusCode": 500, "body": json.dumps({"message": f"Erro ao buscar pedido no DynamoDB: {str(db_ex)}"}, default=decimal_default)}
        if "Item" not in response_order: return {"statusCode": 404, "body": json.dumps({"message": f"Pedido {id_pedido} não encontrado."}, default=decimal_default)}
        order = response_order["Item"]

        payment_id = order.get("id_Pagamento")
        payment_id_for_logging = payment_id
        id_loja = order.get("fk_id_Endereco")
        if not payment_id: return {"statusCode": 400, "body": json.dumps({"message": "Pedido não possui ID de pagamento (id_Pagamento)."}, default=decimal_default)}
        if not id_loja: return {"statusCode": 400, "body": json.dumps({"message": "Pedido não possui ID da loja (fk_id_Endereco)."}, default=decimal_default)}

        try:
            response_store = table_store.get_item(Key={"id_Endereco": id_loja})
        except Exception as db_ex:
             print(f"Erro ao acessar DynamoDB (Store Table): {db_ex}")
             return {"statusCode": 500, "body": json.dumps({"message": f"Erro ao buscar loja no DynamoDB: {str(db_ex)}"}, default=decimal_default)}
        if "Item" not in response_store: return {"statusCode": 404, "body": json.dumps({"message": f"Loja com ID {id_loja} não encontrada."}, default=decimal_default)}
        store = response_store["Item"]
        access_token = store.get("access_token")
        if not access_token: return {"statusCode": 400, "body": json.dumps({"message": "Loja não possui token de acesso do Mercado Pago (access_token)."}, default=decimal_default)}

        current_payment_status = get_payment_status(payment_id, access_token)
        if current_payment_status is None:
            return {
                "statusCode": 503,
                "body": json.dumps({
                    "message": "Falha ao verificar o status atual do pagamento no Mercado Pago. Não foi possível prosseguir com o reembolso.",
                    "id_Pedido": id_pedido, "payment_id": payment_id
                }, default=decimal_default)
            }
        if current_payment_status != 'approved':
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": f"Reembolso não permitido. O estado atual do pagamento é ",
                    "id_Pedido": id_pedido, "payment_id": payment_id, "current_status": current_payment_status
                }, default=decimal_default)
            }

        refund_result = refund_total_payment(payment_id, access_token)

        if isinstance(refund_result, dict) and refund_result.get("error_locked"):
            return {
                "statusCode": 423,
                "body": json.dumps({
                    "message": "Falha ao processar reembolso: Recurso bloqueado devido a tentativa recente.",
                    "id_Pedido": id_pedido, "payment_id": payment_id,
                    "error_details": refund_result.get("details", "Already posted the same request in the last minute")
                }, default=decimal_default)
            }
        
        if isinstance(refund_result, dict) and "error" in refund_result:
            status_code = 500
            if "details" in refund_result and isinstance(refund_result["details"], dict):
                 api_status = refund_result["details"].get("status")
                 if isinstance(api_status, int) and 400 <= api_status < 500:
                     status_code = api_status
            elif isinstance(refund_result["error"], str) and "423" in refund_result["error"]:
                 status_code = 423
                 
            return {
                "statusCode": status_code,
                "body": json.dumps({
                    "message": "Falha ao processar reembolso total.",
                    "id_Pedido": id_pedido, "payment_id": payment_id,
                    "error_details": refund_result
                }, default=decimal_default)
            }
        
        hora_atual = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Reembolso total para pedido {id_pedido} (payment_id {payment_id}) processado em {hora_atual}")

        try:
            novo_status_pedido = 'Reembolsado'
            id_reembolso_mp = refund_result.get('id')
            update_expression = "set status_pedido = :status, hora_atualizacao = :hora"
            expression_values = {':status': novo_status_pedido, ':hora': hora_atual}
            if id_reembolso_mp:
                update_expression += ", id_reembolso = :reembolso_id"
                expression_values[':reembolso_id'] = id_reembolso_mp
            
            table_order.update_item(
                Key={'id_Pedido': id_pedido},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values
            )
            print(f"Status do pedido {id_pedido} atualizado para {novo_status_pedido} no DynamoDB.")
        except Exception as update_ex:
            print(f"AVISO: Falha ao atualizar status do pedido {id_pedido} no DynamoDB após reembolso bem-sucedido: {update_ex}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Solicitação de reembolso total processada com sucesso e status do pedido atualizado.",
                "id_Pedido": id_pedido, "payment_id": payment_id,
                "refund_details": refund_result,
                "processed_at": hora_atual
            }, default=decimal_default)
        }

    except Exception as ex:
        print(f"Erro GERAL/INESPERADO no lambda_handler: {ex}")
        error_body = {"message": f"Erro interno inesperado no servidor: {str(ex)}"}
        if id_pedido_for_logging: error_body["id_Pedido"] = id_pedido_for_logging
        if payment_id_for_logging: error_body["payment_id_involved"] = payment_id_for_logging
        return {
            "statusCode": 500,
            "body": json.dumps(error_body, default=decimal_default)
        }
