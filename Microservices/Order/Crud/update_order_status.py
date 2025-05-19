import json
import boto3
import os
import datetime
import mercadopago


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

def refresh_status_pagamento(access_token: str, order: dict) -> None:
    id_Pagamento = order['id_Pagamento']
    if id_Pagamento is None:
        raise ValueError("id_Pagamento não encontrado no pedido")
    
    sdk = mercadopago.SDK(access_token)
    payment = sdk.payment().get(id_Pagamento)
    
    order['status_pedido'] = map_status_pagamento(payment['response']['status'])
    if order['status_pedido'] == "Pago":
        order['id_Transacao'] = payment['response']['transaction_details']['transaction_id']

    order['hora_atualizacao'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def lambda_handler(event:any, context:any): 
    try:
        id_Pedido = json.loads(event['body'])['id_Pedido'] if 'id_Pedido' in json.loads(event['body']) else None
        if id_Pedido is None:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "id_Pedido não encontrado no corpo da requisição"})
            }
        
        dynamoDB = boto3.resource('dynamodb')
        table_order = dynamoDB.Table(os.environ['TABLE_ORDER'])

        order = table_order.get_item(Key={'id_Pedido': id_Pedido})
        if 'Item' not in order:
            return {
                "statusCode": 404,
                "body": json.dumps({f"Pedido {id_Pedido} não encontrado"}, default=str)
            }

        order = order['Item']
        store_id = order['fk_id_Endereco']
        table_store = dynamoDB.Table(os.environ['TABLE_STORE'])
        store = table_store.get_item(Key={'id_Endereco': store_id})
        if 'Item' not in store:
            return {
                "statusCode": 404,
                "body": json.dumps({f"Loja {store_id} não encontrada"}, default=str)
            }
        
        store = store['Item']
        access_token = store['access_token']

        refresh_status_pagamento(access_token, order)

        table_order.put_item(Item=order)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Pedido {id_Pedido} atualizado com sucesso", "pedido": table_order.get_item(Key={'id_Pedido': id_Pedido})['Item']}, default=str)
        }
    except ValueError as err:
        print(str(err))
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(err)})
        }
    except KeyError as err:
        print(f"Campo obrigatório não encontrado: {err}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Campo obrigatório não encontrado: {err}"})
        }
    except Exception as ex:
        print(f"Erro ao autualizar o status do pedido: {ex}")
        return {
            "statusCode": 500,
            "body": f"Erro ao autualizar o status do pedido {id_Pedido}: " + str(ex)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_ORDER'] = 'Pedido'
    os.environ['TABLE_STORE'] = 'Loja_Endereco'
    event = {
        "body": json.dumps({
            "id_Pedido":"e2e45550-909f-4e8b-8fcb-b0933d263e0d"
        })
    }
    context = {}
    print(lambda_handler(event, context))