import json
import boto3
import os
import datetime

def lambda_handler(event:any, context:any): 
    try:
        id_Pedido = json.loads(event['body'])['id_Pedido'] if 'id_Pedido' in json.loads(event['body']) else None
        if id_Pedido is None:
            return {
                "statusCode": 400,
                "body": "id_Pedido não encontrado no corpo da requisição"
            }
        
        status = json.loads(event['body'])['status'] if 'status' in json.loads(event['body']) else None
        if status is None:
            return {
                "statusCode": 400,
                "body": "status não encontrado no corpo da requisição"
            }
        
        dynamoDB = boto3.resource('dynamodb')
        table_order = dynamoDB.Table(os.environ['TABLE_ORDER'])

        if table_order.get_item(Key={'id_Pedido': id_Pedido}) is None:
            return {
                "statusCode": 404,
                "body": json.dumps({f"Pedido {id_Pedido} não encontrado"}, default=str)
            }
        
        table_order.update_item(
            Key={'id_Pedido': id_Pedido},
            UpdateExpression="set status_pedido = :status_pedido, hora_atualizacao = :hora_atualizacao",
            ExpressionAttributeValues={
                ':status_pedido': status,
                ':hora_atualizacao': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Pedido {id_Pedido} atualizado com sucesso", "pedido": table_order.get_item(Key={'id_Pedido': id_Pedido})['Item']}, default=str)
        }
    except Exception as ex:
        print(f"Erro ao autualizar o status do pedido: {ex}")
        return {
            "statusCode": 500,
            "body": f"Erro ao autualizar o status do pedido {id_Pedido}: " + str(ex)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_ORDER'] = 'Pedido'
    event = {
        "body": json.dumps({
            "id_Pedido":"88d8dd8f-2d34-4f6b-af77-e14da8c0a242",
            "status": "Pago"
        })
    }
    context = {}
    print(lambda_handler(event, context))