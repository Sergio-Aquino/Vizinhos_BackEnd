import json
import os
import boto3

def lambda_handler(event:any, context:any):
    try:
        id_pedido = event.get('queryStringParameters', {}).get('id_Pedido')
        if not id_pedido:
            print("ID do pedido não informado")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'ID do pedido não informado'})
            }
        
        dynamodb = boto3.resource('dynamodb')
        table_order = dynamodb.Table(os.environ['TABLE_ORDER'])
        response = table_order.get_item(Key={'id_Pedido': id_pedido})

        if 'Item' in response:
            table_order.delete_item(Key={'id_Pedido': id_pedido})
            print(f"Pedido deletado com sucesso: {id_pedido}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Pedido deletado com sucesso'})
            }
        else:
            print(f"Pedido não encontrado: {id_pedido}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Pedido não encontrado'})
            }
    except KeyError as err:
        print(f"Chave não encontrada: {str(err)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Chave não encontrada: ' + str(err)}, default=str)
        }
    except Exception as ex:
        print(f"Erro ao deletar pedido {id_pedido}: {str(ex)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro ao deletar pedido: ' + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_ORDER'] = 'Pedido'
    event = {
        "queryStringParameters": {
            "id_Pedido": "8376f2bb-813e-430e-b591-1fcaf6fee62a"
        }
    }
    print(lambda_handler(event, None))