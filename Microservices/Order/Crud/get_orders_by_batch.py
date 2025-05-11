import json
import os
import boto3

def lambda_handler(event:any, context:any):
    try:
        id_Lote = event.get('queryStringParameters', {}).get('id_Lote', None)
        if not id_Lote:
            print("ID do lote não informado")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'campo obrigatório não informado: id_Lote'})
            }
        
        table_lote = boto3.resource('dynamodb').Table(os.environ['LOTES_TABLE'])
        response_lote = table_lote.get_item(Key={'id_Lote': id_Lote})

        if 'Item' not in response_lote:
            print(f"Lote {id_Lote} não encontrado")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Lote não encontrado'})
            }
        
        table_order = boto3.resource('dynamodb').Table(os.environ['ORDERS_TABLE'])
        response_orders = table_order.query(
            IndexName='fk_Lote_id_Lote-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_Lote_id_Lote').eq(id_Lote)
        )

        if 'Items' in response_orders:
            return {
                'statusCode': 200,
                'body': json.dumps({"pedidos":response_orders['Items']}, default=str)
            }
        return None
    except Exception as ex:
        print(f"Erro ao buscar produtos: {str(ex)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro ao busacar produtos: ' + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['LOTES_TABLE'] = 'Produto_Lote'
    os.environ['ORDERS_TABLE'] = 'Pedido'
    event = {
        'queryStringParameters': {
            'id_Lote': "5c07dfdd-c929-455c-8a16-6091a3d4868f"
        }
    }
    print(lambda_handler(event, None))