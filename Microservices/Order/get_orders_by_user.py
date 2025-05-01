import json
import os
import boto3

def lambda_handler(event:any, context:any):
    try:
        cpf = event.get('queryStringParameters', {}).get('cpf', None)
        if not cpf:
            print("CPF não informado")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'campo obrigatório não informado: CPF'})
            }
        
        table_user = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
        response_user = table_user.get_item(Key={'cpf': cpf})

        if 'Item' not in response_user:
            print(f"Usuário {cpf} não encontrado")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'CPF não encontrado'})
            }
        
        table_order = boto3.resource('dynamodb').Table(os.environ['ORDERS_TABLE'])
        response_orders = table_order.query(
            IndexName='fk_Usuario_cpf-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_Usuario_cpf').eq(cpf)
        )

        if 'Items' in response_orders:
            return {
                'statusCode': 200,
                'body': json.dumps({"pedidos": response_orders['Items']}, default=str)
            }
        return None
    except Exception as ex:
        print(f"Erro ao buscar produtos: {str(ex)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro ao busacar produtos: ' + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['ORDERS_TABLE'] = 'Pedido'
    event = {
        'queryStringParameters': {
            'cpf': "48812172830"
        }
    }
    print(lambda_handler(event, None))