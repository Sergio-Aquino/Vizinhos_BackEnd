import json
import os
import boto3

def lambda_handler(event:any, context:any):
    user_id = event.get('queryStringParameters', {}).get('cpf')
    if not user_id:
        return {
            'statusCode': 400,
            'body': 'ID usuário não fornecido'
        }
    
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        response = table.get_item(Key={'cpf': user_id})

        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Usuário não encontrado'}, default=str)
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps(response['Item'], default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro ao buscar usuário: ' + str(ex)}, default=str)
        }
    
if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Usuario'
    event = {
        'queryStringParameters': {
            'cpf': "12345678901"
        }
    }
    print(lambda_handler(event, None))