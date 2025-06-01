import json
import os
import boto3

def lambda_handler(event:any, context:any):
    try:
        user_id = event.get('queryStringParameters', {}).get('cpf')
        if not user_id:
            raise ValueError("CPF não fornecido")
        
        if not isinstance(user_id, str):
            raise TypeError("CPF deve ser uma string")

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        response = table.get_item(Key={'cpf': user_id})

        if 'Item' not in response:
            return {
            'statusCode': 404,
            'body': json.dumps({'message': "Usuário não encontrado"}, default=str)
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({"usuario": response['Item']}, default=str)
        }
    except ValueError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str)
        }
    except TypeError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro ao buscar usuário: ' + str(ex)}, default=str)
        }
    
if __name__ == "__main__":
    os.environ['TABLE_NAME'] = ''
    event = {
        'queryStringParameters': {
            'cpf': ""
        }
    }
    print(lambda_handler(event, None))