import json
import os
import boto3
import re

def is_valid_email(email: str) -> bool:
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_regex, email) is not None

def lambda_handler(event:any, context:any):
    try:
        email = event.get('queryStringParameters', {}).get('email')
        if not email:
            raise ValueError("email não fornecido")
        
        if not isinstance(email, str):
            raise TypeError("email deve ser uma string")
        
        if not is_valid_email(email):
            raise ValueError("email inválido")

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        response = table.query(
            IndexName='email-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('email').eq(email)
        )

        if 'Items' not in response or len(response['Items']) == 0:
            return {
            'statusCode': 404,
            'body': json.dumps({'message': "Usuário não encontrado"}, default=str)
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({"usuario": response['Items']}, default=str)
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
    os.environ['TABLE_NAME'] = 'Usuario'
    event = {
        'queryStringParameters': {
            'email': "sergioadm120@gmail.com"
        }
    }
    print(lambda_handler(event, None))