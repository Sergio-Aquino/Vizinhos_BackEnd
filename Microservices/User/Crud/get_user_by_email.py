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
        user_table = dynamodb.Table(os.environ['TABLE_NAME'])
        response_user = user_table.query(
            IndexName='email-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('email').eq(email)
        )

        if 'Items' not in response_user or len(response_user['Items']) == 0:
            return {
            'statusCode': 404,
            'body': json.dumps({'message': "Usuário não encontrado"}, default=str)
            }

        address_id = int(response_user['Items'][0]['fk_id_Endereco'])

        address_table = dynamodb.Table(os.environ['ADRESS_STORE_TABLE'])
        response_address = address_table.get_item(Key={'id_Endereco': address_id})

        if 'Item' not in response_address:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': "Endereço de usuário não encontrado"}, default=str)
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps(
                {
                    "usuario": response_user['Items'][0],
                    "endereco": response_address['Item']
                 }, 
                default=str)
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
    os.environ["ADRESS_STORE_TABLE"] = 'Loja_Endereco'
    event = {
        'queryStringParameters': {
            'email': "sergioadm120@gmail.com"
        }
    }
    print(lambda_handler(event, None))