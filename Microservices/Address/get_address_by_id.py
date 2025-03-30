import json
import os
import boto3

def lamda_handler(event:any, context:any):
    address_id = event.get('queryStringParameters', {}).get('id_Endereco')
    if not address_id:
        return {
            'statusCode': 400,
            'body': json.dumps({"message": "id_Endereco não informado"}, default=str)
        }
    
    try: 
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])

        response = table.get_item(Key={'id_Endereco': int(address_id)})
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "Endereço não encontrado"}, default=str)
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps(response['Item'], default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "Erro ao buscar endereço: " + str(ex)}, default=str)
        }
    
if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Loja_Endereco'
    event = {
        'queryStringParameters': {
            'id_Endereco': 199297594630771120
        }
    }
    print(lamda_handler(event, None))