import json
import os
import boto3

def lambda_handler(event:any, context:any):
    try: 
        address_id = event.get('queryStringParameters', {}).get('id_Endereco')
        if not address_id:
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Campo obrigatório não informado: id_Endereco"}, default=str)
            }
        
        address_id = int(address_id)

        if not isinstance(address_id, int):
            raise ValueError("id_Endereco deve ser um inteiro!")
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])

        response = table.get_item(Key={'id_Endereco': address_id})
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "Endereço não encontrado"}, default=str)
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({"endereco":response['Item']}, default=str)
        }
    except ValueError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({"message": str(err)}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "Erro ao buscar endereço: " + str(ex)}, default=str)
        }
    
if __name__ == "__main__":
    os.environ['TABLE_NAME'] = ''
    event = {
        'queryStringParameters': {
            'id_Endereco': ""
        }
    }
    print(lambda_handler(event, None))