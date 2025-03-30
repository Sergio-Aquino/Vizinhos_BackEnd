import json
import boto3
import os

def lambda_handler(event:any, context:any):
    address_id = event.get("queryStringParameters", {}).get("id_Endereco")
    if not address_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "O parâmetro 'id_Endereco' é obrigatório!"}, default=str)
        }
    
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])

        response = table.get_item(
            Key={'id_Endereco': int(address_id)}
        )
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Endereço não encontrado!"}, default=str)
            }
        
        table.delete_item(
            Key={'id_Endereco': int(address_id)}
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Endereço deletado com sucesso!"}, default=str)
        }
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao deletar o endereço!", "error": str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Loja_Endereco'
    event = {
        "queryStringParameters": {
            "id_Endereco": "118457028588601987"
        }
    }
    print(lambda_handler(event, None))