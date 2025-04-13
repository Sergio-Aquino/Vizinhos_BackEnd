import boto3
import os
import json

def lambda_handler(event: any, context: any):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    category_id = event.get("queryStringParameters", {}).get("id_Categoria")

    if not category_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "O parâmetro 'id_Categoria' é obrigatório!"}, default=str)
        }
    
    try:
        response = table.get_item(Key={'id_Categoria': int(category_id)})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message":"Categoria não encontrada!"})
            }

        table.delete_item(Key={'id_Categoria': int(category_id)})
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao deletar categoria: " + str(ex)})
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Categoria deletada com sucesso!"})
    }

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Categoria'
    event = {
        "queryStringParameters": {
            "id_Categoria": "55"
        }
    }
    print(lambda_handler(event, None))