import json
import boto3
import os

def lambda_handler(event:any, context:any):
    category_id = event.get("queryStringParameters", {}).get("id_Categoria")
    if not category_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message":"O parâmetro 'id_Categoria' é obrigatório!"}, default=str)
        }

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])

    try:
        response = table.get_item(Key={'id_Categoria': int(category_id)})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message":"Categoria não encontrada!"}) 
            }

        return {
            "statusCode": 200,
            "body": json.dumps(response['Item'], default=str)
        }

    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message":"Erro ao buscar categoria: " + str(ex)}, default=str)
        }



if __name__ == "__main__":
    os.environ['TABLE_NAME'] = ''

    event = {
        "queryStringParameters": {
            "id_Categoria": ""
        }
    }
    print(lambda_handler(event, None))