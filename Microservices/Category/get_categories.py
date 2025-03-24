import boto3
import os
import json

def lambda_handler(event: any, context: any):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    
    try:
        all_categories = table.scan()
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message":"Erro ao buscar categorias: " + str(ex)}, default=str)
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"categorias":all_categories['Items']}, default=str)
    }

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Categoria'
    print(lambda_handler(None, None))