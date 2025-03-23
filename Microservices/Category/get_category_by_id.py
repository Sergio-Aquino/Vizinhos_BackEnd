import boto3
import os

def lambda_handler(event: any, context: any):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    category_id = event['id_Categoria']

    try:
        response = table.get_item(Key={'id_Categoria': category_id})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": "Categoria não encontrada!"
            }
        category = table.get_item(Key={'id_Categoria': category_id})
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": "Erro ao buscar categoria: " + str(ex)
        }
    
    return {
        "statusCode": 200,
        "body": category['Item']
    }

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Categoria'
    event = {
        "id_Categoria": 418
    }
    print(lambda_handler(event, None))