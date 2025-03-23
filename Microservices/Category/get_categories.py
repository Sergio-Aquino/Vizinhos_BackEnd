import boto3
import os

def lambda_handler(event: any, context: any):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    
    try:
        all_categories = table.scan()
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": "Erro ao buscar categorias: " + str(ex)
        }

    return {
        "statusCode": 200,
        "body": all_categories['Items']
    }

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Categoria'
    print(lambda_handler(None, None))