import random
import boto3
import os

def lambda_handler(event: any, context: any):
    category_description = event['descricao']
    category_id = random.randint(1, 1000)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    
    try:
        table.put_item(Item={
            "id_Categoria": category_id,
            "descricao": category_description
            })
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": "Erro ao criar categoria!"
        }
    
    return {
        "statusCode": 200,
        "body": "Categoria criada com sucesso!"
    }
    
if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Categoria'
    event = {
        "descricao": "Categoria de teste 5"
    }
    print(lambda_handler(event, None))