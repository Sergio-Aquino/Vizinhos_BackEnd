import random
import boto3
import os
import json

def lambda_handler(event: any, context: any):
    category_description = json.loads(event['body'])['descricao'] if 'descricao' in json.loads(event['body']) else None
    if not category_description:
        return {
            "statusCode": 400,
            "body": json.dumps({"message":"Descrição não informada!"}, default=str)
        }

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
            "body": json.dumps({"message":"Erro ao criar categoria: " + str(ex)}, default=str)
        }
    
    return {
        "statusCode": 200,
        "body": json.dumps({"message":"Categoria criada com sucesso!"}, default=str)
    }
    
if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Categoria'
    event = {
        "body": json.dumps({
            "descricao": "Categoria Teste"
        })
    }
    print(lambda_handler(event, None))