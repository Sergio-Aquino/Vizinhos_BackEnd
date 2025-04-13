import boto3
import os
import json

def lambda_handler(event: any, context: any):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])

    category_id = json.loads(event['body'])['id_Categoria'] if 'id_Categoria' in json.loads(event['body']) else None
    new_category_description = json.loads(event['body'])['descricao'] if 'descricao' in json.loads(event['body']) else None

    if not category_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message":"ID da categoria não informado!"}, default=str)
        }
    
    if not new_category_description:
        return {
            "statusCode": 400,
            "body": json.dumps({"message":"Descrição da categoria não informada!"}, default=str)
        }

    try:
        response = table.get_item(Key={'id_Categoria': category_id})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message":"Categoria não encontrada!"}, default=str)
            }

        table.update_item(
            Key={'id_Categoria': category_id},
            UpdateExpression="SET descricao = :descricao",
            ExpressionAttributeValues={
                ':descricao': new_category_description
            }
        )
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message":"Erro ao atualizar categoria: " + str(ex)}, default=str)
        }    
    
    return {
        "statusCode": 200,
        "body": json.dumps({"message":"Categoria atualizada com sucesso!"}, default=str)
    }

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Categoria'
    event = {
        "body": json.dumps({
            "id_Categoria": 1,
            "descricao": "Categoria Teste 2"
        })
    }
    print(lambda_handler(event, None))