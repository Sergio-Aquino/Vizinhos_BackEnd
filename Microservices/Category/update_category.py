import boto3
import os

def lambda_handler(event: any, context: any):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])

    category_id = event['id_Categoria']
    new_category_description = event['descricao']

    try:
        response = table.get_item(Key={'id_Categoria': category_id})

        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": "Categoria não encontrada!"
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
            "body": "Erro ao atualizar categoria: " + str(ex)
        }    
    
    return {
        "statusCode": 200,
        "body": "Categoria atualizada com sucesso!"
    }

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Categoria'
    event = {
        "id_Categoria": 418,
        "descricao": "Categoria atualizada"
    }
    print(lambda_handler(event, None))