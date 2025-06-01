import json
import boto3
import os

def lambda_handler(event:any, context:any):
    try:
        characteristic_id = json.loads(event['body'])['id_Caracteristica'] if 'id_Caracteristica' in json.loads(event['body']) else None
        new_characteristic_description = json.loads(event['body'])['descricao'] if 'descricao' in json.loads(event['body']) else None

        if not characteristic_id:
            raise ValueError("id_Caracteristica não fornecido.")
        
        if not new_characteristic_description:
            raise ValueError("descricao não fornecida.")
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        
        response = table.get_item(Key={'id_Caracteristica': characteristic_id})
        if 'Item' not in response:
           return {
                "statusCode": 404,
                "body": json.dumps({"message": "Característica não encontrada."}, default=str)
            }
        
        table.update_item(
            Key={'id_Caracteristica': characteristic_id},
            UpdateExpression="SET descricao = :descricao",
            ExpressionAttributeValues={
                ':descricao': new_characteristic_description
            }
        )
        return {
            "statusCode": 200,
            "body": json.dumps({"message":"Característica atualizada com sucesso!"}, default=str)
        }
    except ValueError as err:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(err)}, default=str)
        }
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message":"Erro ao atualizar característica: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = ''
    event = {
        "body": json.dumps({
            "id_Caracteristica": "",
            "descricao": "Característica Teste UPTADE"
        })
    }
    print(lambda_handler(event, None))