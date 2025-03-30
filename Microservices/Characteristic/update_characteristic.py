import json
import boto3
import os

def lambda_handler(event:any, context:any):
    characteristic_id = json.loads(event['body'])['id_Caracteristica'] if 'id_Caracteristica' in json.loads(event['body']) else None
    new_characteristic_description = json.loads(event['body'])['descricao'] if 'descricao' in json.loads(event['body']) else None

    if not characteristic_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message":"ID da característica não informado!"}, default=str)
        }
    
    if not new_characteristic_description:
        return {
            "statusCode": 400,
            "body": json.dumps({"message":"Descrição da característica não informada!"}, default=str)
        }
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    
    try:
        response = table.get_item(Key={'id_Caracteristica': int(characteristic_id)})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message":"Característica não encontrada!"}, default=str)
            }
        
        table.update_item(
            Key={'id_Caracteristica': int(characteristic_id)},
            UpdateExpression="SET descricao = :descricao",
            ExpressionAttributeValues={
                ':descricao': new_characteristic_description
            }
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message":"Característica atualizada com sucesso!"}, default=str)
        }
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message":"Erro ao atualizar característica: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Caracteristica'
    event = {
        "body": json.dumps({
            "id_Caracteristica": 395763845136307662,
            "descricao": "Característica Teste UPTADE"
        })
    }
    print(lambda_handler(event, None))