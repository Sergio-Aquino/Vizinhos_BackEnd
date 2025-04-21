import json
import uuid
import boto3
import os

def lambda_handler(event:any, context:any):
    try:
        characteristic_description = json.loads(event['body'])['descricao'] if 'descricao' in json.loads(event['body']) else None
        if not characteristic_description:
            raise ValueError("Descrição não informada")
        
        if not isinstance(characteristic_description, str):
            raise ValueError("Descrição deve ser uma string")
        
        characteristic_id = str((uuid.uuid4()))

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        table.put_item(Item={
            "id_Caracteristica": characteristic_id,
            "descricao": characteristic_description
        })

        return {
            "statusCode": 200,
            "body": json.dumps({"message":"Característica criada com sucesso!"}, default=str)
        }
    except ValueError as err:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(err)}, default=str)
        }
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message":"Erro ao criar característica: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Caracteristica'
    event = {
        "body": json.dumps({
            "descricao": "Caracteristica Teste"
        })
    }
    print(lambda_handler(event, None))
