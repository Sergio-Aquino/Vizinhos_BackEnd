import json
import boto3
import os

def lambda_handler(event:any, context:any):
    try:
        characteristic_id = event.get("queryStringParameters", {}).get("id_Caracteristica")
        if not characteristic_id:
            raise ValueError("id_Caracteristica não informado")
        
        if not isinstance(characteristic_id, int):
            raise ValueError("id_Caracteristica deve ser um inteiro")
            
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ["TABLE_NAME"])

        response = table.get_item(Key={'id_Caracteristica':characteristic_id})
        if 'Item' not in response:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"message": "Característica não encontrada"})
                }
            
        table.delete_item(Key={'id_Caracteristica': characteristic_id})
            
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Característica deletada com sucesso!"})
         }
    except ValueError as err:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(err)}, default=str)
        }
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao deletar característica: " + str(ex)}, default=str)
        }



if __name__ == "__main__":
    os.environ["TABLE_NAME"] = "Caracteristica"
    event = {
        "queryStringParameters": {
            "id_Caracteristica": 20
        }
    }
    print(lambda_handler(event, None))
