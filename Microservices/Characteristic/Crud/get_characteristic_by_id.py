import json
import boto3
import os

def lambda_handler(event:any, context:any):
    characteristic_id = event.get("queryStringParameters", {}).get("id_Caracteristica")
    if not characteristic_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message":"O parâmetro 'id_Caracteristica' é obrigatório!"}, default=str)
        }
    
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])

        response = table.get_item(Key={"id_Caracteristica": int(characteristic_id)})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message":"Característica não encontrada!"})
            }
                
        return {
            "statusCode": 200,
            "body": json.dumps(response['Item'], default=str)
        }
    except Exception as ex:
        return {
                "statusCode": 500,
                "body": json.dumps({"message":"Erro ao buscar característica: " + str(ex)}, default=str)
            }


if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Caracteristica'
    event = {
        "queryStringParameters": {
            "id_Caracteristica": 395763845136307662
        }
    }
    print(lambda_handler(event, None))
