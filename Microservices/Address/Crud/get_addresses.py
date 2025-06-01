import json
import boto3
import os

def lambda_handler(event:any, context:any):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        all_addresses = table.scan()
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message":"Erro ao retornar endereços: " + str(ex)}, default=str)
        }
    
    return {
        "statusCode": 200,
        "body": json.dumps({"enderecos": all_addresses['Items']}, default=str)
    }


if __name__ == "__main__":
    os.environ['TABLE_NAME'] = ''
    print(lambda_handler(None, None))