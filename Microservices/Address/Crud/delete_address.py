import json
import boto3
import os

def lambda_handler(event:any, context:any):
    try:
        address_id = event.get("queryStringParameters", {}).get("id_Endereco")
        if not address_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "id_Endereco não informado!"}, default=str)
            }
        
        if not isinstance(address_id, int):
            raise ValueError("id_Endereco deve ser um inteiro!")
    
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])

        response = table.get_item(
            Key={'id_Endereco': address_id}
        )
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Endereço não encontrado"}, default=str)
            }
        
        table.delete_item(
            Key={'id_Endereco': address_id}
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Endereço deletado com sucesso!"}, default=str)
        }
    except ValueError as err:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(err)}, default=str)
        }
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao deletar o endereço!", "error": str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Loja_Endereco'
    event = {
        "queryStringParameters": {
            "id_Endereco": 489864014224531116
        }
    }
    print(lambda_handler(event, None))