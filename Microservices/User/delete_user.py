import json
import boto3
import os

def lambda_handler(event:any, context:any): 
    user_id = event.get("queryStringParameters", {}).get("cpf")
    if not user_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "CPF não informado"}, default=str)
        }
    
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        response = table.get_item(Key={'cpf': user_id})

        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Usuário não encontrado"}, default=str)

            }
        user_email = response['Item']['email']
        
        cognito = boto3.client('cognito-idp')
        cognito.admin_delete_user(
            UserPoolId=os.environ['USER_POOL_ID'],
            Username=user_email
        )

        table.delete_item(Key={'cpf': user_id})

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Usuário deletado com sucesso!"}, default=str)
        }
        
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao deletar usuário: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['USER_POOL_ID'] = 'us-east-2_K0dp1BUPW'
    os.environ['TABLE_NAME'] = 'Usuario'
    event = {
        "queryStringParameters": {
            "cpf": "12345678901"
        }
    }
    print(lambda_handler(event, None))
    



    
