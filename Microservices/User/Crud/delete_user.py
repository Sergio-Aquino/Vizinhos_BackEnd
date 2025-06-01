import json
import boto3
import os

def lambda_handler(event:any, context:any):
    try: 
        user_id = event.get("queryStringParameters", {}).get("cpf")
        if not user_id:
            raise ValueError("CPF não fornecido")
        
        if not isinstance(user_id, str):
            raise TypeError("CPF deve ser uma string")
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        response = table.get_item(Key={'cpf': user_id})

        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Usuário não encontrado."}, default=str)
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
    except ValueError as err:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(err)}, default=str)
        }
    except TypeError as err:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(err)}, default=str)
        }
    
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao deletar usuário: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['USER_POOL_ID'] = ''
    os.environ['TABLE_NAME'] = ''
    event = {
        "queryStringParameters": {
            "cpf": ""
        }
    }
    print(lambda_handler(event, None))
    



    
