import json
import boto3
import os

def lambda_handler(event:any, context:any):
    try:
        email = json.loads(event['body'])['email'] if 'email' in json.loads(event['body']) else None
        if not email:
            return {
                'statusCode': 400,
                'body': json.dumps({'message':'Email não informado'}, default=str)
            }
        
        if not isinstance(email, str): 
            raise TypeError("Email deve ser uma string")
    
        cognito = boto3.client('cognito-idp')
        cognito.admin_reset_user_password(
            UserPoolId=os.environ['USER_POOL_ID'],
            Username=email
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message':'Email  de troca de senha enviado com sucesso'}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message':f'Erro ao enviar pedido de troca de senha: {str(ex)}'}, default=str)
        }

if __name__ == "__main__":
    os.environ['USER_POOL_ID'] = ''
    event = {
        "body": json.dumps({
            "email": ""
        })
    }
    print(lambda_handler(event, None))

