import json
import boto3
import os

def lambda_handler(event:any, context:any):
    try:
        email = json.loads(event['body'])['email'] if 'email' in json.loads(event['body']) else None
        if not email:
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Email não informado!"}, default=str)
            }
        
        if not isinstance(email, str): 
            raise TypeError("Email deve ser uma string")
        
        password = json.loads(event['body'])['senha'] if 'senha' in json.loads(event['body']) else None
        if not password:
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Senha não informada!"}, default=str)
            }

        cognito = boto3.client('cognito-idp')
        response = cognito.initiate_auth(
            ClientId=os.environ['COGNITO_CLIENT_ID'],
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters={
                'USERNAME': email,
                'PASSWORD': password
            })

        return {
            'statusCode': 200,
            'body': json.dumps({
                "AccessToken": f"{response['AuthenticationResult']['AccessToken']}",
                "idToken": f"{response['AuthenticationResult']['IdToken']}",
                "refreshToken": f"{response['AuthenticationResult']['RefreshToken']}",
                "expiresIn": f"{response['AuthenticationResult']['ExpiresIn']}"
            }, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 400,
            'body': json.dumps({"message": "Erro ao logar usuário: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['COGNITO_CLIENT_ID'] = ''
    event = {
        "body": json.dumps({
            "email":"",
            "senha":""
        })
    }
    print(lambda_handler(event, None))
