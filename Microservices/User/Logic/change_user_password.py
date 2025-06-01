import json
import boto3
import os

def lambda_handler(event, context):
    try:
        email = json.loads(event['body']).get('email')
        confirmation_code = json.loads(event['body']).get('confirmation_code')
        new_password = json.loads(event['body']).get('new_password')

        if not email:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Email não informado'}, default=str)
            }
        
        if not isinstance(email, str): 
            raise TypeError("Email deve ser uma string")
        

        if not confirmation_code:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Código de confirmação para troca de senha não informado'}, default=str)
            }

        if not new_password:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Nova senha não informada'}, default=str)
            }
        
        cognito = boto3.client('cognito-idp')

        cognito.confirm_forgot_password(
            ClientId=os.environ['COGNITO_CLIENT_ID'],
            Username=email,
            ConfirmationCode=confirmation_code,
            Password=new_password
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Senha redefinida com sucesso!'}, default=str)
        }

    except TypeError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Erro ao redefinir senha: {str(ex)}'}, default=str)
        }

if __name__ == "__main__":
    os.environ['COGNITO_CLIENT_ID'] = '12rp435mgucks8jfndh1dufr0e'

    event = {
        "body": json.dumps({
            "email": "",
            "confirmation_code": "",
            "new_password": ""
        })
    }

    print(lambda_handler(event, None))