import json
import boto3
import os

def lambda_handler(event:any, context:any): 
    cpf = json.loads(event['body'])['cpf'] if 'cpf' in json.loads(event['body']) else None
    if not cpf:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'CPF não informado'}, default=str)
        }
    
    new_user_type = json.loads(event['body'])['Usuario_Tipo'] if 'Usuario_Tipo' in json.loads(event['body']) else None
    if not new_user_type:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Tipo de usuário não informado'}, default=str)
        }
    
    new_id_address = json.loads(event['body'])['fk_id_Endereco'] if 'fk_id_Endereco' in json.loads(event['body']) else None
    if not new_id_address:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'ID do endereço não informado'}, default=str)
        }
    
    new_phone = json.loads(event['body'])['telefone'] if 'telefone' in json.loads(event['body']) else None
    if not new_phone:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Telefone não informado'}, default=str)
        }
    
    new_email = json.loads(event['body'])['email'] if 'email' in json.loads(event['body']) else None
    if not new_email:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Email não informado'}, default=str)
        }
    
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])

        response = table.get_item(Key={'cpf': cpf})
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Usuário não encontrado'}, default=str)
            }

        cognito = boto3.client('cognito-idp')
        cognito.admin_update_user_attributes(
            UserPoolId=os.environ['USER_POOL_ID'],
            Username=response['Item']['email'],
            UserAttributes=[
                {
                    'Name': 'email',
                    'Value': new_email
                },
                {
                    'Name': 'email_verified', 
                    'Value': 'true'
                }
            ]
        )

        table.update_item(
            Key={'cpf': cpf},
            UpdateExpression="SET Usuario_Tipo = :Usuario_Tipo, fk_id_Endereco = :fk_id_Endereco, telefone = :telefone, email = :email",
            ExpressionAttributeValues={
                ':Usuario_Tipo': new_user_type,
                ':fk_id_Endereco': new_id_address,
                ':telefone': new_phone,
                ':email': new_email
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message':'Usuário atualizado com sucesso!'}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message':'Erro ao atualizar usuário: ' + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Usuario'
    os.environ['USER_POOL_ID'] = 'us-east-2_K0dp1BUPW'
    os.environ['COGNITO_CLIENT_ID'] = '12rp435mgucks8jfndh1dufr0e'

    event = {
        "body": json.dumps({
            "cpf": "25563678512",
            "Usuario_Tipo": "seller",
            "fk_id_Endereco": 123456789,
            "telefone": "40028922",
            "email": "aquino.lima@aluno.ifsp.edu.br"
        })
    }

    print(lambda_handler(event, None))