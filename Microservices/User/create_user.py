import json
import datetime
import boto3
import os
import re

def lambda_handler(event:any, context:any):
    name = json.loads(event['body'])['nome'] if 'nome' in json.loads(event['body']) else None
    if not name:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Nome não informado'}, default=str)
        }

    cpf = json.loads(event['body'])['cpf'] if 'cpf' in json.loads(event['body']) else None
    if not cpf:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'CPF não informado'}, default=str)
        }
    
    cpf = re.sub(r'\D', '', cpf) 
    if not re.match(r'^\d{11}$', cpf):
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Formatação de CPF inválida'}, default=str)
        }
    
    user_type = json.loads(event['body'])['Usuario_Tipo'] if 'Usuario_Tipo' in json.loads(event['body']) else None
    if not user_type:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Tipo de usuário não informado'}, default=str)
        }
    
    id_address = json.loads(event['body'])['fk_id_Endereco'] if 'fk_id_Endereco' in json.loads(event['body']) else None
    if not id_address:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'ID do endereço não informado'}, default=str)
        }
    
    phone = json.loads(event['body'])['telefone'] if 'telefone' in json.loads(event['body']) else None
    if not phone:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Telefone não informado'}, default=str)
        }
    
    email = json.loads(event['body'])['email'] if 'email' in json.loads(event['body']) else None
    if not email:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Email não informado'}, default=str)
        }
    
    password = json.loads(event['body'])['senha'] if 'senha' in json.loads(event['body']) else None
    if not password:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Senha não informada'}, default=str)
        }
    
    register_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        cognito = boto3.client('cognito-idp')
        cognito.sign_up(
            ClientId=os.environ['COGNITO_CLIENT_ID'],
            Username=email,
            Password=password,
        )

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('Usuario')

        table.put_item(
            Item={
                'nome': name,
                'cpf': cpf,
                'Usuario_Tipo': user_type,
                'fk_id_Endereco': id_address,
                'telefone': phone,
                'email': email,
                'data_cadastro': register_date
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message':'Usuário criado com sucesso!'}, default=str)
        }
    
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message':'Erro ao criar usuário: ' + str(ex)}, default=str)
        }
    

if __name__ == "__main__": 
    os.environ['COGNITO_CLIENT_ID'] = '12rp435mgucks8jfndh1dufr0e'
    os.environ['TABLE_NAME'] = 'Usuario'

    event = {
        "body": json.dumps({
            "nome": "Sergio",
            "cpf": "588.851.245.-40",
            "Usuario_Tipo": "customer",
            "fk_id_Endereco": 199297594630771120,
            "telefone": "1234567890",
            "email": "aquino.lima@aluno.ifsp.edu.br",
            "senha": "MinhaSenha123#"
        })
    }
    print(lambda_handler(event, None))