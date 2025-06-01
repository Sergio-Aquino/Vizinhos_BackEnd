import json
import datetime
import boto3
import os
import re
from dataclasses import dataclass

@dataclass
class User:
    nome: str
    cpf: str
    Usuario_Tipo: str
    fk_id_Endereco: int
    telefone: str
    email: str
    senha: str
    data_cadastro: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def from_json(json_data: dict) -> 'User':        
        if not isinstance(json_data['nome'], str):
            raise TypeError('nome deve ser uma string')
        if not isinstance(json_data['fk_id_Endereco'], int):
            raise TypeError('fk_id_Endereco deve ser um inteiro')
        if not isinstance(json_data['cpf'], str):
            raise TypeError('cpf deve ser uma string')
        if not isinstance(json_data['Usuario_Tipo'], str):
            raise TypeError('Usuario_Tipo deve ser uma string')
        if not isinstance(json_data['telefone'], str):
            raise TypeError('telefone deve ser uma string')
        if not isinstance(json_data['email'], str):
            raise TypeError('email deve ser uma string')
            
        json_data['cpf'] = re.sub(r'\D', '', json_data.get('cpf', ''))
        if not re.match(r'^\d{11}$', json_data['cpf']):
            raise ValueError('Formatação de CPF inválida')
        
        telefone_pattern = r'^\+55\d{11}$'
        if not re.match(telefone_pattern, json_data['telefone']):
            raise ValueError('Formatação de telefone inválida. Deve seguir o padrão +5511954674532')

        return User(
            nome=json_data['nome'],
            cpf=json_data['cpf'],
            Usuario_Tipo=json_data['Usuario_Tipo'],
            fk_id_Endereco=json_data['fk_id_Endereco'],
            telefone=json_data['telefone'],
            email=json_data['email'],
            senha=json_data['senha']
        )

def lambda_handler(event:any, context:any):
    try:
        body = json.loads(event["body"])
        user = User.from_json(body)

        dynamodb = boto3.resource('dynamodb')
        user_table = dynamodb.Table(os.environ['USER_TABLE'])

        response = user_table.get_item(
            Key={'cpf': user.cpf}
        )

        if 'Item' in response:
            raise ValueError("CPF já cadastrado")
        
        address_store_table = dynamodb.Table(os.environ['ADDRESS_STORE_TABLE'])
        response = address_store_table.get_item(
            Key={'id_Endereco': user.fk_id_Endereco}
        )

        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Endereço não encontrado"})
            }

        cognito = boto3.client('cognito-idp')
        cognito.sign_up(
            ClientId=os.environ['COGNITO_CLIENT_ID'],
            Username=user.email,
            Password=user.senha,
        )

        user_table.put_item(
            Item={
                'cpf': user.cpf,
                'nome': user.nome,
                'Usuario_Tipo': user.Usuario_Tipo,
                'fk_id_Endereco': user.fk_id_Endereco,
                'telefone': user.telefone,
                'email': user.email,
                'data_cadastro': user.data_cadastro
            }
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Usuário criado com sucesso",
                    "id_Usuario": user.cpf,
                    "fk_id_Endereco": user.fk_id_Endereco
                }, 
                default=str)
        }
    except KeyError as err:
        return {
            "statusCode": 400,
            'body': json.dumps({'message': f'Campo obrigatório não informado: {str(err)}'}, default=str)
        }
    except ValueError as ve:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(ve)}, default=str)
        }
    except Exception as ex:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao criar usuário: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__": 
    os.environ['COGNITO_CLIENT_ID'] = '12rp435mgucks8jfndh1dufr0e'
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['ADDRESS_STORE_TABLE'] = 'Loja_Endereco'

    event = {
        "body": json.dumps({
            "nome": "Sergio",
            "cpf": "",
            "email": "",
            "Usuario_Tipo": "customer",
            "fk_id_Endereco": "",
            "telefone": "1234567890",
            "senha": ""
        })
    }
    print(lambda_handler(event, None))