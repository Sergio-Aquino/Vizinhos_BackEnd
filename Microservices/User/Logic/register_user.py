import datetime
import json
import re
from dataclasses import dataclass
import boto3
import os
import uuid

@dataclass
class User:
    nome: str
    cpf: str
    Usuario_Tipo: str
    telefone: str
    email: str
    senha: str
    data_cadastro: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fk_id_Endereco: int = None

    @staticmethod
    def from_json(json_data: dict) -> 'User':
        json_data['cpf'] = re.sub(r'\D', '', json_data.get('cpf', ''))
        if not re.match(r'^\d{11}$', json_data['cpf']):
            raise ValueError('Formatação de CPF inválida')

        return User(
            nome=json_data['nome'],
            cpf=json_data['cpf'],
            Usuario_Tipo=json_data['Usuario_Tipo'],
            telefone=json_data['telefone'],
            email=json_data['email'],
            senha=json_data['senha']
        )
    
@dataclass
class Address_Store:
    cep: str
    logradouro: str
    numero: str
    complemento: str
    nome_Loja: str
    descricao_Loja: str
    id_Imagem: str
    tipo_Entrega: str
    id_Endereco: int = int(str((uuid.uuid4().int))[:18])

    @staticmethod
    def from_json(json_data: dict):
        json_data['cep'] = re.sub(r'\D', '', json_data.get('cep', ''))
        if not re.match(r'^\d{8}$', json_data['cep']):
            raise ValueError('Formatação de CEP inválida')

        return Address_Store(
            cep=json_data['cep'],
            logradouro=json_data['logradouro'],
            numero=json_data['numero'],
            complemento=json_data['complemento'],
            id_Endereco=int(str((uuid.uuid4().int))[:18]),
            nome_Loja=json_data.get('nome_Loja', None),
            descricao_Loja=json_data.get('descricao_Loja', None),
            id_Imagem=json_data.get('id_Imagem', None),
            tipo_Entrega=json_data.get('tipo_Entrega', None)
        )

def lambda_handler(event:any, context:any): 
        try:
            body = json.loads(event['body'])
            user = User.from_json(body)
            address_store = Address_Store.from_json(body)
            user.fk_id_Endereco = address_store.id_Endereco

            cognito = boto3.client('cognito-idp')
            cognito.sign_up(
                ClientId=os.environ['COGNITO_CLIENT_ID'],
                Username=user.email,
                Password=user.senha,
            )
            
            dynamodb = boto3.resource('dynamodb')

            address_store_table = dynamodb.Table(os.environ['ADDRESS_STORE_TABLE'])

            address_store_item = {
                'id_Endereco': address_store.id_Endereco,
                'cep': address_store.cep,
                'logradouro': address_store.logradouro,
                'numero': address_store.numero,
                'complemento': address_store.complemento,
            }

            if user.Usuario_Tipo != 'customer':
                address_store_item.update({
                    'nome_Loja': address_store.nome_Loja,
                    'descricao_Loja': address_store.descricao_Loja,
                    'id_Imagem': address_store.id_Imagem,
                    'tipo_Entrega': address_store.tipo_Entrega
                })

            address_store_table.put_item(Item=address_store_item)

            user_table = dynamodb.Table(os.environ['USER_TABLE'])
            user_table.put_item(
                Item={
                    'nome': user.nome,
                    'cpf': user.cpf,
                    'Usuario_Tipo': user.Usuario_Tipo,
                    'fk_id_Endereco': user.fk_id_Endereco,
                    'telefone': user.telefone,
                    'email': user.email,
                    'data_cadastro': user.data_cadastro
                }
            )

            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Registro criado com sucesso!'}, default=str)
            }

        except KeyError as err:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': f'Campo obrigatório não informado: {str(err)}'}, default=str)
            }
        except ValueError as err:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': str(err)}, default=str)
            }
        except Exception as ex:
            return {
                'statusCode': 500,
                'body': json.dumps({'message': 'Erro ao criar registro: ' + str(ex)}, default=str)
            }


if __name__ == "__main__":
    os.environ['COGNITO_CLIENT_ID'] = '12rp435mgucks8jfndh1dufr0e'
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['ADDRESS_STORE_TABLE'] = 'Loja_Endereco'

    event = {
        "body": json.dumps({
            "nome": "Sergio",
            "cpf": "686.451.145-70",
            "Usuario_Tipo": "seller",
            "telefone": "1234567890",
            "email": "aquino.lima@aluno.ifsp.edu.br",
            "senha": "MinhaSenha123#",
            "cep": "12345678",
            "logradouro": "Rua Teste 2",
            "numero": "1234",
            "complemento": "Apto 102",
            "nome_Loja": "Loja Teste",
            "descricao_Loja": "Descrição da loja teste",
            "id_Imagem": "https://us-east-2.console.aws.amazon.com/s3/object/loja-profile-pictures?region=us-east-2&bucketType=general&prefix=37dc297e-527b-4744-8f00-95a3bb4d25dd.jpg",
            "tipo_Entrega": "Entrega rápida"
        })
    }
    print(lambda_handler(event, None))