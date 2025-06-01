import datetime
import json
import re
from dataclasses import dataclass
import boto3
import os
import uuid
import requests


def validate_cep(cep:str):
    try:
        response = requests.get(f"https://viacep.com.br/ws/{cep}/json/")
        if response.status_code != 200 or 'erro' in response.json():
            raise ValueError('Problema ao validar CEP informado')
    except Exception as ex:
        raise Exception(str(ex))
    
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
        if not isinstance(json_data['nome'], str):
            raise TypeError('nome deve ser uma string')
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
    nome_Loja: str = None
    descricao_Loja: str = None
    id_Imagem: str = None
    tipo_Entrega: str = None
    id_Endereco: int = None

    @staticmethod
    def from_json(json_data: dict):
        if 'cep' not in json_data:
            raise KeyError('cep')
        
        if not isinstance(json_data['cep'], str):
            raise TypeError('cep deve ser uma string')
        
        json_data['cep'] = re.sub(r'\D', '', json_data.get('cep', ''))
        if not re.match(r'^\d{8}$', json_data['cep']):
            raise ValueError('Formatação de CEP inválida')
        
        validate_cep(json_data['cep'])
        
        user_type = json_data['Usuario_Tipo']
        if user_type not in ['seller', 'customer', 'seller_customer']:
            raise ValueError('Tipo de usuário inválido')
        
        if not isinstance(json_data['logradouro'], str):
            raise TypeError('logradouro deve ser uma string')
        if not isinstance(json_data['numero'], str):
            raise TypeError('numero deve ser uma string')
        if not isinstance(json_data['complemento'], str):
            raise TypeError('complemento deve ser uma string')
        
        if json_data['Usuario_Tipo'] == 'customer':
            return Address_Store(
                cep=json_data['cep'],
                logradouro=json_data['logradouro'],
                numero=json_data['numero'],
                complemento=json_data['complemento'],
                id_Endereco=int(str((uuid.uuid4().int))[:18]),
            )
        else:
            if not isinstance(json_data['nome_Loja'], str):
                raise TypeError('nome_Loja deve ser uma string')
            if not isinstance(json_data['descricao_Loja'], str):
                raise TypeError('descricao_Loja deve ser uma string')
            if not isinstance(json_data['id_Imagem'], str):
                raise TypeError('id_Imagem deve ser uma string')
            if not isinstance(json_data['tipo_Entrega'], str):
                raise TypeError('tipo_Entrega deve ser uma string')
            
            return Address_Store(
                cep=json_data['cep'],
                logradouro=json_data['logradouro'],
                numero=json_data['numero'],
                complemento=json_data['complemento'],
                id_Endereco=int(str((uuid.uuid4().int))[:18]),
                nome_Loja=json_data['nome_Loja'],
                descricao_Loja=json_data['descricao_Loja'],
                id_Imagem=json_data['id_Imagem'],
                tipo_Entrega=json_data['tipo_Entrega']
            )
        

def lambda_handler(event:any, context:any): 
        try:
            body = json.loads(event['body'])
            user = User.from_json(body)
            address_store = Address_Store.from_json(body)
            user.fk_id_Endereco = address_store.id_Endereco

            dynamodb = boto3.resource('dynamodb')
            
            user_table = dynamodb.Table(os.environ['USER_TABLE'])
            if 'Item' in user_table.get_item(Key={'cpf': user.cpf}):
                raise ValueError('CPF já cadastrado')

            cognito = boto3.client('cognito-idp')
            cognito.sign_up(
                ClientId=os.environ['COGNITO_CLIENT_ID'],
                Username=user.email,
                Password=user.senha,
            )
            
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

            return {
                'statusCode': 200,
                'body': json.dumps(
                    {
                        'message': 'Registro criado com sucesso!',
                        "cpf": user.cpf,
                        "id_Endereco": user.fk_id_Endereco,
                    }, default=str)
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
        except TypeError as err:
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
            "nome": "Sergio Gabriel",
             "cpf": "",
            "Usuario_Tipo": "customer",
            "telefone": "+5511234567890",
            "email": "",
            "senha": "",
            "cep": "",
            "logradouro": "Rua das Flores",
            "numero": "12",
            "complemento": "Apto 102",
            "nome_Loja": "loja do sergio",
            "descricao_Loja": "descricao da loja do sergio",
            "id_Imagem": "",
            "tipo_Entrega": "entrega feita por mim"
        })
    }
    print(lambda_handler(event, None))