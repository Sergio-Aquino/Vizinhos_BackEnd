import json
import os
import uuid
import boto3
import re
import requests
from dataclasses import dataclass


def validate_cep(cep:str):
    try:
        response = requests.get(f"https://viacep.com.br/ws/{cep}/json/")
        if response.status_code != 200 or 'erro' in response.json():
            raise ValueError('Problema ao validar CEP informado')
    except Exception as ex:
        raise Exception(str(ex))

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
        body = json.loads(event["body"])
        address_store = Address_Store.from_json(body)

        dynamodb = boto3.resource('dynamodb')
        address_store_table = dynamodb.Table(os.environ['TABLE_NAME'])

        if body['Usuario_Tipo'] == 'customer':
            address_store_table.put_item(
                Item={
                    'id_Endereco': address_store.id_Endereco,
                    'cep': address_store.cep,
                    'logradouro': address_store.logradouro,
                    'numero': address_store.numero,
                    'complemento': address_store.complemento,
                }
            )
        else:
            address_store_table.put_item(
                Item={
                    'id_Endereco': address_store.id_Endereco,
                    'cep': address_store.cep,
                    'logradouro': address_store.logradouro,
                    'numero': address_store.numero,
                    'complemento': address_store.complemento,
                    'nome_Loja': address_store.nome_Loja,
                    'descricao_Loja': address_store.descricao_Loja,
                    'id_Imagem': address_store.id_Imagem,
                    'tipo_Entrega': address_store.tipo_Entrega
                }
            )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Dados salvos com sucesso!",
                    "id_Endereco": address_store.id_Endereco,
                }
            )
        }
    except KeyError as err:
        return {
            "statusCode": 400,
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
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao salvar dados: " + str(ex)})
        }

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = ''
    event = {
        "body": json.dumps({
            "cep": "08583620",
            "logradouro": "logradouro sergio",
            "numero": "43",
            "complemento": "casa 1",
            "Usuario_Tipo": "customer",
            "nome_Loja": "loja do sergio",
            "descricao_Loja": "descricao da loja do sergio",
            "id_Imagem": "",
            "tipo_Entrega": "entrega feita por mim"
        })
    }
    print(lambda_handler(event, None))

