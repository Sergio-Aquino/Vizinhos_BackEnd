import json
import boto3
import os
from dataclasses import dataclass
import re

@dataclass
class User:
    nome: str
    cpf: str
    Usuario_Tipo: str
    fk_id_Endereco: int
    telefone: str

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
            
        json_data['cpf'] = re.sub(r'\D', '', json_data.get('cpf', ''))
        if not re.match(r'^\d{11}$', json_data['cpf']):
            raise ValueError('Formatação de CPF inválida')

        return User(
            nome=json_data['nome'],
            cpf=json_data['cpf'],
            Usuario_Tipo=json_data['Usuario_Tipo'],
            fk_id_Endereco=json_data['fk_id_Endereco'],
            telefone=json_data['telefone'],
        )

def lambda_handler(event:any, context:any):
    try:
        body = json.loads(event['body'])
        user = User.from_json(body)

        dynamodb = boto3.resource('dynamodb')
        user_table = dynamodb.Table(os.environ['USER_TABLE'])

        response = user_table.get_item(Key={'cpf': user.cpf})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Usuário não encontrado"})
            }
        
        address_store_table = dynamodb.Table(os.environ['ADDRESS_STORE_TABLE'])
        response = address_store_table.get_item(Key={'id_Endereco': user.fk_id_Endereco})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Endereço não encontrado"})
            }

        user_table.update_item(
                Key={'cpf': user.cpf},
                UpdateExpression="SET Usuario_Tipo = :Usuario_Tipo, fk_id_Endereco = :fk_id_Endereco, telefone = :telefone, nome = :nome",
                ExpressionAttributeValues={
                    ':nome': user.nome,
                    ':Usuario_Tipo': user.Usuario_Tipo,
                    ':fk_id_Endereco': user.fk_id_Endereco,
                    ':telefone': user.telefone,
                }
            )
        
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Usuário atualizado com sucesso!"}, default=str)
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
            "body": json.dumps({"message": "Erro ao atualizar usuário: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['ADDRESS_STORE_TABLE'] = 'Loja_Endereco'

    event = {
        "body": json.dumps({
            "nome": "Aquino Lima",
            "cpf": "12345678901",
            "Usuario_Tipo": "seller",
            "fk_id_Endereco": 743647647193518997,
            "telefone": "40028922",
        })
    }
    print(lambda_handler(event, None))