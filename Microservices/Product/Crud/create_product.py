import json
from typing import List
import boto3
import os
from dataclasses import dataclass
import uuid
from decimal import Decimal


@dataclass
class Product:
    nome: str
    fk_id_Endereco: int
    fk_id_Categoria: int
    dias_vcto: int
    valor_venda: Decimal
    valor_custo: Decimal
    tamanho: str
    descricao: str
    id_imagem: str
    disponivel: bool
    caracteristicas_IDs: List[str]
    id_Produto: str =  None
    flag_oferta: bool = False


    @staticmethod
    def from_json(json_data: dict):
        if not isinstance(json_data['nome'], str):
            raise TypeError('nome deve ser uma string')
        if not isinstance(json_data['fk_id_Endereco'], int):
            raise TypeError('fk_id_Endereco deve ser um inteiro')
        if not isinstance(json_data['fk_id_Categoria'], int):
            raise TypeError('fk_id_Categoria deve ser um inteiro')
        if not isinstance(json_data['dias_vcto'], int):
            raise TypeError('dias_vcto deve ser um inteiro')
        
        if isinstance(json_data['valor_venda'], (float, int)):
            json_data['valor_venda'] = Decimal(str(json_data['valor_venda']))
        if not isinstance(json_data['valor_venda'], Decimal):
            raise TypeError('valor_venda deve ser um decimal')
    
        if isinstance(json_data['valor_custo'], (float, int)):
            json_data['valor_custo'] = Decimal(str(json_data['valor_custo']))
        if not isinstance(json_data['valor_custo'], Decimal):
            raise TypeError('valor_custo deve ser um decimal')
        
        if not isinstance(json_data['tamanho'], str):
            raise TypeError('tamanho deve ser uma string')
        if not isinstance(json_data['descricao'], str):
            raise TypeError('descricao deve ser uma string')
        if not isinstance(json_data['id_imagem'], str):
            raise TypeError('id_imagem deve ser uma string')
        if not isinstance(json_data['disponivel'], bool):
            raise TypeError('disponivel deve ser um booleano')
        if not isinstance(json_data['caracteristicas_IDs'], list):
            raise TypeError('caracteristicas_IDs deve ser uma lista')
        if not all(isinstance(i, str) for i in json_data['caracteristicas_IDs']):
            raise TypeError('Todos os elementos de caracteristicas_IDs devem ser strings')
        
        json_data['id_Produto'] = str(uuid.uuid4())

        return Product(**json_data)

def lambda_handler(event:any, context:any): 
    try:
        body = json.loads(event['body'])
        product = Product.from_json(body)

        dynamodb = boto3.resource('dynamodb')

        table_address_store = dynamodb.Table(os.environ['ADDRESS_TABLE'])
        if 'Item' not in table_address_store.get_item(Key={'id_Endereco': product.fk_id_Endereco}):
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Loja não encontrada'})
            }
        
        table_user = dynamodb.Table(os.environ['USER_TABLE'])
        response_user = table_user.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(product.fk_id_Endereco)
        )

        if 'Items' not in response_user or len(response_user['Items']) == 0:
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Não foi possível relacionar a loja com um vendedor'})
            }
        
        if response_user['Items'][0]['Usuario_Tipo'] not in ['seller', 'customer_seller']:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Apenas vendedores podem criar produtos'})
            }
        


        table_category = dynamodb.Table(os.environ['CATEGORY_TABLE'])
        if 'Item' not in table_category.get_item(Key={'id_Categoria': product.fk_id_Categoria}):
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Categoria não encontrada'})
            }
        
        table_characteristic = dynamodb.Table(os.environ['CHARACTERISTIC_TABLE'])
        table_product_characteristic = dynamodb.Table(os.environ['PRODUCT_CHARACTERISTIC_TABLE'])

        for id_caracteristica in product.caracteristicas_IDs:
            if 'Item' not in table_characteristic.get_item(Key={'id_Caracteristica': id_caracteristica}):
                return {
                    'statusCode': 404,
                    'body': json.dumps({'message':f'Característica com id:{id_caracteristica} não encontrada'})
                }
            
            table_product_characteristic.put_item(
                Item={
                    "fk_Carecteristica_id_Caracteristica": id_caracteristica,
                    "fk_Produto_id_Produto": product.id_Produto
                }, ConditionExpression="attribute_not_exists(fk_Carecteristica_id_Caracteristica) AND attribute_not_exists(fk_Produto_id_Produto)"
            )

        table_product = dynamodb.Table(os.environ['PRODUCT_TABLE'])
        table_product.put_item(Item={
            'id_Produto': product.id_Produto,
            'nome': product.nome,
            'fk_id_Endereco': product.fk_id_Endereco,
            'fk_id_Categoria': product.fk_id_Categoria,
            'dias_vcto': product.dias_vcto,
            'valor_venda': product.valor_venda,
            'valor_custo': product.valor_custo,
            'tamanho': product.tamanho,
            'descricao': product.descricao,
            'id_imagem': product.id_imagem,
            'disponivel': product.disponivel,
            'flag_oferta': product.flag_oferta,
        }, ConditionExpression="attribute_not_exists(id_Produto)")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Produto criado com sucesso',
                'produto': product.__dict__
            }, default=str)
        }
    except KeyError as err:
        return {
            "statusCode": 400,
            'body': json.dumps({'message': f'Campo obrigatório não informado: {str(err)}'}, default=str)
        }
    except TypeError as err:
         return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Erro ao criar produto: ' + str(ex),
            }, default=str)
        }
    

if __name__ == "__main__":
    os.environ['ADDRESS_TABLE'] = ''
    os.environ['CATEGORY_TABLE'] = ''
    os.environ['PRODUCT_TABLE'] = ''
    os.environ['PRODUCT_CHARACTERISTIC_TABLE'] = ''
    os.environ['CHARACTERISTIC_TABLE'] = ''
    os.environ['USER_TABLE'] = ''

    event = {
        "body": json.dumps({
            "nome": 'Produto Teste',
            "fk_id_Endereco": "",
            "fk_id_Categoria": "",
            "dias_vcto": 30,
            "valor_venda": 10.0,
            "valor_custo": 5.0,
            "tamanho": 'M',
            "descricao": 'Produto de teste',
            "id_imagem": "",
            "disponivel": True,
            "caracteristicas_IDs": [""]
        })
    }

    print(lambda_handler(event, None))