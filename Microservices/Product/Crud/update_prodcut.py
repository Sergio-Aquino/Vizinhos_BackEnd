from dataclasses import dataclass
from decimal import Decimal
import json
import boto3
import os
from typing import List

@dataclass
class Product:
    id_Produto: str
    nome: str
    fk_id_Categoria: int
    dias_vcto: int
    valor_venda: float
    valor_custo: float
    tamanho: str
    descricao: str
    id_imagem: str
    disponivel: bool
    caracteristicas_IDs: list[str]
    caracteristicas: List[str] = None
    imagem_url: str = None

    @staticmethod
    def from_json(json_data: dict):
        if not isinstance(json_data['nome'], str):
            raise TypeError('nome deve ser uma string')
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
        if not isinstance(json_data['id_Produto'], str):
            raise TypeError('id_Produto deve ser uma string')

        json_data['caracteristicas'] = []
        
        return Product(**json_data)
    
def get_product_image(id_imagem: int) -> str:
    try:
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']
        
        if not id_imagem:
            raise ValueError("ID da imagem não informado")

        response = s3.get_object(Bucket=bucket_name, Key=id_imagem)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ValueError("Erro ao buscar imagem no S3")
            
        image_url = f"https://{bucket_name}.s3.amazonaws.com/{id_imagem}"
        return image_url
    except Exception as ex:
        print(f"Erro ao buscar imagem com id: {id_imagem}: {str(ex)}")
        return None

def lambda_handler(event: any, context:any):
    try:
        body = json.loads(event['body'])
        product = Product.from_json(body)

        dynamodb = boto3.resource('dynamodb')

        table_product = dynamodb.Table(os.environ['PRODUCT_TABLE'])
        response_table_product = table_product.get_item(Key={'id_Produto': product.id_Produto})
        if 'Item' not in response_table_product:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    "message": "Produto não encontrado"
                }, default=str)
            }
        
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']
        previous_image = response_table_product['Item']['id_imagem'] if 'id_imagem' in response_table_product['Item'] else None

        if previous_image:
            if previous_image != product.id_imagem:
                try:
                    print(f"Deletando imagem anterior: {previous_image}")
                    s3.delete_object(Bucket=bucket_name, Key=previous_image)
                except Exception as ex:
                    print(f"Erro ao deletar imagem anterior: {str(ex)}")
                    
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
            
        response_product_characteristic = table_product_characteristic.query(
            IndexName='fk_Produto_id_Produto-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_Produto_id_Produto').eq(product.id_Produto)
        )

        for item in response_product_characteristic['Items']:
            table_product_characteristic.delete_item(
                Key={
                    'fk_Carecteristica_id_Caracteristica': item['fk_Carecteristica_id_Caracteristica'],
                    'fk_Produto_id_Produto': product.id_Produto
                }
            )

        for id_caracteristica in product.caracteristicas_IDs:
            table_product_characteristic.put_item(
                Item={
                    'fk_Carecteristica_id_Caracteristica': id_caracteristica,
                    'fk_Produto_id_Produto': product.id_Produto
                }, ConditionExpression="attribute_not_exists(fk_Carecteristica_id_Caracteristica) AND attribute_not_exists(fk_Produto_id_Produto)"
            )

        product.imagem_url = get_product_image(product.id_imagem)

        table_product.update_item(
            Key={'id_Produto': product.id_Produto},
            UpdateExpression="SET nome = :nome, fk_id_Categoria = :fk_id_Categoria, dias_vcto = :dias_vcto, valor_venda = :valor_venda, valor_custo = :valor_custo, tamanho = :tamanho, descricao = :descricao, id_imagem = :id_imagem, disponivel = :disponivel",
            ExpressionAttributeValues={
                ':nome': product.nome,
                ':fk_id_Categoria': product.fk_id_Categoria,
                ':dias_vcto': product.dias_vcto,
                ':valor_venda': product.valor_venda,
                ':valor_custo': product.valor_custo,
                ':tamanho': product.tamanho,
                ':descricao': product.descricao,
                ':id_imagem': product.id_imagem,
                ':disponivel': product.disponivel
            },
        )

        for product_characteristic_Id in product.caracteristicas_IDs:
            response_characteristic = table_characteristic.get_item(Key={'id_Caracteristica': product_characteristic_Id})
            if 'Item' not in response_characteristic:
                continue
            product.caracteristicas.append(response_characteristic['Item']['descricao'])

        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": "Produto atualizado com sucesso",
                "id_Produto": product.id_Produto,
                "nome": product.nome,
                "fk_id_Categoria": product.fk_id_Categoria,
                "dias_vcto": product.dias_vcto,
                "valor_venda": str(product.valor_venda),
                "valor_custo": str(product.valor_custo),
                "tamanho": product.tamanho,
                "descricao": product.descricao,
                "id_imagem": product.id_imagem,
                "disponivel": product.disponivel,
                "caracteristicas_IDs": product.caracteristicas_IDs,
                "caracateristicas": product.caracteristicas,
                "imagem_url": product.imagem_url
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
                "message": "Erro ao atualizar produto: " + str(ex)
            }, default=str)
        }
    

if __name__ == "__main__":
    os.environ['PRODUCT_TABLE'] = 'Produto'
    os.environ['CATEGORY_TABLE'] = 'Categoria'
    os.environ['CHARACTERISTIC_TABLE'] = 'Caracteristica'
    os.environ['PRODUCT_CHARACTERISTIC_TABLE'] = 'Produto_Caracteristica'
    os.environ['BUCKET_NAME'] = 'product-image-vizinhos'
    event = {
        "body": json.dumps({
            "id_Produto": '9d55e8f6-68a8-4665-bac7-cc2e1b52322a',
            "nome": 'Produto Teste para atualizar',
            "fk_id_Categoria": 676581255295820208,
            "dias_vcto": 70,
            "valor_venda": 70,
            "valor_custo": 70,
            "tamanho": 'P de pequeno',
            "descricao": 'descricao',
            "id_imagem": "1",
            "disponivel": False,
            "caracteristicas_IDs": ["90c5f19b-a4d4-4991-b863-35da4dcd36ae", "b332ab38-45dc-4535-953a-bae803e642ec"]
        })
    }
    print(lambda_handler(event, None))