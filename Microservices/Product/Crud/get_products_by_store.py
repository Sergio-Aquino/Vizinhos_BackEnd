import json
import boto3
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import List

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
    id_imagem: int
    disponivel: bool
    id_Produto: str
    caracteristicas: List[dict]
    flag_oferta: bool
    categoria: str = None
    imagem_url: str = None
    dt_fabricacao: str = None
    valor_venda_desc: Decimal = None
    quantidade: int = None
    id_lote: str = None

    @staticmethod
    def from_json(json_data: dict):
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
        print("message: " + str(ex))
        print(f"Erro ao buscar imagem com id: {id_imagem}: {str(ex)}")
        return None


def lambda_handler(event: any, context:any): 
    try:
        fk_id_Endereco = event.get('queryStringParameters', {}).get('fk_id_Endereco')
        if not fk_id_Endereco:
            print("message: fk_id_Endereco é obrigatório")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'fk_id_Endereco é obrigatório'})
            }
        
        fk_id_Endereco = int(fk_id_Endereco)
        if isinstance(fk_id_Endereco, str):
            raise TypeError('fk_id_Endereco deve ser um inteiro')
        
        dynamodb = boto3.resource('dynamodb')
        
        table_user = dynamodb.Table(os.environ['USER_TABLE'])
        response_user = table_user.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(fk_id_Endereco)
        )

        if 'Items' not in response_user or len(response_user['Items']) == 0:
            print("message: Não foi possível relacionar a loja com um vendedor")
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Não foi possível relacionar a loja com um vendedor'})
            }
        
        if response_user['Items'][0]['Usuario_Tipo'] not in ['seller', 'customer_seller']:
            print("message: Apenas lojas possuem produtos")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Apenas lojas possuem produtos'})
            }

        table_product = dynamodb.Table(os.environ['PRODUCT_TABLE'])

        response = table_product.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(fk_id_Endereco)
        )

        if 'Items' not in response or len(response['Items']) == 0:
            return None
        
        table_categoria = dynamodb.Table(os.environ['CATEGORY_TABLE'])
        table_lote = dynamodb.Table(os.environ['LOT_TABLE'])
        products = []
        for item in response['Items']:
            product = Product.from_json(item)
            product.imagem_url = get_product_image(product.id_imagem)
            product.categoria = table_categoria.get_item(Key={'id_Categoria': product.fk_id_Categoria}).get('Item', {}).get('descricao')
            products.append(product)

        table_product_characteristics = dynamodb.Table(os.environ['PRODUCT_CHARACTERISTIC_TABLE'])
        for product in products:
            response_product_characteristics = table_product_characteristics.query(
                IndexName='fk_Produto_id_Produto-index',
                KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_Produto_id_Produto').eq(product.id_Produto)
            )

            product_characteristics = response_product_characteristics['Items']
            characteristic_table = dynamodb.Table(os.environ['CHARACTERISTIC_TABLE'])

            for product_characteristic in product_characteristics:
                characteristic_id = product_characteristic['fk_Carecteristica_id_Caracteristica']
                response_characteristic = characteristic_table.get_item(Key={'id_Caracteristica': characteristic_id})
                if 'Item' not in response_characteristic:
                    continue
                characteristic_data = {
                    'id_Caracteristica': characteristic_id,
                    'descricao': response_characteristic['Item']['descricao']
                }
                product.caracteristicas.append(characteristic_data)

            lote = table_lote.query(
                IndexName='fk_id_Produto-index',
                KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Produto').eq(product.id_Produto)
            )

            if len(lote['Items']) > 1:
                print(f"Produto {product.id_Produto} possui mais de um lote")
                continue

            if lote['Items']:
                lote_item = lote['Items'][0]
                product.dt_fabricacao = lote_item.get('dt_fabricacao', None)
                product.valor_venda_desc = lote_item.get('valor_venda_desc', None)
                product.quantidade = lote_item.get('quantidade', None)
                product.id_lote = lote_item.get('id_Lote', None)
                
        return {
            'statusCode': 200,
            'body': json.dumps({"produtos": [product.__dict__ for product in products]}, default=str)
        }

    except TypeError as ex:
        print("message: " + str(ex))
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(ex)}, default=str)
        }
    except KeyError as err:
        print("message: " + str(err))
        return {
            "statusCode": 400,
            'body': json.dumps({'message': f'Campo obrigatório não encontrado: {str(err)}'}, default=str)
        }
    except Exception as ex:
        print("message: " + str(ex))
        return {
            'statusCode': 500,
            'body': json.dumps({'message': "Erro ao retornar produto: " + str(ex)}, default=str)
        }
    
if __name__ == "__main__":
    os.environ['PRODUCT_TABLE'] = ''
    os.environ['PRODUCT_CHARACTERISTIC_TABLE'] = ''
    os.environ['CHARACTERISTIC_TABLE'] = ''
    os.environ['CATEGORY_TABLE'] = ''
    os.environ['USER_TABLE'] = ''
    os.environ['BUCKET_NAME'] = ''
    os.environ['LOT_TABLE'] = ''

    event = {
        'queryStringParameters': {
            'fk_id_Endereco': ""
        }
    }

    print(lambda_handler(event, None))