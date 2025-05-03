from dataclasses import dataclass
import json
import boto3
import os
from decimal import Decimal

@dataclass
class Batch:
    id_Lote: str
    fk_id_Produto: str
    dt_fabricacao: str
    quantidade: int
    valor_venda_desc: Decimal

    @staticmethod
    def from_dict(data: dict) -> 'Batch':
        return Batch(**data)
    
    
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
    caracteristicas: list[dict] = None
    categoria: str = None
    imagem_url: str = None
    lote: Batch = None

    @staticmethod
    def from_dict(data: dict) -> 'Product':
        return Product(**data)
    

@dataclass
class Store:
    id_Endereco: int
    cep: str
    logradouro: str
    numero: str
    complemento: str
    nome_Loja: str
    descricao_Loja: str
    id_Imagem: str
    tipo_Entrega: str
    imagem_url: str
    produtos: list[Product] = None

    @staticmethod
    def from_dict(data: dict) -> 'Store':
        return Store(**data)
    
def get_store_image(id_imagem):
    try:
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME_STORE']
        
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
    
def get_product_image(id_imagem: int) -> str:
    try:
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME_PRODUCT']
        
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
    

def lambda_handler(event:any, context:any):
    try:
        id_loja = event.get('queryStringParameters', {}).get('id_loja')
        id_loja = int(id_loja) if id_loja else None

        if not id_loja:
            print("ID da loja não fornecido")
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "ID da loja não fornecido"})
            }
        
        dynamodb = boto3.resource('dynamodb')
        address_table = dynamodb.Table(os.environ['ADDRESS_TABLE'])

        response = address_table.get_item(Key={'id_Endereco': id_loja})
        if 'Item' not in response:
            print(f"Loja com id: {id_loja} não encontrada")
            return {
                'statusCode': 404,
                'body': json.dumps({"message": f"Loja com id: {id_loja} não encontrada"})
            }
        
        item = response.get('Item')
        imagem_url = get_store_image(item['id_Imagem'])
        item['imagem_url'] = imagem_url if imagem_url else None
        store = Store.from_dict(item)

        product_table = dynamodb.Table(os.environ['PRODUCT_TABLE'])
        category_table = dynamodb.Table(os.environ['CATEGORY_TABLE'])
        product_characteristics_table = dynamodb.Table(os.environ['PRODUCT_CHARACTERISTICS_TABLE'])
        characteristic_table = dynamodb.Table(os.environ['CHARACTERISTIC_TABLE'])
        table_bacth = dynamodb.Table(os.environ['BATCH_TABLE'])

        response = product_table.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(store.id_Endereco)
        )
        if 'Items' not in response or len(response['Items']) == 0:
            store.produtos = []
        else:
            store.produtos = []
            produtos = [Product.from_dict(item) for item in response['Items']]
            for product in produtos:
                product.imagem_url = get_product_image(product.id_imagem)
                product.categoria = category_table.get_item(Key={'id_Categoria': product.fk_id_Categoria}).get('Item', {}).get('descricao')

                product_characteristics = product_characteristics_table.query(
                    IndexName='fk_Produto_id_Produto-index',
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_Produto_id_Produto').eq(product.id_Produto)
                )

                if 'Items' not in product_characteristics or len(product_characteristics['Items']) == 0:
                    product.caracteristicas = []
                else:
                    product.caracteristicas = []
                    for product_characteristic in product_characteristics['Items']:
                        characteristic = characteristic_table.get_item(Key={'id_Caracteristica': product_characteristic['fk_Carecteristica_id_Caracteristica']})
                        if 'Item' in characteristic:
                            characteristic_data = {
                                'id_Caracteristica': characteristic['Item']['id_Caracteristica'],
                                'descricao': characteristic['Item']['descricao']
                            }
                            product.caracteristicas.append(characteristic_data)
                        else:
                            print(f"Característica com id: {product_characteristic['fk_Carecteristica_id_Caracteristica']} não encontrada")

                batch = table_bacth.query(
                    IndexName='fk_id_Produto-index',
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Produto').eq(product.id_Produto)
                )
                if len(batch['Items']) > 1:
                    print(f"Produto {product.id_Produto} possui mais de um lote")
                    product.lote = None
                    continue

                product.lote = Batch.from_dict(batch['Items'][0]).__dict__
                store.produtos.append(product.__dict__)

        return {
            'statusCode': 200,
            'body': json.dumps(store.__dict__, default=str)
        }
    except ValueError as err:
        print(f"message: {err}")
        return {
            'statusCode': 400,
            'body': json.dumps({"message": str(err)})
        }
    except KeyError as err:
        print(f"Campo obrigatório não encontrado: {err}")
        return {
            "statusCode": 400,
            'body': json.dumps({'message': f'Campo obrigatório não encontrado: {str(err)}'}, default=str)
        }
    except Exception as ex:
        print(f"Erro ao buscar loja: {ex}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "Erro ao buscar loja: " + str(ex)})
        }
    

if __name__ == "__main__":
    os.environ['BUCKET_NAME_STORE'] = 'loja-profile-pictures'
    os.environ['BUCKET_NAME_PRODUCT'] = 'product-image-vizinhos'
    os.environ['ADDRESS_TABLE'] = 'Loja_Endereco'
    os.environ['PRODUCT_TABLE'] = 'Produto'
    os.environ['CATEGORY_TABLE'] = 'Categoria'
    os.environ['PRODUCT_CHARACTERISTICS_TABLE'] = 'Produto_Caracteristica'
    os.environ['CHARACTERISTIC_TABLE'] = 'Caracteristica'
    os.environ['BATCH_TABLE'] = 'Produto_Lote'
    event = {
        'queryStringParameters': {
            'id_loja': '167868419853349668'
        }
    }
    response = lambda_handler(event, None)
    print(response)