import json
import boto3
import os
from dataclasses import dataclass
from decimal import Decimal

PRODUCT_CACHE = {}
LOTE_CACHE = {}
IMAGE_CACHE = {}

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')


@dataclass
class ProductResponse:
    nome_produto: str
    imagem_produto: str
    quantidade: int
    valor_unitario: float

    def to_dict(self):
        return {
            'nome_produto': self.nome_produto,
            'imagem_produto': self.imagem_produto,
            'quantidade': int(self.quantidade) if isinstance(self.quantidade, Decimal) else self.quantidade,
            'valor_unitario': float(self.valor_unitario) if isinstance(self.valor_unitario, Decimal) else self.valor_unitario
        }

@dataclass
class OrderResponse:
    id_Pedido: str
    status_pedido: str
    valor_total: float
    data_pedido: str
    AvaliacaoFeita: bool = False
    produtos: list[ProductResponse] = None

    def to_dict(self):
        return {
            'id_Pedido': self.id_Pedido,
            'status_pedido': self.status_pedido,
            'valor_total': float(self.valor_total) if isinstance(self.valor_total, Decimal) else self.valor_total,
            'data_pedido': self.data_pedido,
            'AvaliacaoFeita': self.AvaliacaoFeita,
            'produtos': [p.to_dict() for p in self.produtos] if self.produtos else []
        }

def get_table(table_env_var):
    table_name = os.environ[table_env_var]
    return dynamodb.Table(table_name)

def convert_decimal_values(item):
    if isinstance(item, dict):
        return {k: convert_decimal_values(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convert_decimal_values(v) for v in item]
    elif isinstance(item, Decimal):
        return int(item) if item % 1 == 0 else float(item)
    return item

def get_lote(lote_id: str):
    if lote_id in LOTE_CACHE:
        return LOTE_CACHE[lote_id]
    
    table_lote = get_table('TABLE_LOTE')
    response_lote = table_lote.get_item(Key={'id_Lote': lote_id})
    
    if 'Item' in response_lote:
        item = convert_decimal_values(response_lote['Item'])
        LOTE_CACHE[lote_id] = item
        return item
    
    print(f"Nenhum lote encontrado com o id: {lote_id}")
    return None

def get_product(product_id: str):
    if product_id in PRODUCT_CACHE:
        return PRODUCT_CACHE[product_id]
    
    table_product = get_table('TABLE_PRODUCT')
    response_product = table_product.get_item(Key={'id_Produto': product_id})
    
    if 'Item' in response_product:
        item = convert_decimal_values(response_product['Item'])
        PRODUCT_CACHE[product_id] = item
        return item
    
    print(f"Nenhum produto encontrado com o id: {product_id}")
    return None

def get_product_name(lote_id: str):
    lote = get_lote(lote_id)
    if not lote:
        return None
    
    product = get_product(lote['fk_id_Produto'])
    if not product:
        return None
    
    return product['nome']

def get_image(id_imagem: str):
    if not id_imagem:
        return None
    
    if id_imagem in IMAGE_CACHE:
        return IMAGE_CACHE[id_imagem]
    
    try:
        bucket_name = os.environ['BUCKET_NAME']
        image_url = f"https://{bucket_name}.s3.amazonaws.com/{id_imagem}"
        
        IMAGE_CACHE[id_imagem] = image_url
        return image_url
    except Exception as ex:
        print(f"Erro ao buscar imagem com id: {id_imagem}: {str(ex)}")
        return None
    
def get_product_image(lote_id: str):
    lote = get_lote(lote_id)
    if not lote:
        return None
    
    product = get_product(lote['fk_id_Produto'])
    if not product:
        return None
    
    return get_image(product['id_imagem'])

def get_order_items(order_id: str):
    table_item_order = get_table('TABLE_ITEM_ORDER')
    
    response_item_order = table_item_order.query(
        IndexName='fk_id_Pedido-index',
        KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Pedido').eq(order_id)
    )
    
    if 'Items' not in response_item_order or len(response_item_order['Items']) == 0:
        print(f"Nenhum item encontrado para o pedido: {order_id}")
        return []
    
    return [convert_decimal_values(item) for item in response_item_order['Items']]

def get_order_products(order: OrderResponse):
    items = get_order_items(order.id_Pedido)
    if not items:
        return None
    
    product_list = []
    for item in items:
        lote_id = item['fk_id_Lote']
        
        product_response = ProductResponse(
            nome_produto=get_product_name(lote_id),
            imagem_produto=get_product_image(lote_id),
            quantidade=item['quantidade_item'],
            valor_unitario=item['preco_unitario']
        )
        product_list.append(product_response)
    
    return product_list
    
def lambda_handler(event: any, context: any): 
    try:
        query_params = event.get('queryStringParameters', {}) or {}
        store_id = query_params.get('id_Loja')
        
        limit = int(query_params.get('limit', '10'))
        include_products = query_params.get('include_products', 'true').lower() == 'true'
        
        try:
            store_id = int(store_id) if store_id else None
        except ValueError:
            print("ID da loja inválido.")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "ID da loja inválido."})
            }
            
        if not store_id:
            print("ID da loja não fornecido.")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "ID da loja não fornecido."})
            }
        
        table_order = get_table('TABLE_ORDER')
        query_params_dynamo = {
            'IndexName': 'fk_id_Endereco-index',
            'KeyConditionExpression': boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(store_id),
            'Limit': limit
        }
        
        response_orders = table_order.query(**query_params_dynamo)

        if 'Items' not in response_orders or len(response_orders['Items']) == 0:
            print(f"Nenhum pedido encontrado para a loja: {store_id}")
            return {
                "statusCode": 404,
                "body": json.dumps({"message": f"Nenhum pedido encontrado para a loja: {store_id}"})
            }
        
        orders = [convert_decimal_values(order) for order in response_orders['Items']]
        order_list = []
        
        for order in orders:
            order_response = OrderResponse(
                id_Pedido=order['id_Pedido'],
                status_pedido=order['status_pedido'],
                valor_total=order['valor'],
                data_pedido=order['data_pedido'],
                AvaliacaoFeita=order.get('AvaliacaoFeita', False)
            )
            order_list.append(order_response)
        
        if include_products:
            for order_resp in order_list:
                produtos = get_order_products(order_resp)
                if produtos:
                    order_resp.produtos = produtos

        next_token = None
        if 'LastEvaluatedKey' in response_orders:
            last_key = convert_decimal_values(response_orders['LastEvaluatedKey'])
            next_token = json.dumps(last_key)
        
        response_body = {
            "pedidos": [order.to_dict() for order in order_list]
        }
        
        if next_token:
            response_body["nextToken"] = next_token

        return {
            "statusCode": 200,
            "body": json.dumps(response_body)
        }

    except KeyError as err:
        print(f"Campo obrigatório não encontrado: {str(err)}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Campo obrigatório não encontrado: {str(err)}"})
        }
    except Exception as ex:
        print(f"Erro ao pegar pedidos: {str(ex)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Erro ao pegar pedidos: {str(ex)}"})
        }


    

if __name__ == "__main__":
    os.environ['TABLE_ORDER'] = 'Pedido'
    os.environ['TABLE_ITEM_ORDER'] = 'Itens_Pedido'
    os.environ['TABLE_LOTE'] = 'Produto_Lote'
    os.environ['TABLE_PRODUCT'] = 'Produto'
    os.environ['BUCKET_NAME'] = 'product-image-vizinhos'

    event = {
        "queryStringParameters": {
            "id_Loja": 185962218056648587
        }
    }
    context = {}
    response = lambda_handler(event, context)
    print(response)