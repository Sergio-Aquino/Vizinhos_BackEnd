import json
import boto3
import os
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class ProductResponse:
    nome_produto: str
    imagem_produto: str
    quantidade: int
    valor_unitario: Decimal


@dataclass
class OrderResponse:
    id_Pedido: str
    status_pedido: str
    valor_total: Decimal
    data_pedido: str
    produtos: list[ProductResponse] = None

def get_product_name(lote_id: str):
    dynamodb = boto3.resource('dynamodb')
    table_lote = dynamodb.Table(os.environ['TABLE_LOTE'])
    table_product = dynamodb.Table(os.environ['TABLE_PRODUCT'])
    response_lote = table_lote.get_item(Key={'id_Lote': lote_id})

    if 'Item' not in response_lote:
        print("Nenhum lote encontrado com o id: " + str(lote_id))
        return None
    
    lote = response_lote['Item']
    response_product = table_product.get_item(Key={'id_Produto': lote['fk_id_Produto']})

    if 'Item' not in response_product:
        print("Nenhum produto encontrado com o id: " + str(lote['fk_id_Produto']))
        return None
    
    product = response_product['Item']
    return product['nome']

def get_image(id_imagem: str):
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
    
def get_product_image(lote_id: str):
    dynamodb = boto3.resource('dynamodb')
    table_lote = dynamodb.Table(os.environ['TABLE_LOTE'])
    table_product = dynamodb.Table(os.environ['TABLE_PRODUCT'])
    response_lote = table_lote.get_item(Key={'id_Lote': lote_id})

    if 'Item' not in response_lote:
        print("Nenhum lote encontrado com o id: " + str(lote_id))
        return None
    
    lote = response_lote['Item']
    response_product = table_product.get_item(Key={'id_Produto': lote['fk_id_Produto']})

    if 'Item' not in response_product:
        print("Nenhum produto encontrado com o id: " + str(lote['fk_id_Produto']))
        return None
    
    product = response_product['Item']
    product_image = get_image(product['id_imagem'])

    return product_image
    

def get_order_products(order: OrderResponse):
    dynamodb = boto3.resource('dynamodb')
    table_item_order = dynamodb.Table(os.environ['TABLE_ITEM_ORDER'])

    response_item_order = table_item_order.query(
        IndexName='fk_id_Pedido-index',
        KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Pedido').eq(order.id_Pedido)
    )

    if 'Items' not in response_item_order or len(response_item_order['Items']) == 0:
        print("Nenhum item encontrado para o pedido: " + str(order.id_Pedido))
        return None
    
    items = response_item_order['Items']
    product_list = []
    for item in items:
        item_response = {
            'nome_produto': get_product_name(item['fk_id_Lote']),
            'imagem_produto': get_product_image(item['fk_id_Lote']),
            'quantidade': item['quantidade_item'],
            'valor_unitario': item['preco_unitario']
        }
        product_list.append(item_response)
    
    return product_list

    
def lambda_handler(event: any, context:any): 
    try:
        store_id = event.get('queryStringParameters', {}).get('id_Loja', None)
        store_id = int(store_id) if store_id else None
        if not store_id:
            print("ID da loja não fornecido.")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "ID da loja não fornecido."})
            }
        
        dynamodb = boto3.resource('dynamodb')
        table_order = dynamodb.Table(os.environ['TABLE_ORDER'])
        orders = table_order.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(store_id)
        )

        if 'Items' not in orders or len(orders['Items']) == 0:
            print("Nenhum pedido encontrado para a loja: " + str(store_id))
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Nenhum pedido encontrado para a loja: " + str(store_id)}, default=str)
            }
        
        orders = orders['Items']
        order_list = []
        for order in orders:
            order_response = OrderResponse(
                id_Pedido=order['id_Pedido'],
                status_pedido=order['status_pedido'],
                valor_total=order['valor'],
                data_pedido=order['data_pedido'],
            )
            order_list.append(order_response)
        
        for order in order_list:
            produtos = get_order_products(order)
            if produtos:
                order.produtos = []
                for produto in produtos:
                    product_response = ProductResponse(
                        nome_produto=produto['nome_produto'],
                        imagem_produto=produto['imagem_produto'],
                        quantidade=produto['quantidade'],
                        valor_unitario=produto['valor_unitario']
                    )
                    order.produtos.append(product_response)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "pedidos": [
                        {
                            **order.__dict__,
                            "produtos": [p.__dict__ for p in order.produtos] if order.produtos else []
                        }
                        for order in order_list
                    ]
                },
                default=str
        )
        }

    except KeyError as err:
        print(f"Campo obrigatório não encontrado: {str(err)}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Campo obrigatório não encontrado: {str(err)}"}, default=str)
        }
    except Exception as ex:
        print(f"Erro ao pegar pedidos: {str(ex)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro ao pegar pedidos: " + str(ex),}, default=str)
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