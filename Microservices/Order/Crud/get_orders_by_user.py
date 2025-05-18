import json
import os
import boto3
from dataclasses import dataclass
from decimal import Decimal
import mercadopago

@dataclass
class StoreResponse:
    id_loja: str
    nome_loja: str
    imagem_loja: str
    endereco_loja: str
    cep_loja: str

@dataclass
class ProductResponse:
    nome_produto: str
    imagem_produto: str
    quantidade: int
    valor_unitario: Decimal
    loja: StoreResponse


@dataclass
class OrderResponse:
    id_Pedido: str
    id_Pagamento: str
    status_pedido: str
    valor_total: Decimal
    data_pedido: str
    qr_code: str = None
    qr_code_base64: str = None
    produtos: list[ProductResponse] = None




def get_store_image(id_imagem: str):
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
        print("message: " + str(ex))
        print(f"Erro ao buscar imagem com id: {id_imagem}: {str(ex)}")
        return None

def get_product_store(lote_id: str):
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
    
    store_id = response_product['Item']['fk_id_Endereco']

    table_store = dynamodb.Table(os.environ['TABLE_STORE'])
    response_store = table_store.get_item(Key={'id_Endereco': store_id})

    if 'Item' not in response_store:
        print("Nenhuma loja encontrada com o id: " + str(store_id))
        return None
    
    store = response_store['Item']
    store_response = StoreResponse(
        id_loja=store['id_Endereco'],
        nome_loja=store['nome_Loja'],
        imagem_loja=get_store_image(store['id_Imagem']),
        endereco_loja=store['logradouro'],
        cep_loja=store['cep']
    )

    return store_response

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
        bucket_name = os.environ['BUCKET_NAME_PRODUCT']
        
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
            'valor_unitario': item['preco_unitario'],
            'loja': get_product_store(item['fk_id_Lote'])
        }
        product_list.append(item_response)
    
    return product_list

def get_order_qr_code(order: dict):
    try:
        table_store = boto3.resource('dynamodb').Table(os.environ['TABLE_STORE'])
        response_store = table_store.get_item(Key={'id_Endereco': order['fk_id_Endereco']})

        if 'Item' not in response_store:
            print("Nenhuma loja encontrada com o id: " + str(order['fk_id_Endereco']))
            return None
        
        access_token = response_store['Item']['access_token']
        if not access_token:
            print("Access token não encontrado para a loja: " + str(order['fk_id_Endereco']))
            return None
        
        sdk = mercadopago.SDK(access_token)
        payment = sdk.payment().get(order['id_Pagamento'])

        return payment['response']['point_of_interaction']['transaction_data']['qr_code']
    except Exception as ex:
        print("message: " + str(ex))
        print(f"Erro ao buscar access token para pedido com id Pagemento: {order['id_Pagamento']}: {str(ex)}")
        return None

def get_order_qr_code_base64(order: dict):
    try:
        table_store = boto3.resource('dynamodb').Table(os.environ['TABLE_STORE'])
        response_store = table_store.get_item(Key={'id_Endereco': order['fk_id_Endereco']})

        if 'Item' not in response_store:
            print("Nenhuma loja encontrada com o id: " + str(order['fk_id_Endereco']))
            return None
        
        access_token = response_store['Item']['access_token']
        if not access_token:
            print("Access token não encontrado para a loja: " + str(order['fk_id_Endereco']))
            return None
        
        sdk = mercadopago.SDK(access_token)
        payment = sdk.payment().get(order['id_Pagamento'])

        return payment['response']['point_of_interaction']['transaction_data']['qr_code_base64']
    except Exception as ex:
        print("message: " + str(ex))
        print(f"Erro ao buscar access token para pedido com id Pagemento: {order['id_Pagamento']}: {str(ex)}")
        return None

def lambda_handler(event:any, context:any):
    try:
        cpf = event.get('queryStringParameters', {}).get('cpf', None)
        if not cpf:
            print("CPF não informado")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'campo obrigatório não informado: CPF'})
            }
        
        table_user = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
        response_user = table_user.get_item(Key={'cpf': cpf})

        if 'Item' not in response_user:
            print(f"Usuário {cpf} não encontrado")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'CPF não encontrado'})
            }
        
        table_order = boto3.resource('dynamodb').Table(os.environ['ORDERS_TABLE'])
        response_orders = table_order.query(
            IndexName='fk_Usuario_cpf-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_Usuario_cpf').eq(cpf)
        )

        if 'Items' not in response_orders or len(response_orders['Items']) == 0:
            print(f"Nenhum pedido encontrado para o CPF: {cpf}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Nenhum pedido encontrado para o CPF: ' + str(cpf)}, default=str)
            }
        
        orders = response_orders['Items']
        order_list = []
        for order in orders:
            order_response = OrderResponse(
                id_Pedido=order['id_Pedido'],
                id_Pagamento=order['id_Pagamento'],
                status_pedido=order['status_pedido'],
                valor_total=order['valor'],
                data_pedido=order['data_pedido'],
                qr_code=get_order_qr_code(order),
                qr_code_base64=get_order_qr_code_base64(order)
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
                        valor_unitario=produto['valor_unitario'],
                        loja=produto['loja']
                    )
                    order.produtos.append(product_response)

        return {
            'statusCode': 200,
            'body': json.dumps(
                {
                    "pedidos": [
                        {
                            **order.__dict__,
                            "produtos": [
                                {
                                    **p.__dict__,
                                    "loja": p.loja.__dict__ if p.loja else None
                                }
                                for p in order.produtos
                            ] if order.produtos else []
                        }
                        for order in order_list
                    ]
                },
                default=str
            )
        }
    except Exception as ex:
        print(f"Erro ao buscar produtos: {str(ex)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro ao busacar produtos: ' + str(ex)}, default=str)
        }


if __name__ == "__main__":
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['ORDERS_TABLE'] = 'Pedido'
    os.environ['TABLE_ITEM_ORDER'] = 'Itens_Pedido'
    os.environ['TABLE_LOTE'] = 'Produto_Lote'
    os.environ['TABLE_PRODUCT'] = 'Produto'
    os.environ['BUCKET_NAME_PRODUCT'] = 'product-image-vizinhos'
    os.environ['BUCKET_NAME_STORE'] = 'loja-profile-pictures'
    os.environ['TABLE_STORE'] = 'Loja_Endereco'
    event = {
        'queryStringParameters': {
            'cpf': "48812172830"
        }
    }
    print(lambda_handler(event, None))