import json
import os
import boto3
from dataclasses import dataclass, asdict
from decimal import Decimal
import mercadopago

PRODUCT_CACHE = {}
STORE_CACHE = {}
LOTE_CACHE = {}
IMAGE_CACHE = {}
QR_CODE_CACHE = {}
MP_SDK_INSTANCES = {}

dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, bool):
        return obj
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

@dataclass
class StoreResponse:
    id_loja: str
    nome_loja: str
    imagem_loja: str
    endereco_loja: str
    cep_loja: str
    tipo_entrega: str

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
    AvaliacaoFeita: bool = False
    qr_code: str = None
    produtos: list[ProductResponse] = None

def get_table(table_env_var):
    table_name = os.environ[table_env_var]
    return dynamodb.Table(table_name)

def convert_decimal_values(item):
    if isinstance(item, dict):
        return {k: convert_decimal_values(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convert_decimal_values(v) for v in item]
    elif isinstance(item, Decimal):
        return item 
    return item

def get_store_image(id_imagem: str):
    if not id_imagem:
        return None
        
    cache_key = f"store:{id_imagem}"
    if cache_key in IMAGE_CACHE:
        return IMAGE_CACHE[cache_key]
    
    try:
        bucket_name = os.environ["BUCKET_NAME_STORE"]
        image_url = f"https://{bucket_name}.s3.amazonaws.com/{id_imagem}"
        IMAGE_CACHE[cache_key] = image_url
        return image_url
    except Exception as ex:
        print(f"Erro ao buscar imagem com id: {id_imagem}: {str(ex)}")
        return None

def get_lote(lote_id: str):
    if lote_id in LOTE_CACHE:
        return LOTE_CACHE[lote_id]
    
    table_lote = get_table("TABLE_LOTE")
    response_lote = table_lote.get_item(Key={"id_Lote": lote_id})
    
    if "Item" in response_lote:
        item = convert_decimal_values(response_lote["Item"])
        LOTE_CACHE[lote_id] = item
        return item
    
    print(f"Nenhum lote encontrado com o id: {lote_id}")
    return None

def get_product(product_id: str):
    if product_id in PRODUCT_CACHE:
        return PRODUCT_CACHE[product_id]
    
    table_product = get_table("TABLE_PRODUCT")
    response_product = table_product.get_item(Key={"id_Produto": product_id})
    
    if "Item" in response_product:
        item = convert_decimal_values(response_product["Item"])
        PRODUCT_CACHE[product_id] = item
        return item
    
    print(f"Nenhum produto encontrado com o id: {product_id}")
    return None

def get_store(store_id: str):
    store_id_str = str(store_id) 
    if store_id_str in STORE_CACHE:
        return STORE_CACHE[store_id_str]
    
    table_store = get_table("TABLE_STORE")
    response_store = table_store.get_item(Key={"id_Endereco": store_id})
    
    if "Item" in response_store:
        item = convert_decimal_values(response_store["Item"])
        STORE_CACHE[store_id_str] = item
        return item
    
    print(f"Nenhuma loja encontrada com o id: {store_id_str}")
    return None

def get_product_store(lote_id: str):
    lote = get_lote(lote_id)
    if not lote:
        return None
    
    product = get_product(lote["fk_id_Produto"])
    if not product:
        return None
    
    store_id = product["fk_id_Endereco"]
    store = get_store(store_id)
    if not store:
        return None
    
    store_response = StoreResponse(
        id_loja=str(store["id_Endereco"]),
        nome_loja=store["nome_Loja"],
        imagem_loja=get_store_image(store.get("id_Imagem")),
        endereco_loja=store["logradouro"],
        cep_loja=store["cep"],
        tipo_entrega=store["tipo_Entrega"]
    )
    
    return store_response

def get_product_name(lote_id: str):
    lote = get_lote(lote_id)
    if not lote:
        return None
    
    product = get_product(lote["fk_id_Produto"])
    if not product:
        return None
    
    return product["nome"]

def get_image(id_imagem: str):
    if not id_imagem:
        return None
        
    cache_key = f"product:{id_imagem}"
    if cache_key in IMAGE_CACHE:
        return IMAGE_CACHE[cache_key]
    
    try:
        bucket_name = os.environ["BUCKET_NAME_PRODUCT"]
        image_url = f"https://{bucket_name}.s3.amazonaws.com/{id_imagem}"
        IMAGE_CACHE[cache_key] = image_url
        return image_url
    except Exception as ex:
        print(f"Erro ao buscar imagem com id: {id_imagem}: {str(ex)}")
        return None

def get_product_image(lote_id: str):
    lote = get_lote(lote_id)
    if not lote:
        return None
    
    product = get_product(lote["fk_id_Produto"])
    if not product:
        return None
    
    return get_image(product.get("id_imagem"))

def get_mp_sdk(access_token):
    if access_token in MP_SDK_INSTANCES:
        return MP_SDK_INSTANCES[access_token]
    
    sdk = mercadopago.SDK(access_token)
    MP_SDK_INSTANCES[access_token] = sdk
    return sdk

def get_order_qr_code(order: dict):
    payment_id = order.get("id_Pagamento")
    if not payment_id:
        print("ID de Pagamento não encontrado no pedido para buscar QR Code.")
        return None
        
    cache_key = str(payment_id)
    if cache_key in QR_CODE_CACHE:
        return QR_CODE_CACHE[cache_key]
    
    try:
        store_id = order.get("fk_id_Endereco")
        if not store_id:
             print(f"fk_id_Endereco não encontrado no pedido {order.get('id_Pedido')} para buscar QR Code.")
             return None

        store = get_store(store_id)
        if not store:
            print(f"Loja {store_id} não encontrada para buscar QR Code.")
            return None
        
        access_token = store.get("access_token")
        if not access_token:
            print(f"Access token não encontrado para a loja {store_id}.")
            return None
        
        sdk = get_mp_sdk(access_token)
        payment = sdk.payment().get(str(payment_id))
        
        if payment and payment.get("status") == 200 and payment.get("response"):
            qr_code = payment["response"].get("point_of_interaction", {}).get("transaction_data", {}).get("qr_code")
            if qr_code:
                QR_CODE_CACHE[cache_key] = qr_code
                return qr_code
            else:
                print(f"QR code não encontrado na resposta do MP para pagamento {payment_id}.")
        else:
             print(f"Erro ao buscar pagamento {payment_id} no MP: {payment.get('response') if payment else 'Resposta vazia'}")

        return None
    except Exception as ex:
        print(f"Erro ao buscar QR code para pagamento {payment_id}: {str(ex)}")
        return None

def get_order_items(order_id: str):
    table_item_order = get_table("TABLE_ITEM_ORDER")
    
    response_item_order = table_item_order.query(
        IndexName="fk_id_Pedido-index",
        KeyConditionExpression=boto3.dynamodb.conditions.Key("fk_id_Pedido").eq(order_id)
    )
    
    if "Items" not in response_item_order or len(response_item_order["Items"]) == 0:
        print(f"Nenhum item encontrado para o pedido: {order_id}")
        return []
    
    return [convert_decimal_values(item) for item in response_item_order["Items"]]

def get_order_products(order_response: OrderResponse):
    items = get_order_items(order_response.id_Pedido)
    if not items:
        return None
    
    product_list = []
    for item in items:
        lote_id = item.get("fk_id_Lote")
        if not lote_id:
            print(f"Item sem fk_id_Lote no pedido {order_response.id_Pedido}")
            continue
            
        store_info = get_product_store(lote_id)
        if not store_info:
             print(f"Não foi possível obter dados da loja para o lote {lote_id}")
             continue 

        product_response = ProductResponse(
            nome_produto=get_product_name(lote_id),
            imagem_produto=get_product_image(lote_id),
            quantidade=int(item.get("quantidade_item", 0)), 
            valor_unitario=item.get("preco_unitario"),
            loja=store_info
        )
        product_list.append(product_response)
    
    return product_list

def lambda_handler(event, context):
    try:
        query_params = event.get("queryStringParameters", {}) or {}
        cpf = query_params.get("cpf")
        
        limit = int(query_params.get("limit", "10"))
        include_products = query_params.get("include_products", "true").lower() == "true"
        include_qr_code = query_params.get("include_qr_code", "true").lower() == "true"
        
        if not cpf:
            print("CPF não informado")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "campo obrigatório não informado: CPF"})
            }
        
        table_order = get_table("ORDERS_TABLE")
        query_params_dynamo = {
            "IndexName": "fk_Usuario_cpf-index",
            "KeyConditionExpression": boto3.dynamodb.conditions.Key("fk_Usuario_cpf").eq(cpf),
            "Limit": limit
        }
        
        last_evaluated_key_str = query_params.get("nextToken")
        if last_evaluated_key_str:
            try:
                query_params_dynamo["ExclusiveStartKey"] = json.loads(last_evaluated_key_str)
            except json.JSONDecodeError:
                 print(f"nextToken inválido: {last_evaluated_key_str}")
                 return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "nextToken inválido"})
                 }

        response_orders = table_order.query(**query_params_dynamo)
        
        if "Items" not in response_orders or len(response_orders["Items"]) == 0:
            if not last_evaluated_key_str:
                print(f"Nenhum pedido encontrado para o CPF: {cpf}")
                return {
                    "statusCode": 404,
                    "body": json.dumps({"message": f"Nenhum pedido encontrado para o CPF: {cpf}"})
                }
            else:
                 return {
                    "statusCode": 200,
                    "body": json.dumps({"pedidos": [], "nextToken": None})
                 }

        orders_raw = [convert_decimal_values(order) for order in response_orders["Items"]]
        order_list = []
        
        for order_data in orders_raw:
            order_response = OrderResponse(
                id_Pedido=order_data["id_Pedido"],
                id_Pagamento=str(order_data.get("id_Pagamento")),
                status_pedido=order_data.get("status_pedido"),
                valor_total=order_data.get("valor"),
                data_pedido=order_data.get("data_pedido"),
                AvaliacaoFeita=order_data.get("AvaliacaoFeita", False)
            )
            
            if include_qr_code:
                order_response.qr_code = get_order_qr_code(order_data)
            
            if include_products:
                order_response.produtos = get_order_products(order_response)
            
            order_list.append(order_response)
        
        next_token = None
        if "LastEvaluatedKey" in response_orders:
            lek_serializable = convert_decimal_values(response_orders["LastEvaluatedKey"])
            next_token = json.dumps(lek_serializable, default=decimal_serializer)
        
        pedidos_dict_list = [asdict(order) for order in order_list]

        response_body = {
            "pedidos": pedidos_dict_list
        }
        
        if next_token:
            response_body["nextToken"] = next_token
        
        return {
            "statusCode": 200,
            "body": json.dumps(response_body, default=decimal_serializer)
        }
        
    except KeyError as err:
        print(f"Erro de chave: {str(err)} - Verifique se o campo existe nos dados retornados.")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Erro interno ao processar pedido: chave não encontrada {str(err)}"})
        }
    except Exception as ex:
        import traceback
        print(f"Erro inesperado ao buscar pedidos: {str(ex)}")
        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Erro inesperado ao buscar pedidos: {str(ex)}"})
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