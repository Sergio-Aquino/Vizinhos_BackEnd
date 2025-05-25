from dataclasses import dataclass
import datetime
from decimal import Decimal
import json
import boto3
import os
import uuid
import re
from typing import List
import mercadopago
from zoneinfo import ZoneInfo

@dataclass
class Order_Item:
    fk_id_Lote: str
    fk_id_Pedido: str
    quantidade_item: int
    preco_unitario: Decimal

    @staticmethod
    def from_json(json_data: dict):
        if not isinstance(json_data['fk_id_Lote'], str):
            raise TypeError('fk_id_Lote deve ser uma string')
        if not isinstance(json_data['fk_id_Pedido'], str):
            raise TypeError('fk_id_Pedido deve ser uma string')
        if not isinstance(json_data['quantidade_item'], int):
            raise TypeError('quantidade_item deve ser um inteiro')
        if not isinstance(json_data['preco_unitario'], (float, int)):
            raise TypeError('preco_unitario deve ser um decimal')
        
        json_data['preco_unitario'] = Decimal(str(json_data['preco_unitario']))
        
        return Order_Item(**json_data)

@dataclass
class Order:
    id_Loja: int
    fk_Usuario_cpf: str
    valor: Decimal
    tipo_entrega: str
    status_pedido: str = "Aguardando Pagamento"
    data_pedido: str = datetime.datetime.now(ZoneInfo("America/Sao_Paulo")) .strftime("%Y-%m-%d %H:%M:%S")
    hora_atualizacao: str = data_pedido
    item_pedido: List[Order_Item] = None
    id_Pedido: str = None
    id_Pagamento: str = None
    id_Transacao: str = None


    @staticmethod
    def from_json(json_data: dict):
        if not isinstance(json_data['fk_Usuario_cpf'], str):
            raise TypeError('fk_Usuario_cpf deve ser uma string')
        if not isinstance(json_data['valor'], (float, int)):
            raise TypeError('valor deve ser um decimal')
        if not isinstance(json_data['tipo_entrega'], str):
            raise TypeError('tipo_entrega deve ser uma string')

        json_data['fk_Usuario_cpf'] = re.sub(r'\D', '', json_data['fk_Usuario_cpf'])
        if not re.match(r'^\d{11}$', json_data.get('fk_Usuario_cpf', '')):
            raise ValueError('Formatação de CPF inválida')
        
        json_data['valor'] = Decimal(str(json_data['valor']))
        json_data['id_Pedido'] = str(uuid.uuid4())

        items = []
        for item in json_data['item_pedido']:
            item['fk_id_Pedido'] = json_data['id_Pedido']
            items.append(Order_Item.from_json(item))
        json_data['item_pedido'] = items

        return Order(**json_data)

def generate_pix_payment(order: Order, email: str):
    payer = email
    table_store = boto3.resource('dynamodb').Table(os.environ['STORE_ADDRESS_TABLE'])
    store = table_store.get_item(Key={'id_Endereco': order.id_Loja})['Item']
    access_token = store['access_token']

    sdk = mercadopago.SDK(access_token)
    request_options = mercadopago.config.RequestOptions()
    request_options.custom_headers = {
        'x-idempotency-key': str(uuid.uuid4())
    }

    payment_data = {
        "transaction_amount": float(order.valor),
        "payment_method_id": "pix",
        "payer": {
            "email": payer,
        },
        "description": "Pedido Vizinhos",
        "additional_info": {
            "items": [
                {
                    "id": item.fk_id_Pedido,
                    "title": item.fk_id_Lote,
                    "quantity": item.quantidade_item,
                    "unit_price": float(item.preco_unitario)
                } for item in order.item_pedido
            ]
        },
    }
    payment = sdk.payment().create(payment_data, request_options)
    payment_response = payment["response"]

    if payment["status"] != 201:
            return {
                "statusCode": payment_response["status"],
                "body": json.dumps({"message": "Erro ao criar pagamento: " + payment_response['message']}, default=str)
            }
    return json.dumps(
            {
                "transaction_ammount": payment_response["transaction_amount"],
                "payment_id": payment_response["id"],
                "collector_id": payment_response["collector_id"],
                "qr_code": payment_response["point_of_interaction"]["transaction_data"]["qr_code"],
                "qr_code_base64": payment_response["point_of_interaction"]["transaction_data"]["qr_code_base64"],
            }, 
            default=str)

def lambda_handler(event:any, context:any):
    try:

        body = json.loads(event['body'])
        order = Order.from_json(body)

        table_user = boto3.resource('dynamodb').Table(os.environ['TABLE_USER'])
        table_lote = boto3.resource('dynamodb').Table(os.environ['TABLE_LOTE'])
        table_order = boto3.resource('dynamodb').Table(os.environ['TABLE_ORDER'])
        table_item_order = boto3.resource('dynamodb').Table(os.environ['TABLE_ITEM_ORDER'])
        table_product = boto3.resource('dynamodb').Table(os.environ['TABLE_PRODUCT'])

        response_user = table_user.get_item(
            Key={'cpf': order.fk_Usuario_cpf}
        )
        if 'Item' not in response_user:
            print(f"Usuário não encontrado: {order.fk_Usuario_cpf}")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Usuário não encontrado'})
            }
        
        email = response_user['Item']['email']

        valor_total = 0
        for item in order.item_pedido:
            response_lote = table_lote.get_item(
                Key={'id_Lote': item.fk_id_Lote}
            )
            if 'Item' not in response_lote:
                print(f"Lote não encontrado: {item.fk_id_Lote}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': f'Lote {item.fk_id_Lote} não encontrado'})
                }
            response_lote_item = response_lote['Item']

            product = table_product.get_item(
                Key={'id_Produto': response_lote_item['fk_id_Produto']}
            )

            if 'Item' not in product:
                print(f"Produto não encontrado: {response_lote_item['fk_id_Produto']}")
                return {
                    'statusCode': 404,
                    'body': json.dumps({'message': f'Produto {response_lote_item["fk_id_Produto"]} não encontrado'})
                }
            
            product_item = product['Item']
            valor_total += product_item['valor_venda'] * item.quantidade_item

        if valor_total != order.valor:
            print(f"Valor total do pedido ({valor_total}) não corresponde ao valor informado ({order.valor})")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Valor total do pedido não corresponde ao valor informado'})
            }
            
        
        response_payment = json.loads(generate_pix_payment(order, email))

        for item in order.item_pedido:
            table_item_order.put_item(Item=item.__dict__)

        table_order.put_item(Item={
                'id_Pedido': order.id_Pedido,
                'fk_Usuario_cpf': order.fk_Usuario_cpf,
                'valor': order.valor,
                'tipo_entrega': order.tipo_entrega,
                'status_pedido': order.status_pedido,
                'data_pedido': order.data_pedido,
                'hora_atualizacao': order.hora_atualizacao,
                'fk_id_Endereco': order.id_Loja,
                'id_Pagamento': response_payment['payment_id'],
                'id_Transacao': order.id_Transacao
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps(
                {
                'message': 'Pedido criado com sucesso', 
                'id_Pedido': order.id_Pedido,
                'fk_Usuario_cpf': order.fk_Usuario_cpf,
                'valor': float(order.valor),
                'tipo_entrega': order.tipo_entrega,
                'status_pedido': order.status_pedido,
                'data_pedido': order.data_pedido,
                'hora_atualizacao': order.hora_atualizacao,
                'pagamento': response_payment
                }
            )
        }
    except KeyError as err:
        print(f"Campo obrigatório ausente: {str(err)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Campo obrigatório ausente: {str(err)}'})
        }
    except TypeError as err:
        print(str(err))
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)})
        }
    except ValueError as err:
        print(str(err))
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)})
        }
    except Exception as ex:
        print(f"Erro ao criar pedido: {ex}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro ao criar pedido: ' + str(ex)}, default=str)
        }


if __name__ == "__main__":
    os.environ['TABLE_USER'] = 'Usuario'
    os.environ['TABLE_LOTE'] = 'Produto_Lote'
    os.environ['TABLE_ORDER'] = 'Pedido'
    os.environ['TABLE_ITEM_ORDER'] = 'Itens_Pedido'
    os.environ['TABLE_PRODUCT'] = 'Produto'
    os.environ['STORE_ADDRESS_TABLE'] = 'Loja_Endereco'
    event = {
        "body": json.dumps({
            "fk_Usuario_cpf": "48812172830",
            "valor": 180.0,
            "tipo_entrega": "Entrega Rápida",
            "item_pedido": [
                {
                    "fk_id_Lote": "2d8d657c-3381-44f2-b7d9-cc71ad35e0e7",
                    "quantidade_item": 2,
                    "preco_unitario": 40.0
                },
                {
                    "fk_id_Lote": "816470bc-2e54-4ec6-ba7c-0497dc461af4",
                    "quantidade_item": 2,
                    "preco_unitario": 50.0
                }
            ],
            "id_Loja": 185962218056648587
            
        })
    }
    print(lambda_handler(event, None))