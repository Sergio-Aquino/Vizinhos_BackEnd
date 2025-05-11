from dataclasses import dataclass
import datetime
from decimal import Decimal
import json
import boto3
import os
import uuid
import re


@dataclass
class Order:
    fk_Usuario_cpf: str
    fk_Lote_id_Lote: str
    valor: Decimal
    quantidade: int
    tipo_entrega: str
    status_pedido: str = "Aguardando confirmação"
    data_pedido: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hora_atualizacao: str = data_pedido
    id_Pedido: str = None


    @staticmethod
    def from_json(json_data: dict):
        if not isinstance(json_data['fk_Usuario_cpf'], str):
            raise TypeError('fk_Usuario_cpf deve ser uma string')
        if not isinstance(json_data['fk_Lote_id_Lote'], str):
            raise TypeError('fk_Lote_id_Lote deve ser uma string')
        if not isinstance(json_data['valor'], (float, int)):
            raise TypeError('valor deve ser um decimal')
        if not isinstance(json_data['quantidade'], int):
            raise TypeError('quantidade deve ser um inteiro')
        if not isinstance(json_data['tipo_entrega'], str):
            raise TypeError('tipo_entrega deve ser uma string')

        json_data['fk_Usuario_cpf'] = re.sub(r'\D', '', json_data['fk_Usuario_cpf'])
        if not re.match(r'^\d{11}$', json_data.get('fk_Usuario_cpf', '')):
            raise ValueError('Formatação de CPF inválida')
        
        json_data['valor'] = Decimal(str(json_data['valor']))
        json_data['id_Pedido'] = str(uuid.uuid4())

        return Order(**json_data)

def lambda_handler(event:any, context:any): 
    body = json.loads(event['body'])
    order = Order.from_json(body)

    table_user = boto3.resource('dynamodb').Table(os.environ['TABLE_USER'])
    table_lote = boto3.resource('dynamodb').Table(os.environ['TABLE_LOTE'])
    table_order = boto3.resource('dynamodb').Table(os.environ['TABLE_ORDER'])

    response_user = table_user.get_item(
        Key={'cpf': order.fk_Usuario_cpf}
    )
    if 'Item' not in response_user:
        print(f"Usuário não encontrado: {order.fk_Usuario_cpf}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Usuário não encontrado'})
        }

    response_lote = table_lote.get_item(
        Key={'id_Lote': order.fk_Lote_id_Lote}
    )
    if 'Item' not in response_lote:
        print(f"Lote não encontrado: {order.fk_Lote_id_Lote}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Lote não encontrado'})
        }
    
    table_order.put_item(Item=order.__dict__)
    return {
        'statusCode': 200,
        'body': json.dumps(
            {'message': 'Pedido criado com sucesso', 
             'id_Pedido': order.id_Pedido,
             'fk_Usuario_cpf': order.fk_Usuario_cpf,
             'fk_Lote_id_Lote': order.fk_Lote_id_Lote,
             'valor': float(order.valor),
             'quantidade': order.quantidade,
             'tipo_entrega': order.tipo_entrega,
             'status_pedido': order.status_pedido,
             'data_pedido': order.data_pedido,
             'hora_atualizacao': order.hora_atualizacao,
            }
        )
    }


if __name__ == "__main__":
    os.environ['TABLE_USER'] = 'Usuario'
    os.environ['TABLE_LOTE'] = 'Produto_Lote'
    os.environ['TABLE_ORDER'] = 'Pedido'
    event = {
        "body": json.dumps({
            "fk_Usuario_cpf": "48812172830",
            "fk_Lote_id_Lote": "5c07dfdd-c929-455c-8a16-6091a3d4868f",
            "valor": 100.50,
            "quantidade": 2,
            "tipo_entrega": "Entrega Rápida"
        })
    }
    print(lambda_handler(event, None))