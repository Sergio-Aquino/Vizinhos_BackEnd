import json
import boto3
import os


dynamodb = boto3.resource('dynamodb')

def get_user_address(fk_id_Endereco):
    table_address = dynamodb.Table(os.environ['TABLE_ADDRESS'])
    response = table_address.get_item(Key={'id_Endereco': fk_id_Endereco}, ProjectionExpression='cep, logradouro, numero, complemento')

    return response.get('Item')

def get_user(cpf):
    table_user = dynamodb.Table(os.environ['TABLE_USER'])
    response = table_user.get_item(
        Key={'cpf': cpf},
        ProjectionExpression="nome, telefone, fk_id_Endereco"
    )
    return response.get('Item', None)

def lambda_handler(event:any, context:any): 
    try:
        id_pedido = event.get("queryStringParameters", {}).get("id_Pedido", None)
        if id_pedido is None:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'ID do pedido não fornecido'}, default=str)
            }
        table_order = dynamodb.Table(os.environ['TABLE_ORDER'])

        response = table_order.get_item(Key={'id_Pedido': id_pedido})
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'Pedido com id {id_pedido} não encontrado'}, default=str)
            }
        
        order = response['Item']
        user_cpf = order.get('fk_Usuario_cpf', None)
        if user_cpf is None:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'CPF do usuário não encontrado no pedido {id_pedido}'}, default=str)
            }
        
        user = get_user(user_cpf)
        if user is None:
            print(f"Usuário com CPF {user_cpf} não encontrado.")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'Usuário com CPF {user_cpf} não encontrado'}, default=str)
            }

        endereco = None
        if user.get('fk_id_Endereco'):
            endereco = get_user_address(user['fk_id_Endereco'])

        user_info = {
            'nome': user.get('nome'),
            'telefone': user.get('telefone'),
            'endereco': endereco
        }

        return {
            'statusCode': 200,
            'body': json.dumps(user_info, default=str)
        }
    except KeyError as err:
        print(f"Erro ao buscar cliente do pedido {id_pedido}: {err}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Campo obrigatório não encontrado {err}'}, default=str)
        }
    except Exception as ex:
        print(f"Erro ao buscar cliente do pedido {id_pedido}: {ex}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Erro ao buscar cliente do pedido: {id_pedido}. {str(ex)}'}, default=str)
        }



if __name__ == "__main__":
    os.environ['TABLE_ORDER'] = 'Pedido'
    os.environ['TABLE_USER'] = 'Usuario'
    os.environ['TABLE_ADDRESS'] = 'Loja_Endereco'

    event = {
        "queryStringParameters": {
            "id_Pedido": "e5f41839-1163-4c49-80b4-5b8dd91c6e86"
        }
    }
    context = {}

    print(lambda_handler(event, context))