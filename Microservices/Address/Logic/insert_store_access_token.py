import json
import boto3
import os


def lambda_handler(event:any, context: any): 
    try:
        store_id = int(json.loads(event['body'])['id_Loja']) if 'id_Loja' in json.loads(event['body']) else None
        print(f"ID da loja: {store_id} encontrado no corpo da requisição")
        if store_id is None:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'id_Loja não encontrado no corpo da requisição'})
            }
        
        access_token = json.loads(event['body'])['access_token'] if 'access_token' in json.loads(event['body']) else None
        if access_token is None:
            print("Access token não encontrado no corpo da requisição")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'access_token não encontrado no corpo da requisição'})
            }
        
        dynamodb = boto3.resource('dynamodb')
        table_address_store = dynamodb.Table(os.environ['ADDRESS_TABLE'])
        store = table_address_store.get_item(Key={'id_Endereco': store_id})

        if 'Item' not in store:
            print(f"Loja com id: {store_id} não encontrada")
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Loja não encontrada'})
            }
        
        store = store['Item']

        table_user = dynamodb.Table(os.environ['USER_TABLE'])
        response_user = table_user.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(store_id)
        )

        if 'Items' not in response_user or len(response_user['Items']) == 0 or len(response_user['Items']) > 1:
            print(f"Vendedor não encontrado ou mais de um vendedor encontrado para a loja com id: {store_id}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Não foi possível relacionar a loja com um vendedor'})
            }
        
        if response_user['Items'][0]['Usuario_Tipo'] not in ['seller', 'customer_seller']:
            print(f"Tipo de usuário inválido: {response_user['Items'][0]['Usuario_Tipo']}")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Apenas vendedores podem ter uma integração com o sistema de pagamento do mercado pago'})
            }
        
        store["access_token"] = access_token

        table_address_store.put_item(
            Item=store
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Access token inserido com sucesso', "loja": store}, default=str)
        }

    except KeyError as err:
        print(f"Chave não encontrada: {str(err)}") 
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Chave não encontrada: {str(err)}'}, default=str)
        }
    except Exception as ex:
        print(f"Erro ao inserir access token: {str(ex)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'Erro ao inserir access token: ' + str(ex)}, default=str)
        }
    
if __name__ == "__main__":
    os.environ['ADDRESS_TABLE'] = 'Loja_Endereco'
    os.environ['USER_TABLE'] = 'Usuario'

    event = {
        'body': json.dumps({
            "id_Loja": 185962218056648587,
            "access_token": ""
        })
    }

    print(lambda_handler(event, None))