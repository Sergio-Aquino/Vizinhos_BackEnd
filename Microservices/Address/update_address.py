import json
import boto3
import os

def create_address_for_customer(address_id, cep, logradouro, numero, complemento):
    try:
        dynanodb = boto3.resource('dynamodb')
        table = dynanodb.Table(os.environ['TABLE_NAME'])

        table.update_item(
            Key={'id_Endereco': address_id},
            UpdateExpression="SET cep = :cep, logradouro = :logradouro, numero = :numero, complemento = :complemento",
            ExpressionAttributeValues={
                ':cep': cep,
                ':logradouro': logradouro,
                ':numero': numero,
                ':complemento': complemento
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': "Endereço atualizado com sucesso!"}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro atualizar endereço: ' + str(ex)}, default=str)
        }
    
def create_address_for_seller_or_customer_seller(address_id, cep, logradouro, numero, complemento, event):
    store_name = json.loads(event['body'])['nome_Loja'] if 'nome_Loja' in json.loads(event['body']) else None
    if not store_name:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Nome da loja não informado'}, default=str)
        }

    store_description = json.loads(event['body'])['descricao_Loja'] if 'descricao_Loja' in json.loads(event['body']) else None
    if not store_description:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Descrição da loja não informada'}, default=str)
        }
    
    image_id = json.loads(event['body'])['id_Imagem'] if 'id_Imagem' in json.loads(event['body']) else None
    if not image_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'ID da imagem da loja não informado'}, default=str)
        }
    
    delivery_type = json.loads(event['body'])['tipo_Entrega'] if 'tipo_Entrega' in json.loads(event['body']) else None
    if not delivery_type:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Tipo de entrega não informado'}, default=str)
        }

    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])

        table.update_item(
            Key={'id_Endereco': address_id},
            UpdateExpression="SET cep = :cep, logradouro = :logradouro, numero = :numero, complemento = :complemento, nome_Loja = :nome_Loja, descricao_Loja = :descricao_Loja, id_Imagem = :id_Imagem, tipo_Entrega = :tipo_Entrega",
            ExpressionAttributeValues={
                ':cep': cep,
                ':logradouro': logradouro,
                ':numero': numero,
                ':complemento': complemento,
                ':nome_Loja': store_name,
                ':descricao_Loja': store_description,
                ':id_Imagem': image_id,
                ':tipo_Entrega': delivery_type
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': "Endereço atualizado com sucesso!"}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message':'Erro ao criar endereço: ' + str(ex)}, default=str)
        }


def lambda_handler(event:any, context:any):
    address_id = json.loads(event['body'])['id_Endereco'] if 'id_Endereco' in json.loads(event['body']) else None
    if not address_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'ID do endereço não informado'}, default=str)
        }
    
    cep = json.loads(event['body'])['cep'] if 'cep' in json.loads(event['body']) else None
    if not cep:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'CEP não informado'}, default=str)
        }
    
    logradouro = json.loads(event['body'])['logradouro'] if 'logradouro' in json.loads(event['body']) else None
    if not logradouro:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Logradouro não informado'}, default=str)
        }
    
    numero = json.loads(event['body'])['numero'] if 'numero' in json.loads(event['body']) else None
    if not numero:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Número não informado'}, default=str)
        }
    
    complemento = json.loads(event['body'])['complemento'] if 'complemento' in json.loads(event['body']) else None
    if not complemento:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Complemento não informado'}, default=str)
        }
    
    user_type = json.loads(event['body'])['user_type'] if 'user_type' in json.loads(event['body']) else None
    if not user_type:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Tipo de usuário não informado'}, default=str)
        }
    
    if user_type not in ['seller', 'customer', 'seller_customer']:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Tipo de usuário inválido'}, default=str)
        }
    
    
    if user_type == 'customer':
        return create_address_for_customer(address_id, cep, logradouro, numero, complemento)
    else:
        return create_address_for_seller_or_customer_seller(address_id, cep, logradouro, numero, complemento, event)
    

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Loja_Endereco'
    event = {
        "body": json.dumps({
            "id_Endereco": 300763759403059632,
            "cep": "40028922",
            "logradouro": "Rua ataulizar rua seller_customer",
            "numero": 127,
            "complemento": "Apto 1 update seller_customer",
            "user_type": "seller",
            "nome_Loja": "Loja Teste update",
            "descricao_Loja": "Descrição da loja teste update seller_customer",
            "id_Imagem": "https://us-east-2.console.aws.amazon.com/s3/object/loja-profile-pictures?region=us-east-2&bucketType=general&prefix=37dc297e-527b-4744-8f00-95a3bb4d25dd.jpg",
            "tipo_Entrega": "Entrega rápida"
        })
    }
    print(lambda_handler(event, None))