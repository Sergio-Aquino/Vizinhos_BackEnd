import json
import boto3
import os
import requests
import re
from math import radians, sin, cos, sqrt, atan2

def is_valid_email(email: str) -> bool:
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_regex, email) is not None


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000

    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance


def get_all_stores(address_table, user_table):
    addresses = address_table.scan()
    if 'Items' not in addresses:
        return []
    
    stores = []
    for address in addresses['Items']:
        address_id = address["id_Endereco"]

        response_user = user_table.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(address_id)
        )

        if 'Items' not in response_user or len(response_user['Items']) == 0:
            continue

        if response_user['Items'][0]['Usuario_Tipo'] not in ['seller', 'customer_seller']:
           continue
        
        stores.append(address)

    return stores

def get_stores_within_500_meters(orign_cep, stores):
    brasil_api_response = requests.get(f"https://brasilapi.com.br/api/cep/v2/{orign_cep}")

    if brasil_api_response.status_code != 200:
        raise ValueError("Problema ao validar CEP informado")
    
    brasil_api_response = brasil_api_response.json()
    
    origin_latitude =float(brasil_api_response["location"]["coordinates"]["latitude"])
    origin_longitude = float(brasil_api_response["location"]["coordinates"]["longitude"])

    stores_within_500_meters = []
    for store in stores:
        store_cep = store["cep"]
        if not store_cep:
            continue

        store_cep_response = requests.get(f"https://brasilapi.com.br/api/cep/v2/{store_cep}")
        if store_cep_response.status_code != 200:
            continue

        address_data = store_cep_response.json()
        store_latitude = float(address_data["location"]["coordinates"]["latitude"])
        store_longitude = float(address_data["location"]["coordinates"]["longitude"])

        
        distance = haversine_distance(origin_latitude, origin_longitude, store_latitude, store_longitude)

        if distance <= 500.0: 
            stores_within_500_meters.append(store)

    return stores_within_500_meters

def get_store_image(id_imagem):
    try:
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']
        
        if not id_imagem:
            raise ValueError("ID da imagem não informado")
            
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']

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
        email = event.get('queryStringParameters', {}).get('email', None)

        if not email:
            raise ValueError("email não fornecido")
        
        if not isinstance(email, str):
            raise TypeError("email deve ser uma string")
        
        if not is_valid_email(email):
            raise ValueError("email inválido")

        dynamodb = boto3.resource('dynamodb')
        user_table = dynamodb.Table(os.environ['TABLE_USER'])

        response_user = user_table.query(
            IndexName='email-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('email').eq(email)
        )

        if 'Items' not in response_user or len(response_user['Items']) == 0:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': "Usuário não encontrado"}, default=str)
            }
        
        address_id = int(response_user['Items'][0]['fk_id_Endereco'])

        address_table = dynamodb.Table(os.environ['ADRESS_STORE_TABLE'])
        response_address = address_table.get_item(Key={'id_Endereco': address_id})

        if 'Item' not in response_address:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': "Endereço do usuário encontrado"}, default=str)
            }

        cep = response_address['Item']['cep']

        stores = get_all_stores(address_table, user_table)
        stores_with_500_range = get_stores_within_500_meters(cep, stores)

        for store in stores_with_500_range:
            store_image = store['id_Imagem']
            store['imagem'] = get_store_image(store_image) if store_image else None

        return {
            "statusCode": 200,
            "body": json.dumps({"lojas": stores_with_500_range}, default=str),
        }

    except KeyError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Campo obrigatório não informado: {str(err)}'}, default=str),
        }
    except ValueError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str),
        }
    except TypeError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str),
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "Erro ao buscar lojas próximas: " + str(ex)}, default=str),
        }

if __name__ == "__main__":
   os.environ['TABLE_USER'] = 'Usuario'
   os.environ['ADRESS_STORE_TABLE'] = 'Loja_Endereco'
   os.environ['BUCKET_NAME'] = 'loja-profile-pictures'

   event = {
        'queryStringParameters': {
            'email': "sergioadm120@gmail.com"
        }
   }
   print(lambda_handler(event, None))