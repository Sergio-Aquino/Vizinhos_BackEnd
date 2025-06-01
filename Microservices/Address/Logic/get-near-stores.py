import json
import boto3
import os
import requests
import re
from math import radians, sin, cos, sqrt, atan2

IMAGE_CACHE = {}
CEP_CACHE = {}

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

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

def get_table(table_env_var):
    table_name = os.environ[table_env_var]
    return dynamodb.Table(table_name)

def get_cep_coordinates(cep):
    if cep in CEP_CACHE:
        return CEP_CACHE[cep]
    
    try:
        response = requests.get(f"https://cep.awesomeapi.com.br/json/{cep}")
        if response.status_code != 200:
            return None
        
        data = response.json()
        coordinates = {
            'latitude': float(data["lat"]),
            'longitude': float(data["lng"])
        }
        
        CEP_CACHE[cep] = coordinates
        return coordinates
    except Exception as ex:
        print(f"Erro ao buscar coordenadas do CEP {cep}: {str(ex)}")
        return None

def get_user_by_email(email):
    user_table = get_table('TABLE_USER')
    response = user_table.query(
        IndexName='email-index',
        KeyConditionExpression=boto3.dynamodb.conditions.Key('email').eq(email)
    )
    
    if 'Items' in response and len(response['Items']) > 0:
        return response['Items'][0]
    
    return None

def get_address(address_id):    
    address_table = get_table('ADRESS_STORE_TABLE')
    response = address_table.get_item(Key={'id_Endereco': address_id})
    
    if 'Item' in response:
        return response['Item']
    
    return None

def get_store_image(id_imagem):
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

def get_all_stores(limit=100):
    address_table = get_table('ADRESS_STORE_TABLE')
    user_table = get_table('TABLE_USER')
    
    scan_params = {}
    
    stores = []
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_params['ExclusiveStartKey'] = start_key
        
        addresses = address_table.scan(**scan_params)
        
        if 'Items' not in addresses:
            print("Nenhum endereço encontrado.")
            break

        for address in addresses['Items']:
            address_id = address.get("id_Endereco")
            if not address_id:
                continue

            response_user = user_table.query(
                IndexName='fk_id_Endereco-index',
                KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(address_id)
            )

            if 'Items' in response_user and len(response_user['Items']) > 0:
                user_info = response_user['Items'][0]
                if user_info.get('Usuario_Tipo') in ['seller', 'customer_seller']:
                    stores.append(address)
        
        start_key = addresses.get('LastEvaluatedKey', None)
        done = start_key is None

        if len(stores) >= limit: 
             print(f"Atingido limite de {limit} lojas encontradas. Interrompendo scan.")
             break 
        
    return stores

def get_stores_within_500_meters(origin_cep, stores):
    origin_coords = get_cep_coordinates(origin_cep)
    if not origin_coords:
        raise ValueError("Problema ao validar CEP informado")
    
    origin_latitude = origin_coords['latitude']
    origin_longitude = origin_coords['longitude']

    stores_within_500_meters = []
    for store in stores:
        store_cep = store.get("cep")
        if not store_cep:
            continue

        store_coords = get_cep_coordinates(store_cep)
        if not store_coords:
            continue

        store_latitude = store_coords['latitude']
        store_longitude = store_coords['longitude']
        
        distance = haversine_distance(origin_latitude, origin_longitude, store_latitude, store_longitude)

        if distance <= 500.0: 
            stores_within_500_meters.append(store)

    return stores_within_500_meters

def lambda_handler(event: any, context: any):
    try:
        query_params = event.get('queryStringParameters', {}) or {}
        email = query_params.get('email')
        
        try:
            limit = min(int(query_params.get('limit', '100')), 1000)
        except ValueError:
            limit = 100
        
        if not email:
            raise ValueError("email não fornecido")
        
        if not isinstance(email, str):
            raise TypeError("email deve ser uma string")
        
        if not is_valid_email(email):
            raise ValueError("email inválido")

        user = get_user_by_email(email)
        if not user:
            print(f"Usuário não encontrado: {email}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': "Usuário não encontrado"}, default=str)
            }
        
        address_id_str = user.get('fk_id_Endereco')
        if not address_id_str:
             print(f"Usuário {email} não possui fk_id_Endereco.")
             return {
                'statusCode': 400,
                'body': json.dumps({'message': "Endereço não associado ao usuário"}, default=str)
            }
        try:
            address_id = int(address_id_str)
        except (ValueError, TypeError):
            print(f"fk_id_Endereco inválido para usuário {email}: {address_id_str}")
            return {
                'statusCode': 500, 
                'body': json.dumps({'message': "ID de endereço inválido no registro do usuário"}, default=str)
            }

        address = get_address(address_id)
        if not address:
            print(f"Endereço do usuário não encontrado: {address_id}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': "Endereço do usuário não encontrado"}, default=str)
            }

        cep = address.get('cep')
        if not cep:
            print(f"Endereço {address_id} não possui CEP.")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': "CEP não encontrado para o endereço do usuário"}, default=str)
            }

        all_stores = get_all_stores(limit)
        
        stores_within_500_range = get_stores_within_500_meters(cep, all_stores)

        result_stores = []
        for store in stores_within_500_range:
            store_data = store.copy()
            store_image_id = store_data.get('id_Imagem')
            store_data['imagem'] = get_store_image(store_image_id) if store_image_id else None
            result_stores.append(store_data)

        return {
            "statusCode": 200,
            "body": json.dumps({"lojas": result_stores}, default=str),
        }

    except KeyError as err:
        print(f"Campo obrigatório não informado: {str(err)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Campo obrigatório não informado: {str(err)}'}, default=str),
        }
    except ValueError as err:
        print(f"message: {str(err)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str),
        }
    except TypeError as err:
        print(f"message: {str(err)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str),
        }
    except Exception as ex:
        print(f"message: {str(ex)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "Erro ao buscar lojas próximas: " + str(ex)}, default=str),
        }


if __name__ == "__main__":
   os.environ['TABLE_USER'] = ''
   os.environ['ADRESS_STORE_TABLE'] = ''
   os.environ['BUCKET_NAME'] = ''

   event = {
        'queryStringParameters': {
            'email': ""
        }
   }
   print(lambda_handler(event, None))