import json
import os
import boto3
from boto3.dynamodb.conditions import Attr

def lambda_handler(event, context):
    try:
        id_loja = event.get("queryStringParameters", {}).get("idLoja")
        if not id_loja:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'idLoja não fornecido'})
            }

        dynamodb = boto3.resource('dynamodb')
        table_review = dynamodb.Table(os.environ['REVIEW_TABLE'])
        table_loja = dynamodb.Table(os.environ['LOJA_TABLE'])

        response_reviews = table_review.scan(
            FilterExpression=Attr('fk_id_Endereco').eq(int(id_loja))
        )

        avaliacoes = response_reviews.get('Items', [])

        if avaliacoes:
            pesos = {1: 0.5, 2: 0.75, 3: 1.0, 4: 1.25, 5: 1.5}
            soma_ponderada = sum(int(av['avaliacao']) * pesos[int(av['avaliacao'])] for av in avaliacoes)
            peso_total = sum(pesos[int(av['avaliacao'])] for av in avaliacoes)
            media_avaliacoes = round(soma_ponderada / peso_total, 2)
        else:
            media_avaliacoes = 0

        response_loja = table_loja.get_item(Key={'id_Endereco': int(id_loja)})
        loja = response_loja.get('Item', {})

        return {
            'statusCode': 200,
            'body': json.dumps({
                'loja': loja,
                'avaliacoes': avaliacoes,
                'media_avaliacoes': media_avaliacoes
            }, default=str)
        }

    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': "Erro ao buscar avaliações e loja: " + str(ex)}, default=str)
        }
