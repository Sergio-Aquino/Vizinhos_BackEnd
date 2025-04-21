import json
import boto3
import os


def lambda_handler(event:any, context:any):
    try:
        id_Produto = event.get('queryStringParameters', {}).get('id_Produto')
        if not id_Produto:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'campo obrigatório não informado: id_Produto'})
            }
        
        product_table = boto3.resource('dynamodb').Table(os.environ['PRODUCT_TABLE'])
        product_response = product_table.get_item(Key={'id_Produto': id_Produto})
        if 'Item' not in product_response:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Produto não encontrado'})
            }
        
        prodcut_characteristics_table = boto3.resource('dynamodb').Table(os.environ['PRODUCT_CHARACTERISTIC_TABLE'])
        prodcut_characteristics_response = prodcut_characteristics_table.query(
            IndexName='fk_Produto_id_Produto-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_Produto_id_Produto').eq(id_Produto)
        )
        for item in prodcut_characteristics_response['Items']:
            prodcut_characteristics_table.delete_item(Key={'fk_Carecteristica_id_Caracteristica': item['fk_Carecteristica_id_Caracteristica'], 'fk_Produto_id_Produto': id_Produto})
        
        product_table.delete_item(Key={'id_Produto': id_Produto})

        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']
        s3.delete_object(Bucket=bucket_name, Key=product_response['Item']['id_imagem'])
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Produto deletado com sucesso'})
        }
    except KeyError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Campo obrigatório não encontrado: {str(err)}'})
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Erro ao deletar produto: {str(ex)}'})
        }    

if __name__ == "__main__":
    os.environ['PRODUCT_TABLE'] = 'Produto'
    os.environ['PRODUCT_CHARACTERISTIC_TABLE'] = 'Produto_Caracteristica'
    os.environ['BUCKET_NAME'] = 'product-image-vizinhos'
    event = {
        'queryStringParameters': {
            'id_Produto': "b2af6336-8caf-4251-bb11-157208e2f5e8"
        }
    }
    print(lambda_handler(event, None))

