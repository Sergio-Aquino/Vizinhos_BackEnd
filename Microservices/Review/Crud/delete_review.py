import json
import os
import boto3

def lambda_handler(event:any, context:any):
    try:
        review_id = event.get('queryStringParameters', {}).get('id_Avaliacao')
        if not review_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message':'id_Avaliacao não fornecido'}, default=str)
            }
        
        review_id = int(review_id)
        if not isinstance(review_id, int):
            raise TypeError('id_Avaliacao deve ser um inteiro')
        
        dynamodb = boto3.resource('dynamodb')
        table_review = dynamodb.Table(os.environ['REVIEW_TABLE'])

        if 'Item' not in table_review.get_item(Key={'id_Avaliacao': review_id}):
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Avaliação não encontrada'}, default=str)
            }
        
        table_review.delete_item(
            Key={
                'id_Avaliacao': review_id,
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message':'Avaliação deletada com sucesso!'}, default=str)
        }

    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': "Erro ao deletar avaliação: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['REVIEW_TABLE'] = ''

    event = {
        'queryStringParameters': {
            'id_Avaliacao': ""
        }
    }
    print(lambda_handler(event, None))