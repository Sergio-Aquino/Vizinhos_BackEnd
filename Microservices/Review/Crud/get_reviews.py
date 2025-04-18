import json
import os
import boto3

def lambda_handler(event:any, context:any): 
    try:
        dynamodb = boto3.resource('dynamodb')
        table_review = dynamodb.Table(os.environ['REVIEW_TABLE'])
        response = table_review.scan()

        return {
            'statusCode': 200,
            'body': json.dumps({"avaliacoes": response['Items']}, default=str)
        }

    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': "Erro ao buscar avaliações: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['REVIEW_TABLE'] = 'Avaliacao'
    print(lambda_handler(None, None))