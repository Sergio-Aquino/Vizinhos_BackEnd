import boto3
import os
import json

def lambda_handler(event:any, context:any): 
    try:
        id_imagem = event.get('queryStringParameters', {}).get('id_imagem')
        if not id_imagem:
            raise ValueError("ID da imagem não informado")
        
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']

        response = s3.get_object(Bucket=bucket_name, Key=id_imagem)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ValueError("Erro ao buscar imagem no S3")
        
        image_url = f"https://{bucket_name}.s3.amazonaws.com/{id_imagem}"

        return {
            "statusCode": 200,
            "body": json.dumps({
                "imagem": image_url,
            })
        }
    except ValueError as err:
        print(f"Erro: {err}")
        return {
            'statusCode': 400,
            'body': json.dumps({'mensagem: ' + str(err)}, default=str)
        }
    except Exception as ex:
        print(f"Erro: {ex}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message':'Erro ao retornar imagem: ' + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['BUCKET_NAME'] = ''
    event = {
        'queryStringParameters': {
            "id_imagem": ""
        }
    }  
    print(lambda_handler(event, None))