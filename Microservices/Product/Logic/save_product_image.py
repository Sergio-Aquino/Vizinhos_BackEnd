import json
import boto3
import os
import base64
import uuid

def lambda_handler(event:any, context:any):
    try:
        base64_image = json.loads(event['body'])['image'] if 'image' in json.loads(event['body']) else None
        if not base64_image:
            raise ValueError("Imagem não informada")
        
        file_extension = json.loads(event['body'])['file_extension'] if 'file_extension' in json.loads(event['body']) else None
        if not file_extension:
            raise ValueError("Extensão do arquivo não informada")
        
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']

        image_data = base64.b64decode(base64_image)
        file_name = f"{uuid.uuid4()}.{file_extension}"

        s3.put_object(Bucket=bucket_name, Key=file_name, Body=image_data, ContentType=f'image/{file_extension}', ACL='public-read')

        return {
            'statusCode': 200,
            'body': json.dumps({"file_name": file_name})
        }
    except ValueError as err:
        print(err)
        return None
    except Exception as ex:
        print(f"Erro ao salvar imagem: {ex}")
        return None
    

if __name__ == "__main__":
    os.environ['BUCKET_NAME'] = 'product-image-vizinhos'

    with open('Microservices\\Product\\Crud\\Resources\\imagem_teste.jpg', 'rb') as image_file:
        base64_image = base64.b64encode(image_file.read()).decode('utf-8')

    event = {
        "body": json.dumps({
            "image": "",
            "file_extension": "jpg"
        })
    }
    print(lambda_handler(event, None))
