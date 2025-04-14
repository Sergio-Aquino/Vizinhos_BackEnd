from dataclasses import dataclass
import boto3
import os
import json
import uuid

@dataclass
class Review:
    fk_Usuario_cpf: str
    fk_id_Endereco: int
    avaliacao: int
    comentario: str
    id_Avaliacao: int

    @staticmethod
    def from_json(json_data: dict):
        if not isinstance(json_data['id_Avaliacao'], int):
            raise TypeError('id_Avaliacao deve ser um inteiro')
        if not isinstance(json_data['fk_Usuario_cpf'], str):
            raise TypeError('fk_Usuario_cpf deve ser uma string')
        if not isinstance(json_data['fk_id_Endereco'], int):
            raise TypeError('fk_id_Endereco deve ser um inteiro')
        if not isinstance(json_data['avaliacao'], int):
            raise TypeError('avaliacao deve ser um inteiro')
        if not isinstance(json_data['comentario'], str):
            raise TypeError('comment deve ser uma string')
        
        return Review(**json_data)

def lambda_handler(event:any, context:any): 
    try:
        body = json.loads(event['body'])
        review = Review.from_json(body)

        dynamodb = boto3.resource('dynamodb')

        table_review = dynamodb.Table(os.environ['REVIEW_TABLE'])
        response = table_review.get_item(Key={'id_Avaliacao': review.id_Avaliacao})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps('Avaliação não encontrada')
            }
        
        table_user = dynamodb.Table(os.environ['USER_TABLE'])
        response_user = table_user.get_item(Key={'cpf': review.fk_Usuario_cpf})
        
        if 'Item' not in response_user:
            return {
                'statusCode': 404,
                'body': json.dumps('Usuario não encontrado')
            }

        
        table_address_store = dynamodb.Table(os.environ['ADDRESS_TABLE'])
        if 'Item' not in table_address_store.get_item(Key={'id_Endereco': review.fk_id_Endereco}):
            return {
                'statusCode': 404,
                'body': json.dumps('Loja não encontrada')
            }

        
        table_review.update_item(
            Key={'id_Avaliacao': review.id_Avaliacao},
            UpdateExpression="set fk_Usuario_cpf=:u, fk_id_Endereco=:e, avaliacao=:a, comentario=:c",
            ExpressionAttributeValues={
                ':u': review.fk_Usuario_cpf,
                ':e': review.fk_id_Endereco,
                ':a': review.avaliacao,
                ':c': review.comentario
            })

        return {
            'statusCode': 200,
            'body': json.dumps({"avaliacao": body}, default=str)
        }
    except ValueError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str)
        }
    except KeyError as err:
        return {
            "statusCode": 400,
            'body': json.dumps({'message': f'Campo obrigatório não informado: {str(err)}'}, default=str)
        }
    except TypeError as err:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err)}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': "Erro ao criar avaliação: " + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['REVIEW_TABLE'] = 'Avaliacao'
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['ADDRESS_TABLE'] = 'Loja_Endereco'
    
    event = {
        'body': json.dumps({
            'fk_Usuario_cpf': '52750852811',
            'fk_id_Endereco': 1,
            'avaliacao': 5,
            'comentario': 'Ótimo serviço!',
            'id_Avaliacao': 223840994986634202
        })
    }
    print(lambda_handler(event, None))