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
    id_Avaliacao: int = int(str((uuid.uuid4().int))[:18])

    @staticmethod
    def from_json(json_data: dict):
        if not isinstance(json_data['fk_Usuario_cpf'], str):
            raise TypeError('fk_Usuario_cpf deve ser uma string')
        if not isinstance(json_data['fk_id_Endereco'], int):
            raise TypeError('fk_id_Endereco deve ser um inteiro')
        if not isinstance(json_data['avaliacao'], int):
            raise TypeError('avaliacao deve ser um inteiro')
        if not isinstance(json_data['comentario'], str):
            raise TypeError('comment deve ser uma string')
        
        return Review(**json_data)

def lambda_handler(event:any, content:any):
    try:
        body = json.loads(event['body'])
        review = Review.from_json(body)

        dynamodb = boto3.resource('dynamodb')

        table_user = dynamodb.Table(os.environ['USER_TABLE'])
        response = table_user.get_item(Key={'cpf': review.fk_Usuario_cpf})
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Usuario não encontrado'})
            }

        
        table_address_store = dynamodb.Table(os.environ['ADDRESS_TABLE'])
        if 'Item' not in table_address_store.get_item(Key={'id_Endereco': review.fk_id_Endereco}):
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Loja não encontrada'})
            }
        
        response_user = table_user.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(review.fk_id_Endereco)
        )

        if 'Items' not in response_user or len(response_user['Items']) == 0:
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Proprietario não encontrado'})
            }
        
        if response_user['Items'][0]['Usuario_Tipo'] not in ['seller', 'customer_seller']:
            return {
                'statusCode': 400,
                'body': json.dumps({'message':'Só é possível avaliar lojas'})
            }
                
        table_review = dynamodb.Table(os.environ['REVIEW_TABLE'])
        table_review.put_item(
            Item={
                'id_Avaliacao': review.id_Avaliacao,
                'fk_Usuario_cpf': review.fk_Usuario_cpf,
                'fk_id_Endereco': review.fk_id_Endereco,
                'avaliacao': review.avaliacao,
                'comentario': review.comentario,
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps(
                {
                    'message': 'Avaliação criada com sucesso!',
                    'avaliacao': {
                        'id_Avaliacao': review.id_Avaliacao,
                        'fk_Usuario_cpf': review.fk_Usuario_cpf,
                        'fk_id_Endereco': review.fk_id_Endereco,
                        'avaliacao': review.avaliacao,
                        'comentario': review.comentario
                    }
                },
                default=str
            )
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
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['ADDRESS_TABLE'] = 'Loja_Endereco'
    os.environ['REVIEW_TABLE'] = 'Avaliacao'

    event = {
        'body': json.dumps({
            'fk_Usuario_cpf': '52750852811',
            'fk_id_Endereco': 170474410818097413,
            'avaliacao': 5,
            'comentario': 'Boa loja!'
        })
    }
    print(lambda_handler(event, None))