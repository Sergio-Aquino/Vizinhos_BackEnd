from dataclasses import dataclass, field
import boto3
import os
import json
import uuid
from botocore.exceptions import ClientError

@dataclass
class Review:
    fk_Usuario_cpf: str
    fk_id_Endereco: int
    avaliacao: int
    comentario: str
    id_Pedido: str
    id_Avaliacao: int = int(str(uuid.uuid4().int)[:18])

    @staticmethod
    def from_json(json_data: dict):
        required_fields = ['fk_Usuario_cpf', 'fk_id_Endereco', 'avaliacao', 'comentario', 'id_Pedido']
        missing_fields = [field for field in required_fields if field not in json_data]
        if missing_fields:
            raise KeyError(f"Campos obrigatórios não informados no corpo da requisição: {', '.join(missing_fields)}")

        if not isinstance(json_data['fk_Usuario_cpf'], str):
            raise TypeError('Campo fk_Usuario_cpf deve ser uma string')
        if not isinstance(json_data['fk_id_Endereco'], int):
            raise TypeError('Campo fk_id_Endereco deve ser um inteiro')
        if not isinstance(json_data['avaliacao'], int):
            raise TypeError('Campo avaliacao deve ser um inteiro')
        if not isinstance(json_data['comentario'], str):
            raise TypeError('Campo comentario deve ser uma string')
        if not isinstance(json_data['id_Pedido'], str):
            raise TypeError('Campo id_Pedido deve ser uma string')

        try:
            review_instance = Review(
                fk_Usuario_cpf=json_data['fk_Usuario_cpf'],
                fk_id_Endereco=json_data['fk_id_Endereco'],
                avaliacao=json_data['avaliacao'],
                comentario=json_data['comentario'],
                id_Pedido=json_data['id_Pedido']
            )
            return review_instance
        except TypeError as e:
            print(f"Erro de TypeError durante a instanciação de Review: {e}")
            raise ValueError(f"Erro ao processar dados da avaliação: {e}")

def lambda_handler(event:any, content:any):
    try:
        user_table_name = os.environ['USER_TABLE']
        address_table_name = os.environ['ADDRESS_TABLE']
        review_table_name = os.environ['REVIEW_TABLE']
        order_table_name = os.environ['ORDER_TABLE']
    except KeyError as e:
        print(f"Erro: Variável de ambiente não definida: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f"Erro de configuração interna: Variável de ambiente {e} não definida."})
        }

    try:
        print("Recebendo requisição...")
        if 'body' not in event or event['body'] is None:
             raise ValueError("Corpo da requisição ausente.")
        
        body = json.loads(event['body'])
        print("Corpo da requisição parseado.")
        
        review = Review.from_json(body)
        print(f"Objeto Review criado para id_Pedido: {review.id_Pedido}")

        dynamodb = boto3.resource('dynamodb')
        table_user = dynamodb.Table(user_table_name)
        table_address_store = dynamodb.Table(address_table_name)
        table_review = dynamodb.Table(review_table_name)
        table_order = dynamodb.Table(order_table_name)
        print("Recursos DynamoDB inicializados.")


        print(f"Validando usuário: {review.fk_Usuario_cpf}")
        response_user_check = table_user.get_item(Key={'cpf': review.fk_Usuario_cpf})
        if 'Item' not in response_user_check:
            print(f"Usuário não encontrado: {review.fk_Usuario_cpf}")
            return {'statusCode': 404, 'body': json.dumps({'message': 'Usuário não encontrado'})}
        print("Usuário validado.")

        print(f"Validando endereço/loja: {review.fk_id_Endereco}")
        response_addr_check = table_address_store.get_item(Key={'id_Endereco': review.fk_id_Endereco})
        if 'Item' not in response_addr_check:
            print(f"Loja/Endereço não encontrado: {review.fk_id_Endereco}")
            return {'statusCode': 404, 'body': json.dumps({'message': 'Loja/Endereço não encontrado'})}
        print("Endereço/Loja validado.")

        print(f"Verificando tipo de usuário para o endereço: {review.fk_id_Endereco}")
        response_seller_check = table_user.query(
            IndexName='fk_id_Endereco-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('fk_id_Endereco').eq(review.fk_id_Endereco)
        )
        if 'Items' not in response_seller_check or not response_seller_check['Items']:
            print(f"Nenhum usuário (proprietário) encontrado para o endereço: {review.fk_id_Endereco}")
            return {'statusCode': 404, 'body': json.dumps({'message': 'Proprietário não encontrado para o endereço informado'})}
        
        seller_found = False
        for item in response_seller_check['Items']:
            if item.get('Usuario_Tipo') in ['seller', 'customer_seller']:
                seller_found = True
                print(f"Endereço pertence a um vendedor (Tipo: {item.get('Usuario_Tipo')}).")
                break
        
        if not seller_found:
            print(f"Endereço {review.fk_id_Endereco} não pertence a um tipo de usuário 'seller' ou 'customer_seller'.")
            return {'statusCode': 400, 'body': json.dumps({'message': 'Só é possível avaliar endereços associados a vendedores'})}
        print("Validação do tipo de usuário do endereço concluída.")

        print(f"Criando avaliação {review.id_Avaliacao} para o pedido {review.id_Pedido}...")
        review_item = {
            'id_Avaliacao': review.id_Avaliacao,
            'fk_Usuario_cpf': review.fk_Usuario_cpf,
            'fk_id_Endereco': review.fk_id_Endereco,
            'avaliacao': review.avaliacao,
            'comentario': review.comentario,
            'id_Pedido': review.id_Pedido
        }
        table_review.put_item(Item=review_item)
        print(f"Avaliação {review.id_Avaliacao} criada com sucesso na tabela {review_table_name}.")

        print(f"Atualizando pedido {review.id_Pedido} na tabela {order_table_name}...")
        try:
            update_response = table_order.update_item(
                Key={'id_Pedido': review.id_Pedido},
                UpdateExpression='SET AvaliacaoFeita = :val',
                ExpressionAttributeValues={':val': True},
                ConditionExpression='attribute_exists(id_Pedido)',
                ReturnValues='UPDATED_NEW'
            )
            print(f"Pedido {review.id_Pedido} atualizado com sucesso. Valores atualizados: {update_response.get('Attributes')}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(f"AVISO: Falha ao atualizar pedido: Pedido com id_Pedido {review.id_Pedido} não encontrado na tabela {order_table_name}. A avaliação foi criada, mas o pedido não foi marcado.")
            else:
                print(f"AVISO: Erro do DynamoDB ao atualizar pedido {review.id_Pedido}: {e}. A avaliação foi criada, mas o pedido pode não ter sido marcado.")

        print("Operação concluída com sucesso.")
        return {
            'statusCode': 201,
            'body': json.dumps(
                {
                    'message': 'Avaliação criada com sucesso!',
                    'avaliacao': review_item
                },
                default=str
            )
        }

    except json.JSONDecodeError as e:
        print(f"Erro de JSONDecodeError: {e}")
        return {'statusCode': 400, 'body': json.dumps({'message': f"JSON inválido no corpo da requisição: {e}"})}
    except (ValueError, KeyError, TypeError) as e:
        print(f"Erro de validação/dados: {e}")
        return {'statusCode': 400, 'body': json.dumps({'message': str(e)})}
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"Erro do DynamoDB: {error_code} - {error_message}")
        status_code = 500
        if error_code in ['ResourceNotFoundException']:
             status_code = 404
        elif error_code in ['ProvisionedThroughputExceededException', 'ThrottlingException']:
             status_code = 503
        return {'statusCode': status_code, 'body': json.dumps({'message': f"Erro no serviço de banco de dados: {error_message}"})}
    except Exception as e:
        print(f"Erro inesperado: {type(e).__name__} - {e}")
        return {'statusCode': 500, 'body': json.dumps({'message': f"Erro interno inesperado no servidor."})}



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