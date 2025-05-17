import json
import boto3
import os
from decimal import Decimal

def lambda_handler(event:any, context:any): 
    try:
        id_produto = json.loads(event["body"])['id_produto']
        valor_promocao = json.loads(event["body"])['valor_promocao']

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['PRODUCT_TABLE'])

        product = table.get_item(
            Key={
                'id_Produto': id_produto
            }
        )

        if 'Item' not in product:
             print(f"Produto não encontrado: {id_produto}")
             return {
                'statusCode': 404,
                'body': json.dumps({"message": "Produto não encontrado"}, default=str)
             }

        if valor_promocao <= 0:
                raise ValueError("Valor promocao deve ser maior que 0")
        
        product = product['Item']
        product['valor_venda'] = Decimal(str(valor_promocao))
        product['flag_oferta'] = True

        table.put_item(
            Item=product
        )

        return {
            'statusCode': 200,
            'body': json.dumps(
                {
                      "message": "Preço promocional aplicado com sucesso",
                      "produto": product
                }, default=str)
        }

    except ValueError as ex:
         print(f"Valor promocao deve ser maior que 0: {str(ex)}")
    except KeyError as err:
        print(f"Chave obrigatória não encontrada: {err}")
        return {
            'statusCode': 400,
            'body': json.dumps({"message": "Campo obrigatório não encontrado: " + str(err)}, default=str)
        }
    except Exception as ex:
        print(f"Erro ao aplicar preço promocional: {ex}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "Erro ao aplicar preço promocional: " + str(ex)}, default=str)
        }
    
if __name__ == "__main__":
    os.environ['PRODUCT_TABLE'] = 'Produto'
    event = {
        "body": json.dumps({
            "id_produto": "e9488bc7-a561-4447-8554-8b48165dd626",
            "valor_promocao": 10.99
        })
    }
    context = {}
    print(lambda_handler(event, context))
        