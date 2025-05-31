import json
import os
import boto3
import datetime
from zoneinfo import ZoneInfo
import mercadopago
import uuid

dynamodb = boto3.resource('dynamodb')

def is_vendor(email: str):
    table_user = dynamodb.Table(os.environ['USER_TABLE'])
    try:
        response_user = table_user.query(
            IndexName='email-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('email').eq(email)
        )

        if 'Items' in response_user and response_user['Items'][0].get('Usuario_Tipo') in ['seller', 'customer_seller']:
            return True, response_user['Items'][0]
        return False, None
    except Exception as ex:
        print(f"Erro ao verificar se o usuário é um vendedor: {ex}")
        raise


def generate_pix_payment(email: str, value: float):
    payer = email
    access_token = os.environ.get('VIZINHOS_ACCESS_TOKEN')

    sdk = mercadopago.SDK(access_token)
    request_options = mercadopago.config.RequestOptions()
    request_options.custom_headers = {
        'x-idempotency-key': str(uuid.uuid4())
    }

    payment_data = {
        "transaction_amount": float(value),
        "payment_method_id": "pix",
        "payer": {
            "email": payer,
        },
        "description": "Plano de Vendedor - Vizinhos",
    }
    payment = sdk.payment().create(payment_data, request_options)
    payment_response = payment["response"]

    if payment["status"] != 201:
            print(f"Erro ao criar pagamento: {payment_response['message']}")
            return {
                "statusCode": payment["status"],
                "body": json.dumps({"message": "Erro ao criar pagamento: " + payment_response['message']}, default=str)
            }
    return {
        "statusCode": payment["status"],
        "body":
            json.dumps(
                {
                    "transaction_ammount": payment_response["transaction_amount"],
                    "payment_id": payment_response["id"],
                    "collector_id": payment_response["collector_id"],
                    "qr_code": payment_response["point_of_interaction"]["transaction_data"]["qr_code"],
                    "qr_code_base64": payment_response["point_of_interaction"]["transaction_data"]["qr_code_base64"],
                }, default=str)
    }

def lambda_handler(event:any, context:any):
    try:
        email = json.loads(event['body'])['email']
        if not isinstance(email, str):
            raise TypeError("O campo 'email' deve ser uma string.")
        
        vendor_plan = json.loads(event['body'])['vendor_plan']
        if not isinstance(vendor_plan, str):
            raise TypeError("O campo 'vendor_plan' deve ser uma string.")
        if vendor_plan not in ['one_month', 'three_month', 'six_month']:
            raise ValueError("O campo 'vendor_plan' deve ser um dos seguintes valores: 'basic', 'premium', 'enterprise'.")
        
        bool, vendor = is_vendor(email)

        if bool:
            vendor['Plano_Vendedor'] = vendor_plan
            vendor['Status_Plano_Vendedor'] = "Aguardando Pagamento"
            vendor['Data_Atualizacao_Plano_Vendedor'] = datetime.datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")

            if vendor_plan == 'one_month':
                payment = generate_pix_payment(email, 1)
            elif vendor_plan == 'three_month':
                payment = generate_pix_payment(email, 2.5)
            elif vendor_plan == 'six_month':
                payment = generate_pix_payment(email, 3)
            
            if payment.get("statusCode") != 201:
                return payment
            
            vendor['id_Pagamento_Plano_Vendedor'] = json.loads(payment['body'])["payment_id"]
            
            table_user = dynamodb.Table(os.environ['USER_TABLE'])
            table_user.put_item(Item=vendor)

            print(f"Subscrição de plano de vendedor efetivada para o usuário: {email}")
        
            return payment
        else:
            print(f"Usuário {email} não é um vendedor.")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': f"Usuário {email} não é um vendedor."})
            }


    except KeyError as err:
        print(f"Campo obrigatório ausente: {err}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f"Campo obrigatório ausente: {err}"})
        }
    except ValueError as ve:
        print(f"Erro de valor: {ve}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f"Erro de valor: {ve}"})
        }
    except TypeError as te:
        print(f"Erro de tipo: {te}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f"Erro de tipo: {te}"})
        }
    except Exception as ex:
        print(f"erro: {ex}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f"Erro ao criar subscrição de plano de vendedor: {str(ex)}"})
        }
    
if __name__ == "__main__":
    os.environ['USER_TABLE'] = 'Usuario'
    os.environ['VIZINHOS_ACCESS_TOKEN'] = "APP_USR-1356231261866648-051013-96ea67cf7f09765fd6a88e99c93bdd9f-2430273423"
    event = {
        'body': json.dumps({
            'email': 'sergioadm120@gmail.com',
            'vendor_plan': 'one_month'
        })
    }
    print(lambda_handler(event, None))