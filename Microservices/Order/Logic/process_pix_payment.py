import mercadopago
import json
import uuid

def lambda_handler(event:any, context:any):
    try:
        body = json.loads(event.get("body"))
        
        email = body.get("email")
        if not email:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Email não informado"}, default=str)
            }
        
        preco = float(body.get("preco"))
        if not preco:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Preço não informado"}, default=str)
            }

        sdk = mercadopago.SDK("APP_USR-1356231261866648-051013-96ea67cf7f09765fd6a88e99c93bdd9f-2430273423")
        request_options = mercadopago.config.RequestOptions()
        request_options.custom_headers = {
            'x-idempotency-key': str(uuid.uuid4())
        }
        payment_data = {
            "transaction_amount": preco,
            "payment_method_id": "pix",
            "payer": {
                "email": email,
            },
            "description": "Pedido Vizinhos",
            "additional_info": {
                "items": [
                    {
                        "id": item["id"],
                        "title": item["title"],
                        "description": item["description"],
                        "quantity": item["quantity"],
                        "unit_price": item["unit_price"]
                    } for item in body.get("products", [])
                ]
            },
        }

        payment = sdk.payment().create(payment_data, request_options)
        payment_response = payment["response"]
        if payment["status"] != 201:
            return {
                "statusCode": payment_response["status"],
                "body": json.dumps({"message": "Erro ao criar pagamento: " + payment_response['message']}, default=str)
            }
        return json.dumps(
            {
                "transaction_ammount": payment_response["transaction_amount"],
                "payment_id": payment_response["id"],
                "collector_id": payment_response["collector_id"],
                "qr_code": payment_response["point_of_interaction"]["transaction_data"]["qr_code"],
                "qr_code_base64": payment_response["point_of_interaction"]["transaction_data"]["qr_code_base64"],
            }, 
            default=str)
    
    except Exception as ex:
        print("Error processing payment:", ex)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(ex)}, default=str)
        }

if __name__ == "__main__":
    event = {
        "body": json.dumps({
            "email": "sergioadm120@gmail.com",
            "preco": 0.02,
            "products": [
                {
                    "id": "1234",
                    "title": "Test Item",
                    "description": "Description of the test item",
                    "quantity": 1,
                    "unit_price": 0.01
                },
                {
                    "id": "5678",
                    "title": "Test Item 2",
                    "description": "Description of the test item 2",
                    "quantity": 1,
                    "unit_price": 0.01
                }
            ]
        })
    }
    print(lambda_handler(event, {}))