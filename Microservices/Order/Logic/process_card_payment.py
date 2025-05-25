import mercadopago

def generate_card_token(card_number, card_expiration_month, card_expiration_year, card_cvv):
    sdk = mercadopago.SDK("TEST-1356231261866648-051013-0d2ef2167a37e823d733d05ec30379f0-2430273423")

    card_data = {
        "card_number": card_number,
        "expiration_month": card_expiration_month,
        "expiration_year": card_expiration_year,
        "security_code": card_cvv,
        "cardholder": {
            "name": "APRO",
            "identification": {
                "type": "CPF",
                "number": "12345678909"
            }
        }
    }
    
    card_token_response = sdk.card_token().create(card_data)
    if card_token_response["status"] == 201:
        return card_token_response["response"]["id"]
    else:
        print("Error generating card token:", card_token_response)
        return None

def lambda_handler(event:any, context:any):
    sdk = mercadopago.SDK("TEST-1356231261866648-051013-0d2ef2167a37e823d733d05ec30379f0-2430273423")

    token = generate_card_token("5031433215406351", 11, 2030, "123")

    payment_data = {
        "transaction_amount": 100.0,
        "token": token,
        "description": "Pedido de teste",
        "installments": 1,
        "payment_method_id": "master",
        "payer": {
            "email": "aquino.lima@aluno.ifsp.edu.br",
            "identification": {
                "type": "CPF",
                "number": "12345678909"
            },
            "first_name": "TESTUSER598344673",
        }
    }

    payment_response = sdk.payment().create(payment_data)
    payment = payment_response["response"]

    print(payment)



if __name__ == "__main__":
    print(lambda_handler({}, {}))