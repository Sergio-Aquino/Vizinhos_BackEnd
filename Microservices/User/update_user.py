import json
import boto3
import os

def lambda_handler(event:any, context:any):
    cpf = json.loads(event['body'])['cpf'] if 'cpf' in json.loads(event['body']) else None
    if not cpf:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'CPF não informado'}, default=str)
        }
    
    new_name = json.loads(event['body'])['nome'] if 'nome' in json.loads(event['body']) else None
    if not new_name:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Nome não informado'}, default=str)
        }

    new_user_type = json.loads(event['body'])['Usuario_Tipo'] if 'Usuario_Tipo' in json.loads(event['body']) else None
    if not new_user_type:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Tipo de usuário não informado'}, default=str)
        }
    
    new_id_address = json.loads(event['body'])['fk_id_Endereco'] if 'fk_id_Endereco' in json.loads(event['body']) else None
    if not new_id_address:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'ID do endereço não informado'}, default=str)
        }
    
    new_phone = json.loads(event['body'])['telefone'] if 'telefone' in json.loads(event['body']) else None
    if not new_phone:
        return {
            'statusCode': 400,
            'body': json.dumps({'message':'Telefone não informado'}, default=str)
        }
    
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['TABLE_NAME'])

        response = table.get_item(Key={'cpf': cpf})
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'message':'Usuário não encontrado'}, default=str)
            }

        table.update_item(
            Key={'cpf': cpf},
            UpdateExpression="SET Usuario_Tipo = :Usuario_Tipo, fk_id_Endereco = :fk_id_Endereco, telefone = :telefone, nome = :nome",
            ExpressionAttributeValues={
                ':nome': new_name,
                ':Usuario_Tipo': new_user_type,
                ':fk_id_Endereco': new_id_address,
                ':telefone': new_phone,
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message':'Usuário atualizado com sucesso!'}, default=str)
        }
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'message':'Erro ao atualizar usuário: ' + str(ex)}, default=str)
        }
    

if __name__ == "__main__":
    os.environ['TABLE_NAME'] = 'Usuario'

    event = {
        "body": json.dumps({
            "nome": "Aquino Lima",
            "cpf": "25563678512",
            "Usuario_Tipo": "seller",
            "fk_id_Endereco": 123456789,
            "telefone": "40028922",
        })
    }
    print(lambda_handler(event, None))