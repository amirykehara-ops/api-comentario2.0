import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    print(event)

    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_name = os.environ["INGEST_BUCKET"]

    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)

    # Guardar en S3 (Estrategia de Ingesta Push)
    s3 = boto3.client('s3')
    file_key = f"{tenant_id}/{uuidv1}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=json.dumps(comentario),
        ContentType='application/json'
    )

    print(f"Archivo guardado en S3: s3://{bucket_name}/{file_key}")

    # Salida (json)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'response': response,
        's3_path': f"s3://{bucket_name}/{file_key}"
    }
