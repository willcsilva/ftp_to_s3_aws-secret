import paramiko
import boto3
from botocore.exceptions import NoCredentialsError
import json

# Configurações do AWS S3
aws_access_key = 'sua_access_key_aws'
aws_secret_key = 'sua_secret_key_aws'
s3_bucket_name = 'seu_bucket_s3'

# Configurações do AWS Secrets Manager
secret_name = 'seu_secret_name'  # Nome do seu segredo no Secrets Manager
region_name = 'sua_região_aws'   # Região onde o segredo está armazenado

def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    else:
        raise Exception("O segredo não foi encontrado.")

def download_from_sftp_and_upload_to_s3(s3_bucket_name, secret_name, region_name):
    try:
        # Recuperar as credenciais do SFTP do AWS Secrets Manager
        secret_data = get_secret(secret_name, region_name)
        sftp_username = secret_data['username']
        sftp_password = secret_data['password']

         # Conectar ao servidor SFTP
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_username, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Listar os arquivos no diretório remoto
        remote_files = sftp.listdir(sftp_remote_path)

        # Conectar ao S3
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

        # Fazer download dos arquivos do SFTP e enviar para o S3
        for file_name in remote_files:
            remote_file_path = f"{sftp_remote_path}/{file_name}"
            local_file_path = f"/caminho/local/{file_name}"  # Substitua pelo caminho local desejado

            sftp.get(remote_file_path, local_file_path)
            s3.upload_file(local_file_path, s3_bucket_name, file_name)

            print(f"Arquivo {file_name} baixado e enviado para o S3 com sucesso!")

        # Fechar conexões
        sftp.close()
        transport.close()

    except Exception as e:
        print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    download_from_sftp_and_upload_to_s3(sftp_host, sftp_port, sftp_username, sftp_password, sftp_remote_path, aws_access_key, aws_secret_key, s3_bucket_name)
