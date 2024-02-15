import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import gzip
import os
import datetime
from apache_beam.io.gcp.bigquery import *
from apache_beam.io.gcp.pubsub import *
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io.filesystem import CompressionTypes
from apache_beam import io
from apache_beam.transforms import util
from google.cloud import storage  

serviceAccount = r'C:\Users\mikew\Desktop\data_flow\teste-dataflow-410816-5c5cff7773d1.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = serviceAccount

#Passando options dentro do scritp
# pipeline_options = {
#     'project': 'teste-dataflow-410816',
#     'runner': 'DataflowRunner',
#     'region': 'southamerica-east1',
#     'staging_location': 'gs://teste_apache_beam_dataflow/temp',
#     'temp_location': 'gs://teste_apache_beam_dataflow/temp',
#     'template_location': 'gs://teste_apache_beam_dataflow/template/desafio1_storage',
#     'requirements_file': 'C:\\Users\\mikew\\Desktop\\data_flow\\desafio1\\requirements.txt'  # Adicione esta linha
    
# }
# pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

#Executando options no momento da execução terminal. Comando para subir script ao GCP como teplate.
#python desafio1_storage_v3.py \ --runner=DataflowRunner \ --project=teste-dataflow-410816 \ --region=southamerica-east1 \ --staging_location=gs://teste_apache_beam_dataflow/temp \ --temp_location=gs://teste_apache_beam_dataflow/temp \ --template_location=gs://teste_apache_beam_dataflow/template/desafio1_storage_v3 \ --requirements_file=C:\Users\mikew\Desktop\data_flow\desafio1\requirements.txt
#python desafio1_storage_v3.py \ --runner=DataflowRunner \ --project=teste-dataflow-410816 \ --region=us-east1 \ --staging_location=gs://teste_apache_beam_dataflow/temp \ --temp_location=gs://teste_apache_beam_dataflow/temp \ --template_location=gs://teste_apache_beam_dataflow/template/desafio1_storage_v3 \ --requirements_file=C:\Users\mikew\Desktop\data_flow\desafio1\requirements.txt
#https://medium.com/google-cloud/installing-python-dependencies-in-dataflow-fe1c6cf57784

class ReadAndDecompress(beam.DoFn):
    def process(self, element):
        with GcsIO().open(element) as f:
            with gzip.GzipFile(fileobj=f, mode='rb') as gz:
                content = gz.read().decode('utf-8')
                lines = content.splitlines()
                lines = lines[1:]
                for line in lines:
                    yield line
        
        
def add_new_column(record):
    now = datetime.datetime.now()
    register_date = now.strftime('%Y-%m-%d %H:%M:%S')
    return record + [register_date]


def ajust_colums_sent_rows_count(record):
    if record[0] is None or record[0] == '':
        record[0] = -1
    return record


def transform_integer_sent_rows_count(record):
    record[0] = int(record[0])
    return record


def replace_type(record):
    record[1] = record[1].strip().replace('orders', 'order').replace('products', 'product')
    return record

def generate_dict(record):
    dict_data = {}
    dict_data['store_reference'] = record[0]
    dict_data['channel_id'] = record[1]
    dict_data['output_channel_id'] = record[2]
    dict_data['shared_integration_channels'] = record[3]
    dict_data['origin'] = record[4]
    if record[5] == '""' or record[5] == '"':
        dict_data['revision'] = None
    else:
        dict_data['revision'] = record[5]
    dict_data['version'] = record[6]
    dict_data['fullcharge'] = record[7]
    dict_data['file'] = record[8]
    dict_data['sent_rows_count'] = record[9]
    dict_data['type'] = record[10]
    dict_data['register_date'] = record[11]
    dict_data['ts'] = record[12]
    dict_data['creation_date'] = record[13]
    #['store_reference', 'channel_id', 'output_channel_id', 'shared_integration_channels', 'origin', 'revision', 'version','fullcharge', 'file', 'sent_rows_count', 'type', 'register_date', 'ts', 'creation_date'] 
    return dict_data

def list_files_bucket():
    bucket_name = 'teste_apache_beam_dataflow'
    folder_name = 'entrada/'
    # Inicializar o cliente do Google Cloud Storage
    storage_client = storage.Client()
    # Acessar o Data Lake
    bucket = storage_client.get_bucket(bucket_name)
    # Listar objetos no Data Lake com pasta especificada
    blobs = bucket.list_blobs(prefix=folder_name)

    # Construir a lista no formato desejado
    lista_arquivos = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if 'gz' in blob.name]


    return lista_arquivos

def run():
    
    list_files = ['20240110']  # '20240108', '20240109',
    list_files = list_files_bucket()
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True #Permite que as dependências do Python especificadas no arquivo de requisitos (requirements.txt) sejam transferidas para os trabalhadores do Dataflow
    with beam.Pipeline(options=options) as p:
    # with beam.Pipeline(options=PipelineOptions()) as p:
        for file in list_files:
            content = (
                p
                | f'Read GCS File {file}' >> beam.Create([file])
                | f'Read and Decompress {file}' >> beam.ParDo(ReadAndDecompress())
                # | "Read GCS Filessss" >> io.ReadFromText(input_file, compression_type=CompressionTypes.GZIP)
                
            )

            transform = (
                content
                | f"Separar dados por virgula {file}" >> beam.Map(lambda record: record.split(';'))
                # | "Excluir linha de cabeçalho" >> beam.Filter(lambda record: record[0] != 'sent_rows_count')
                | f"Criar coluna register_date {file}" >> beam.Map(add_new_column)
                | f"Substituir nulos por -1 {file}" >> beam.Map(ajust_colums_sent_rows_count)
                | f"Transformando sent_rows_count para inteiro {file}" >> beam.Map(transform_integer_sent_rows_count)
                | f"Replace type and strip {file}" >> beam.Map(replace_type)
            )
            
            # create_dict = (
            #     transform
            #     | f"Ajuste ordem de colunas {file}" >> beam.Map(lambda record: record[8:9] + record[9:10] + record[13:14] +
            #                                       record[10:11] + record[2:3] + record[5:6] + record[6:7] + record[7:8] +
            #                                       record[4:5] + record[0:1] + record[1:2] + record[14:] +
            #                                       record[11:12] + record[12:13]
            #                                       )  # Ajuste de colunas

            #     #Se sua função precisa de argumentos adicionais, a chamada usando lambda pode ser útil para passar esses argumentos
            #     # | "Transforma dados em dicionário" >> beam.Map(lambda record: generate_dict(record))
            #     #Se sua função não precisa de argumentos adicionais, a chamada mais concisa.
            #     | f"Transforma dados em dicionário {file}" >> beam.Map(generate_dict)
            # )

            # create_dict | 'Print Content' >> beam.Map(print)
            name_file = file.split('/')[-1].split('_')[0]
            name_file = name_file[4:]
            transform | f"Save storage {file}" >> beam.io.WriteToText(f"gs://teste_apache_beam_dataflow/saida/{name_file}_resultado.csv")

if __name__ == '__main__':
    run()