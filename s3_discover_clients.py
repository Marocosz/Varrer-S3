# ================= IMPORTS =================
import boto3
import os
import re
import pandas as pd
import io
import logging
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime, timedelta, timezone
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ConnectTimeoutError, ClientError

# ================= CONFIGURAÇÕES GERAIS =================
load_dotenv()
BUCKET_NAME = os.getenv('BUCKET_NAME')

# Controle global para desativar o OCR caso o serviço esteja inacessível
textract_available = True

# --- LISTA DE PASTAS ALVO ---
# Diretórios específicos no S3 que serão analisados
TARGET_FOLDERS = [
    "000000000000011/000000000000735",
    "000000000000011/000000000000738",
    "000000000000011/000000000000739",
    "000000000000011/000000000000740",
    "000000000000011/000000000000741",
    "000000000000011/000000000000743",
    "000000000000011/000000000000744",
    "000000000000011/000000000000745",
    "000000000000011/000000000000746",
    "000000000000011/000000000000748",
    "000000000000011/000000000000749",
    "000000000000011/000000000000750",
    "000000000000011/000000000000751",
    "000000000000011/000000000000752",
    "000000000000011/000000000000753",
    "000000000000011/000000000000754",
    "000000000000011/000000000000755"
]

# --- PARAMETROS DE EXECUÇÃO ---
# Limite de arquivos a serem lidos via OCR por pasta (Amostragem)
SAMPLES_PER_FOLDER = 80

# Arquivos de saída e log
OUTPUT_FILE = 'relatorio_final_clientes.xlsx'
LOG_FILE = 'log_ocr_direct.log'

# Extensões de arquivo suportadas pelo Amazon Textract
VALID_EXTS = ('.pdf', '.png', '.jpg', '.jpeg', '.tiff')

# ================= CONFIGURAÇÃO DE LOGGING =================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    encoding='utf-8'
)

# ================= FUNÇÕES DE EXTRAÇÃO DE TEXTO =================

def extract_info_from_text(text):
    """
    Analisa o texto bruto retornado pelo OCR e extrai entidades estruturadas (CNPJ, CPF, Email)
    e linhas de contexto relevantes usando Expressões Regulares (Regex).
    """
    info = {
        'CNPJs': [],
        'CPFs': [],
        'Emails': [],
        'Possiveis Empresas': [],
        'Linhas de Contexto': []
    }

    # Palavras-chave que indicam propriedade ou origem do documento
    KEYWORDS_CONTEXTO = [
        'TOMADOR', 'PRESTADOR', 'EMPRESA', 'RAZÃO SOCIAL', 'RAZAO SOCIAL',
        'COMPROVANTE', 'DESTINATÁRIO', 'DESTINATARIO', 'BENEFICIÁRIO',
        'PAGADOR', 'CLIENTE', 'FORNECEDOR', 'LTDA', 'S.A.', 'S/A', 'EIRELI'
    ]

    lines = text.split('\n')

    for line in lines:
        upper_line = line.upper()
        
        # Captura a linha inteira se contiver uma palavra-chave de contexto
        if any(key in upper_line for key in KEYWORDS_CONTEXTO):
            clean_context = re.sub(r'\s+', ' ', line).strip()
            if len(clean_context) > 5:
                info['Linhas de Contexto'].append(clean_context)

        # Heurística para tentar identificar nomes de empresas baseada em sufixos jurídicos
        company_suffixes = [' LTDA', ' S.A.', ' S/A', ' EIRELI', ' ME', ' EPP']
        if any(suf in upper_line for suf in company_suffixes):
            clean_name = re.sub(r'[^\w\s\.\/\-&]', '', line).strip()
            if len(clean_name) > 3 and clean_name not in info['Possiveis Empresas']:
                info['Possiveis Empresas'].append(clean_name)

    # Regex para CNPJ (com ou sem pontuação)
    cnpj_pattern = r'\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b'
    raw_cnpjs = re.findall(cnpj_pattern, text)
    for c in raw_cnpjs:
        clean = re.sub(r'\D', '', c)
        if len(clean) == 14:
            formatted = f"{clean[:2]}.{clean[2:5]}.{clean[5:8]}/{clean[8:12]}-{clean[12:]}"
            if formatted not in info['CNPJs']: info['CNPJs'].append(formatted)

    # Regex para CPF
    cpf_pattern = r'\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b'
    raw_cpfs = re.findall(cpf_pattern, text)
    for c in raw_cpfs:
        clean = re.sub(r'\D', '', c)
        if len(clean) == 11:
            formatted = f"{clean[:3]}.{clean[3:6]}.{clean[6:9]}-{clean[9:]}"
            if formatted not in info['CPFs']: info['CPFs'].append(formatted)

    # Regex para E-mail
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    info['Emails'] = list(set(re.findall(email_pattern, text)))

    return info

# ================= FUNÇÕES DE CONEXÃO AWS =================

def get_aws_clients():
    """
    Estabelece a conexão com a AWS.
    Configurada com 'Fail Fast': timeout curto e sem retentativas automáticas
    para evitar travamento do script em caso de instabilidade de rede.
    """
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    if not aws_access_key or not aws_secret_key or not aws_region:
        raise ValueError("ERRO: Credenciais AWS ou Região não encontradas no .env")

    # Configuração de Rede: Timeout de 2 segundos, 0 retentativas.
    my_config = Config(
        region_name=aws_region,
        retries={'max_attempts': 0},
        connect_timeout=2,
        read_timeout=30
    )

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        config=my_config
    )

    # O Textract é forçado para us-east-1 para mitigar possíveis bloqueios regionais
    textract_client = boto3.client(
        'textract',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        config=my_config,
        region_name='us-east-1' 
    )

    return s3_client, textract_client

def analyze_folder(s3, textract, folder):
    """
    Processa uma pasta específica:
    1. Lista arquivos.
    2. Filtra por data (apenas recentes).
    3. Aplica amostragem (apenas os primeiros X arquivos).
    4. Envia para OCR e extrai dados.
    """
    global textract_available

    prefix = folder.strip()
    if not prefix.endswith('/') and prefix: prefix += '/'

    print(f"\n📂 Analisando: {prefix}")

    stats = {'total_files': 0, 'ocr_performed': 0, 'files_data': []}

    # Definição da data de corte (90 dias atrás em UTC)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
    print(f"   📅 Filtro de Data: >= {cutoff_date.strftime('%d/%m/%Y')}")

    paginator = s3.get_paginator('list_objects_v2')
    
    # Tentativa de listar objetos com tratamento de erro de conexão
    try:
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' not in page: continue

            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('/'): continue # Ignora a própria pasta

                stats['total_files'] += 1

                # Filtro de Data: Ignora arquivos antigos
                last_modified = obj['LastModified']
                if last_modified < cutoff_date:
                    continue

                # Verifica se ainda estamos dentro do limite de amostragem
                if stats['ocr_performed'] < SAMPLES_PER_FOLDER:
                    _, ext = os.path.splitext(key)
                    
                    # Filtra apenas extensões compatíveis com OCR
                    if ext.lower() in VALID_EXTS:

                        print(f"   👁️  Lendo RECENTE [{stats['ocr_performed']+1}]: {os.path.basename(key)}")

                        try:
                            # Baixa o arquivo para a memória RAM (BytesIO)
                            file_stream = io.BytesIO()
                            
                            # CORREÇÃO: Removido o parâmetro Config=... que causava o erro.
                            # Os timeouts já estão configurados na instância do cliente 's3'.
                            s3.download_fileobj(
                                BUCKET_NAME,
                                key,
                                file_stream
                            )
                            file_bytes = file_stream.getvalue()

                            # Validação de tamanho para a API Síncrona do Textract (Limite 5MB)
                            if len(file_bytes) > 5 * 1024 * 1024:
                                logging.warning(f"Ignorado (Grande): {key}")
                                print(f"      ⚠️ Arquivo muito grande (>5MB), pulando.")
                                continue

                            # Processamento OCR
                            full_text = ""
                            if textract_available:
                                try:
                                    resp = textract.detect_document_text(Document={'Bytes': file_bytes})
                                    full_text = "\n".join([b['Text'] for b in resp['Blocks'] if b['BlockType'] == 'LINE'])
                                    
                                    # DEBUG: Mostra se achou texto
                                    if not full_text:
                                        print("      ⚠️ OCR retornou texto vazio.")
                                    
                                except (EndpointConnectionError, ConnectTimeoutError):
                                    print("⚠️ Textract inacessível. OCR desativado para evitar travamentos.")
                                    textract_available = False # Desativa globalmente para esta execução
                                    continue
                                except Exception as e:
                                    print(f"      ❌ ERRO NO TEXTRACT: {e}")
                                    logging.error(f"Erro no Textract: {e}")
                                    continue

                            # Extração de Inteligência do texto
                            info = extract_info_from_text(full_text)

                            # Verifica se foi possível identificar algo útil
                            has_data = any([
                                info['CNPJs'], info['CPFs'], info['Emails'], 
                                info['Possiveis Empresas'], info['Linhas de Contexto']
                            ])

                            # Monta o registro do arquivo
                            file_record = {
                                'Pasta': prefix,
                                'Arquivo': os.path.basename(key),
                                'Status Identificação': "SUCESSO" if has_data else "NENHUM DADO ENCONTRADO",
                                'Data Modificação': last_modified.strftime('%d/%m/%Y'),
                                'CNPJs': ", ".join(info['CNPJs']),
                                'Empresas (Estimado)': ", ".join(info['Possiveis Empresas']),
                                'Linhas de Contexto': " | ".join(info['Linhas de Contexto']),
                                'Emails': ", ".join(info['Emails']),
                                'CPFs': ", ".join(info['CPFs']),
                                'Texto Inicial (OCR)': full_text[:100].replace('\n',' ')
                            }

                            stats['files_data'].append(file_record)
                            stats['ocr_performed'] += 1 # Incrementa apenas se der sucesso total

                        # Tratamento de erros de rede durante o processamento do arquivo individual
                        except (EndpointConnectionError, ConnectTimeoutError) as e:
                            print(f"      ❌ FALHA DE REDE (Download S3): {e}. Pulando...")
                            continue
                        except Exception as e:
                            print(f"      ❌ ERRO GENÉRICO: {e}")
                            logging.error(f"Erro genérico no arquivo {key}: {e}")
                            continue
                            
    except (EndpointConnectionError, ConnectTimeoutError):
        print("   ❌ Conexão perdida durante a listagem de arquivos da pasta.")

    return stats

# ================= EXECUÇÃO PRINCIPAL =================

def run():
    if not BUCKET_NAME:
        print("ERRO: Configure BUCKET_NAME no .env")
        return

    # Testa conexão inicial
    try:
        s3, textract = get_aws_clients()
    except Exception as e:
        print(f"❌ Erro fatal na conexão inicial AWS: {e}")
        return

    all_files_details = []
    folder_summaries = []

    print(f"🚀 Iniciando análise direta em {len(TARGET_FOLDERS)} pastas.")
    print(f"💰 Limite: {SAMPLES_PER_FOLDER} arqs/pasta. (Modo Fail Fast ativo)")

    # Itera sobre cada pasta alvo
    for folder in tqdm(TARGET_FOLDERS, desc="Progresso Geral"):
        stats = analyze_folder(s3, textract, folder)

        # Consolida dados para o relatório de resumo da pasta
        cnpjs_set = set()
        companies_set = set()
        context_set = set()

        for f in stats['files_data']:
            if 'CNPJs' in f and f['CNPJs']: cnpjs_set.update(f['CNPJs'].split(', '))
            if 'Empresas (Estimado)' in f and f['Empresas (Estimado)']: companies_set.update(f['Empresas (Estimado)'].split(', '))
            if 'Linhas de Contexto' in f and f['Linhas de Contexto']: context_set.add(f['Linhas de Contexto'][:50] + "...")

            all_files_details.append(f)

        summary = {
            'Pasta': folder,
            'Total Arquivos (S3)': stats['total_files'],
            'Arquivos Lidos (Recentes)': stats['ocr_performed'],
            'CNPJs Identificados': ", ".join(list(cnpjs_set)),
            'Empresas Identificadas': ", ".join(list(companies_set)),
            'Contexto Geral': " | ".join(list(context_set))[:500]
        }
        folder_summaries.append(summary)

    # Geração do Excel
    print(f"\n💾 Gerando relatório final: {OUTPUT_FILE}...")

    try:
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:

            # Aba 1: Visão Geral
            df_summary = pd.DataFrame(folder_summaries)
            df_summary.to_excel(writer, sheet_name='Resumo por Pasta', index=False)

            # Aba 2: Dados Detalhados
            if all_files_details:
                df_details = pd.DataFrame(all_files_details)
                
                # Organização das colunas para melhor leitura
                cols_order = ['Pasta', 'Arquivo', 'Status Identificação', 'Data Modificação', 'CNPJs',
                              'Empresas (Estimado)', 'Linhas de Contexto', 'Emails', 'CPFs', 'Texto Inicial (OCR)']

                # Cria colunas vazias se não existirem
                for c in cols_order:
                    if c not in df_details.columns: df_details[c] = ""

                df_details = df_details[cols_order]
                df_details.to_excel(writer, sheet_name='Detalhe por Arquivo', index=False)
            else:
                pd.DataFrame(['Nenhum arquivo recente compatível lido']).to_excel(writer, sheet_name='Detalhe por Arquivo')

        print("✅ Relatório gerado com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao salvar Excel: {e}")

if __name__ == "__main__":
    run()