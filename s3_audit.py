# ================= IMPORTS E BIBLIOTECAS =================

# 'boto3': O SDK oficial da AWS. É a ponte entre o Python e a nuvem.
import boto3

# 'os': Permite interagir com o sistema operacional e arquivos.
import os

# 'pickle': Biblioteca nativa do Python para serializar objetos.
import pickle

# 'defaultdict': Dicionário inteligente com valores padrão.
from collections import defaultdict

# 'datetime': Para datas no relatório.
from datetime import datetime

# 'tqdm': Barra de progresso visual.
from tqdm import tqdm

# 'dotenv': Carrega variáveis do arquivo .env.
from dotenv import load_dotenv

# 'pandas': A biblioteca padrão para manipulação de dados tabulares e Excel.
import pandas as pd

# 'math': Usado para calcular quantos arquivos (partes) serão necessários.
import math

# 'logging': Biblioteca para criar o arquivo de LOG (registro de atividades).
import logging

# ================= CARREGAMENTO DE AMBIENTE =================

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')
TARGET_FOLDER = os.getenv('TARGET_FOLDER', '')

# --- TRAVA DE SEGURANÇA ---
try:
    MAX_REQUESTS_SAFETY = int(os.getenv('MAX_REQUESTS_SAFETY', 0))
except ValueError:
    MAX_REQUESTS_SAFETY = 0

# Configurações de Arquivos
OUTPUT_FILE = 'relatorio_s3_matriz.xlsx'
CHECKPOINT_STATS_FILE = 'checkpoint_stats.pkl'
CHECKPOINT_TOKEN_FILE = 'checkpoint_token.txt'
LOG_FILE = 'auditoria_robo.log'

# Configuração de Divisão de Arquivos Excel
ROWS_PER_FILE = 20000

# --- NOVO: CONFIGURAÇÃO DE PASTA IGNORADA ---
# O script vai pular qualquer arquivo que comece com este caminho exato.
IGNORED_PREFIX = "000000000000010/000000000099999/"

# ================= CONFIGURAÇÃO DE LOGGING =================

# Configura o logger para escrever no arquivo e mostrar informações importantes
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8' # Garante que acentos no log não buguem
)

# ================= FUNÇÕES AUXILIARES =================

# --- CORREÇÃO DO ERRO DO PICKLE (CTRL+C) ---
# Esta função SUBSTITUI o lambda. Ela precisa estar no escopo global 
# para que o pickle consiga "encontrá-la" e salvar o checkpoint corretamente.
def create_nested_defaultdict():
    return defaultdict(int)

# ================= FUNÇÕES DO SISTEMA =================

def get_s3_client():
    """
    Cria e retorna o cliente de conexão com o S3.
    """
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    if not aws_access_key or not aws_secret_key:
        error_msg = "ERRO: Credenciais AWS não encontradas no .env"
        logging.error(error_msg)
        raise ValueError(error_msg)

    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

def save_checkpoint(folder_stats, all_known_paths, files_found_paths, next_token, total_files):
    """
    Salva o 'estado' atual do script no disco.
    """
    logging.info(f"Salvando Checkpoint... (Total processado até agora: {total_files})")
    
    # 1. Empacota os dados da memória num dicionário
    data_to_save = {
        'stats': folder_stats,
        'all_paths': all_known_paths,
        'file_paths': files_found_paths,
        'total': total_files
    }
    
    # 2. Salva os dados estatísticos usando Pickle
    try:
        with open(CHECKPOINT_STATS_FILE, 'wb') as f:
            pickle.dump(data_to_save, f)
        
        # 3. Salva o Token da AWS num arquivo texto simples
        if next_token:
            with open(CHECKPOINT_TOKEN_FILE, 'w') as f:
                f.write(next_token)
    except Exception as e:
        logging.error(f"Erro ao salvar checkpoint: {e}")

def load_checkpoint():
    """
    Verifica se existem arquivos de checkpoint salvos e tenta carregá-los.
    """
    if os.path.exists(CHECKPOINT_STATS_FILE) and os.path.exists(CHECKPOINT_TOKEN_FILE):
        print("\n🔄 CHECKPOINT ENCONTRADO! Carregando estado anterior...")
        logging.info("Checkpoint encontrado. Tentando carregar estado anterior...")
        try:
            with open(CHECKPOINT_STATS_FILE, 'rb') as f:
                data = pickle.load(f)
            
            with open(CHECKPOINT_TOKEN_FILE, 'r') as f:
                token = f.read().strip()
                
            print(f"   -> Retomando de {data['total']} arquivos já processados.")
            logging.info(f"Retomando varredura a partir de {data['total']} arquivos.")
            return data['stats'], data['all_paths'], data['file_paths'], data['total'], token
        
        except Exception as e:
            msg = f"Erro ao ler checkpoint ({e}). Arquivo corrompido ou versão incompatível. Iniciando do zero."
            print(f"⚠️ {msg}")
            logging.warning(msg)
    
    return None, None, None, 0, None

def generate_excel_report(folder_stats, all_known_paths, files_found_paths, status_msg="Concluído com Sucesso"):
    """
    Gera o relatório em formato de MATRIZ com suporte a múltiplos arquivos (Split).
    """
    print(f"\n💾 Compilando dados para Excel...")
    logging.info("Iniciando compilação e geração dos arquivos Excel...")
    
    # 1. PREPARAÇÃO E NORMALIZAÇÃO
    data_rows = []
    
    # Normaliza chaves para garantir unicidade
    normalized_stats = defaultdict(create_nested_defaultdict) # Usa a função global
    
    for raw_folder, years in folder_stats.items():
        clean_folder = raw_folder.rstrip('/')
        if not clean_folder: clean_folder = "Raiz"
        
        for year, count in years.items():
            normalized_stats[clean_folder][year] += count

    # Prepara a lista de dados
    for folder, years_data in normalized_stats.items():
        for year, count in years_data.items():
            data_rows.append({
                'Pasta': folder,
                'Ano': year,
                'Arquivos': count
            })

    # 2. CRIAÇÃO DA MATRIZ
    df = pd.DataFrame(data_rows)
    df_matrix = pd.DataFrame()

    if not df.empty:
        df_matrix = df.pivot_table(index='Pasta', columns='Ano', values='Arquivos', fill_value=0)
        df_matrix['Total Geral'] = df_matrix.sum(axis=1)
        df_matrix = df_matrix.sort_index()
    else:
        df_matrix = pd.DataFrame(columns=["Pasta", "Total Geral"])

    # 3. TRATAMENTO DE PASTAS VAZIAS
    all_clean_paths = {p.rstrip('/') for p in all_known_paths if p.rstrip('/')}
    stats_clean_paths = set(normalized_stats.keys())
    empty_folders = sorted(list(all_clean_paths - stats_clean_paths))
    
    df_empty = pd.DataFrame()
    if empty_folders:
        df_empty = pd.DataFrame({'Pasta': empty_folders})
        df_empty['Status'] = 'Vazia ou Apenas Subpastas'

    # Resumo
    df_resumo = pd.DataFrame([
        {"Item": "Status da Execução", "Valor": status_msg},
        {"Item": "Data do Relatório", "Valor": datetime.now().strftime('%d/%m/%Y %H:%M:%S')},
        {"Item": "Total de Arquivos Listados", "Valor": df_matrix['Total Geral'].sum() if not df_matrix.empty else 0},
        {"Item": "Bucket", "Valor": BUCKET_NAME},
        {"Item": "Pasta Ignorada", "Valor": IGNORED_PREFIX}
    ])

    # 4. SALVAMENTO COM DIVISÃO (SPLIT)
    total_rows = len(df_matrix)
    num_files = math.ceil(total_rows / ROWS_PER_FILE) if total_rows > 0 else 1

    print(f"   -> Total de linhas na matriz: {total_rows}")
    logging.info(f"Total de linhas na matriz: {total_rows}. Dividindo em {num_files} arquivos.")

    try:
        for i in range(num_files):
            start_row = i * ROWS_PER_FILE
            end_row = start_row + ROWS_PER_FILE
            
            df_chunk = df_matrix.iloc[start_row:end_row]
            
            if num_files > 1:
                current_filename = OUTPUT_FILE.replace('.xlsx', f'_parte_{i+1}.xlsx')
            else:
                current_filename = OUTPUT_FILE

            print(f"      Salvando {current_filename} (Linhas {start_row} a {min(end_row, total_rows)})...")

            with pd.ExcelWriter(current_filename, engine='openpyxl') as writer:
                df_chunk.to_excel(writer, sheet_name='Matriz de Arquivos')
                
                if i == 0:
                    df_resumo.to_excel(writer, sheet_name='Resumo', index=False)
                    if not df_empty.empty:
                        df_empty.to_excel(writer, sheet_name='Pastas Vazias', index=False)
        
        print(f"✅ Todos os relatórios Excel salvos com sucesso!")
        logging.info("Arquivos Excel gerados com sucesso.")
        
    except Exception as e:
        msg = f"Erro ao salvar Excel: {e}"
        print(f"❌ {msg}")
        logging.error(msg)

    if "Sucesso" in status_msg:
        logging.info("Execução finalizada com sucesso. Removendo checkpoints.")
        if os.path.exists(CHECKPOINT_STATS_FILE): os.remove(CHECKPOINT_STATS_FILE)
        if os.path.exists(CHECKPOINT_TOKEN_FILE): os.remove(CHECKPOINT_TOKEN_FILE)

def scan_bucket(bucket_name, prefix_folder):
    """
    Varre o bucket com suporte a Checkpoint, Contador Real, Logs e Proteção de Erros.
    """
    if not bucket_name:
        print("ERRO CRÍTICO: BUCKET_NAME não definido no arquivo .env")
        return

    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    
    # 1. TENTA CARREGAR O CHECKPOINT
    stats, paths, files_paths, total_start, start_token = load_checkpoint()
    
    if stats:
        folder_stats = stats
        all_known_paths = paths
        files_found_paths = files_paths
        total_files = total_start
    else:
        # --- USO DA FUNÇÃO GLOBAL (FIX PICKLE) ---
        folder_stats = defaultdict(create_nested_defaultdict)
        all_known_paths = set()
        files_found_paths = set()
        total_files = 0

    start_msg = f"bucket '{bucket_name}'"
    if prefix_folder: start_msg += f" na pasta '{prefix_folder}'"
    
    print(f"Iniciando varredura em: {start_msg}...")
    logging.info(f"Iniciando varredura. Bucket: {bucket_name}, Prefix: {prefix_folder}")
    logging.info(f"Pasta ignorada configurada: {IGNORED_PREFIX}")

    if MAX_REQUESTS_SAFETY > 0:
        print(f"⚠️  MODO SEGURO ATIVO: Limite de {MAX_REQUESTS_SAFETY} requisições.")
        logging.info(f"Modo seguro ativo. Limite: {MAX_REQUESTS_SAFETY}")

    pagination_config = {'PageSize': 1000}
    if start_token:
        pagination_config['StartingToken'] = start_token

    page_iterator = paginator.paginate(
        Bucket=bucket_name, 
        Prefix=prefix_folder,
        PaginationConfig=pagination_config
    )
    
    status_final = "Concluído com Sucesso"
    requests_made = 0
    pages_since_checkpoint = 0
    ignored_count = 0 # Contador para log

    # --- BLOCO TRY/EXCEPT PRINCIPAL (Protege o Loop Inteiro) ---
    try:
        with tqdm(page_iterator, desc="Lendo AWS") as pbar:
            
            for page in pbar:
                requests_made += 1
                pages_since_checkpoint += 1
                
                # --- AUTO-SAVE (CHECKPOINT) ---
                if 'NextContinuationToken' in page and pages_since_checkpoint >= 500:
                    save_checkpoint(folder_stats, all_known_paths, files_found_paths, page['NextContinuationToken'], total_files)
                    pages_since_checkpoint = 0
                    logging.info(f"Checkpoint automático salvo. Ignorados até agora: {ignored_count}")

                # --- TRAVA DE SEGURANÇA ---
                if MAX_REQUESTS_SAFETY > 0 and requests_made > MAX_REQUESTS_SAFETY:
                    status_final = f"Interrompido (Limite: {MAX_REQUESTS_SAFETY})"
                    logging.warning("Limite de segurança de requisições atingido. Parando.")
                    break 

                if 'Contents' not in page:
                    continue

                # Processamento dos arquivos (Item a Item)
                for obj in page['Contents']:
                    # --- TRATAMENTO DE ERRO INDIVIDUAL (PER FILE) ---
                    # Se um arquivo der erro, o script NÃO PARA. Ele loga e vai pro próximo.
                    try:
                        key = obj['Key']
                        
                        # --- LÓGICA DE IGNORAR PASTA ESPECÍFICA ---
                        if key.startswith(IGNORED_PREFIX):
                            ignored_count += 1
                            continue # Pula este arquivo e vai para o próximo loop
                        # -------------------------------------------

                        last_modified = obj['LastModified']
                        
                        if key.endswith('/'):
                            all_known_paths.add(key)
                            continue

                        folder_path = os.path.dirname(key)
                        if not folder_path: folder_path = "Raiz"
                        
                        files_found_paths.add(folder_path)
                        
                        parts = folder_path.split('/')
                        current_build = ""
                        for part in parts:
                            if part == "Raiz": continue
                            current_build = f"{current_build}{part}/" if current_build else f"{part}/"
                            all_known_paths.add(current_build)

                        year = last_modified.year
                        folder_stats[folder_path][year] += 1
                        total_files += 1

                    except Exception as e_file:
                        # Se der erro num arquivo específico (ex: sem data), loga e continua
                        logging.error(f"Erro ao processar arquivo '{obj.get('Key', 'DESCONHECIDO')}': {e_file}")
                        continue
                
                pbar.set_postfix({'Arquivos': total_files})
                current_next_token = page.get('NextContinuationToken', None)
                
                if MAX_REQUESTS_SAFETY > 0 and requests_made > MAX_REQUESTS_SAFETY:
                    break

    except KeyboardInterrupt:
        print("\n\n⚠️  USUÁRIO INTERROMPEU (Ctrl+C). Salvando estado...")
        logging.warning("Usuário interrompeu execução via teclado (Ctrl+C).")
        status_final = "Cancelado pelo Usuário"
        if 'current_next_token' in locals() and current_next_token:
            save_checkpoint(folder_stats, all_known_paths, files_found_paths, current_next_token, total_files)
    
    except Exception as e:
        print(f"\n\n❌ ERRO INESPERADO: {e}")
        logging.critical(f"Erro Crítico Geral na Varredura: {e}", exc_info=True)
        status_final = f"Erro Crítico: {str(e)}"
        # Tenta salvar estado de emergência
        if 'current_next_token' in locals() and current_next_token:
            try:
                save_checkpoint(folder_stats, all_known_paths, files_found_paths, current_next_token, total_files)
            except:
                pass

    finally:
        print(f"\nProcesso finalizado. Total acumulado: {total_files} arquivos.")
        logging.info(f"Varredura encerrada. Total Processado: {total_files}. Total Ignorado (Prefix): {ignored_count}")
        generate_excel_report(folder_stats, all_known_paths, files_found_paths, status_msg=status_final)

if __name__ == "__main__":
    scan_bucket(BUCKET_NAME, TARGET_FOLDER)