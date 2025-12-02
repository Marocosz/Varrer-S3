# ================= IMPORTS E BIBLIOTECAS =================

import boto3
import os
import pickle
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv
import pandas as pd
import math
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

# --- CONFIGURAÇÃO DE PASTA IGNORADA ---
# O script vai remover esta pasta da lista de varredura inicial (Map & Attack).
# Certifique-se de que o caminho corresponde a uma das pastas que serão mapeadas na raiz.
IGNORED_PREFIX = "000000000000010/" 

# ================= CONFIGURAÇÃO DE LOGGING =================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8'
)

# ================= FUNÇÕES AUXILIARES =================

def create_nested_defaultdict():
    """
    Função global necessária para o pickle salvar o defaultdict corretamente.
    """
    return defaultdict(int)

# ================= FUNÇÕES DO SISTEMA =================

def get_s3_client():
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

def get_immediate_subfolders(s3_client, bucket, prefix):
    """
    NOVO: Mapeia apenas as pastas imediatas usando Delimiter='/'.
    Isso é extremamente rápido e permite filtrar pastas inteiras antes de varrer.
    """
    print(f"🗺️  Mapeando pastas na raiz de '{prefix}'...")
    logging.info(f"Iniciando mapeamento estrutural em: {bucket}/{prefix}")
    
    folders = []
    
    # Garante formato correto do prefixo
    search_prefix = prefix
    if search_prefix and not search_prefix.endswith('/'):
        search_prefix += '/'

    paginator = s3_client.get_paginator('list_objects_v2')
    
    # O Delimiter '/' faz a mágica de listar como se fossem pastas de sistema de arquivos
    iterator = paginator.paginate(Bucket=bucket, Prefix=search_prefix, Delimiter='/')

    for page in iterator:
        # CommonPrefixes contém as "pastas"
        if 'CommonPrefixes' in page:
            for p in page['CommonPrefixes']:
                folders.append(p['Prefix'])
    
    # Ordena para garantir consistência entre execuções
    folders.sort()
    
    print(f"   -> Encontradas {len(folders)} pastas principais.")
    logging.info(f"Mapeamento concluído. {len(folders)} pastas identificadas.")
    return folders

def save_checkpoint(folder_stats, all_known_paths, files_found_paths, next_token, total_files, current_folder_idx):
    """
    Salva o estado.
    ATUALIZADO: Agora salva também 'folder_idx' para saber em qual pasta da lista paramos.
    """
    logging.info(f"Salvando Checkpoint... (Total: {total_files}, Pasta Index: {current_folder_idx})")
    
    data_to_save = {
        'stats': folder_stats,
        'all_paths': all_known_paths,
        'file_paths': files_found_paths,
        'total': total_files,
        'folder_idx': current_folder_idx # Salva o progresso na lista de pastas
    }
    
    try:
        with open(CHECKPOINT_STATS_FILE, 'wb') as f:
            pickle.dump(data_to_save, f)
        
        # Lógica do Token:
        # Se tem token, salva ele (estamos no meio de uma pasta).
        if next_token:
            with open(CHECKPOINT_TOKEN_FILE, 'w') as f:
                f.write(next_token)
        # Se não tem token (terminamos a pasta e vamos pular pra próxima), limpa o arquivo.
        elif os.path.exists(CHECKPOINT_TOKEN_FILE):
             os.remove(CHECKPOINT_TOKEN_FILE)
             
    except Exception as e:
        logging.error(f"Erro ao salvar checkpoint: {e}")

def load_checkpoint():
    """
    Carrega o estado anterior.
    ATUALIZADO: Retorna também o índice da pasta onde parou.
    """
    if os.path.exists(CHECKPOINT_STATS_FILE):
        print("\n🔄 CHECKPOINT ENCONTRADO! Carregando estado anterior...")
        logging.info("Checkpoint encontrado. Tentando carregar...")
        try:
            with open(CHECKPOINT_STATS_FILE, 'rb') as f:
                data = pickle.load(f)
            
            token = None
            if os.path.exists(CHECKPOINT_TOKEN_FILE):
                with open(CHECKPOINT_TOKEN_FILE, 'r') as f:
                    token = f.read().strip()
            
            # Recupera o índice da pasta (se não existir, assume 0)
            idx = data.get('folder_idx', 0)
            
            print(f"   -> Retomando da pasta nº {idx+1}, Arquivos processados: {data['total']}")
            logging.info(f"Retomando varredura. Pasta Index: {idx}, Arquivos: {data['total']}")
            
            return data['stats'], data['all_paths'], data['file_paths'], data['total'], token, idx
        
        except Exception as e:
            msg = f"Erro ao ler checkpoint ({e}). Iniciando do zero."
            print(f"⚠️ {msg}")
            logging.warning(msg)
    
    # Retorna valores zerados se não houver checkpoint
    return None, None, None, 0, None, 0

def generate_excel_report(folder_stats, all_known_paths, files_found_paths, status_msg="Concluído com Sucesso"):
    print(f"\n💾 Compilando dados para Excel...")
    logging.info("Gerando Excel...")
    
    data_rows = []
    normalized_stats = defaultdict(create_nested_defaultdict)
    
    for raw_folder, years in folder_stats.items():
        clean_folder = raw_folder.rstrip('/')
        if not clean_folder: clean_folder = "Raiz"
        for year, count in years.items():
            normalized_stats[clean_folder][year] += count

    for folder, years_data in normalized_stats.items():
        for year, count in years_data.items():
            data_rows.append({'Pasta': folder, 'Ano': year, 'Arquivos': count})

    df = pd.DataFrame(data_rows)
    df_matrix = pd.DataFrame()

    if not df.empty:
        df_matrix = df.pivot_table(index='Pasta', columns='Ano', values='Arquivos', fill_value=0)
        df_matrix['Total Geral'] = df_matrix.sum(axis=1)
        df_matrix = df_matrix.sort_index()
    else:
        df_matrix = pd.DataFrame(columns=["Pasta", "Total Geral"])

    all_clean_paths = {p.rstrip('/') for p in all_known_paths if p.rstrip('/')}
    stats_clean_paths = set(normalized_stats.keys())
    empty_folders = sorted(list(all_clean_paths - stats_clean_paths))
    
    df_empty = pd.DataFrame()
    if empty_folders:
        df_empty = pd.DataFrame({'Pasta': empty_folders, 'Status': 'Vazia ou Apenas Subpastas'})

    df_resumo = pd.DataFrame([
        {"Item": "Status", "Valor": status_msg},
        {"Item": "Data", "Valor": datetime.now().strftime('%d/%m/%Y %H:%M:%S')},
        {"Item": "Total Arquivos", "Valor": df_matrix['Total Geral'].sum() if not df_matrix.empty else 0},
        {"Item": "Pasta Ignorada", "Valor": IGNORED_PREFIX}
    ])

    total_rows = len(df_matrix)
    num_files = math.ceil(total_rows / ROWS_PER_FILE) if total_rows > 0 else 1

    try:
        for i in range(num_files):
            start_row = i * ROWS_PER_FILE
            end_row = start_row + ROWS_PER_FILE
            df_chunk = df_matrix.iloc[start_row:end_row]
            
            fname = OUTPUT_FILE.replace('.xlsx', f'_parte_{i+1}.xlsx') if num_files > 1 else OUTPUT_FILE
            print(f"      Salvando {fname}...")
            
            with pd.ExcelWriter(fname, engine='openpyxl') as writer:
                df_chunk.to_excel(writer, sheet_name='Matriz')
                if i == 0:
                    df_resumo.to_excel(writer, sheet_name='Resumo', index=False)
                    if not df_empty.empty: df_empty.to_excel(writer, sheet_name='Pastas Vazias', index=False)
        
        print(f"✅ Relatórios salvos!")
        
    except Exception as e:
        logging.error(f"Erro ao salvar Excel: {e}")
        print(f"❌ Erro ao salvar Excel: {e}")

    if "Sucesso" in status_msg:
        if os.path.exists(CHECKPOINT_STATS_FILE): os.remove(CHECKPOINT_STATS_FILE)
        if os.path.exists(CHECKPOINT_TOKEN_FILE): os.remove(CHECKPOINT_TOKEN_FILE)

def scan_bucket(bucket_name, root_prefix):
    if not bucket_name:
        print("ERRO CRÍTICO: BUCKET_NAME não definido no .env")
        return

    s3 = get_s3_client()
    
    # --- PASSO 1: MAPEAR PASTAS PRINCIPAIS ---
    # Isso substitui a varredura cega. Pegamos a lista de pastas primeiro.
    all_root_folders = get_immediate_subfolders(s3, bucket_name, root_prefix)
    
    # --- PASSO 2: FILTRAR A PASTA IGNORADA ---
    folders_to_scan = []
    for f in all_root_folders:
        # Se a pasta começa com o prefixo que queremos ignorar, pulamos ela da lista.
        # Assim, nenhuma requisição será feita para dentro dela.
        if IGNORED_PREFIX and f.startswith(IGNORED_PREFIX):
            print(f"🚫 Ignorando pasta inteira (Strategy Map & Attack): {f}")
            logging.info(f"Pasta removida da lista de varredura: {f}")
            continue
        folders_to_scan.append(f)
    
    # Se o bucket não tiver pastas na raiz (flat), adicionamos o próprio root para escanear
    if not folders_to_scan:
        folders_to_scan = [root_prefix]

    print(f"📋 Lista final de varredura: {len(folders_to_scan)} pastas principais.")

    # --- PASSO 3: PREPARAR VARIÁVEIS / CHECKPOINT ---
    # Carrega onde parou (agora incluindo o folder_idx)
    stats, paths, files_paths, total_start, start_token, start_folder_idx = load_checkpoint()
    
    if stats:
        folder_stats = stats
        all_known_paths = paths
        files_found_paths = files_paths
        total_files = total_start
        current_folder_idx = start_folder_idx
    else:
        folder_stats = defaultdict(create_nested_defaultdict)
        all_known_paths = set()
        files_found_paths = set()
        total_files = 0
        current_folder_idx = 0
        start_token = None # Garante que começa limpo se não tiver checkpoint

    status_final = "Concluído com Sucesso"
    requests_made = 0
    ignored_files_count = 0 # Contador para log de arquivos individuais com erro
    
    paginator = s3.get_paginator('list_objects_v2')

    # --- PASSO 4: LOOP DE "ATAQUE" (PASTA POR PASTA) ---
    try:
        # Iteramos sobre a lista filtrada, começando do índice salvo no checkpoint
        for i in range(current_folder_idx, len(folders_to_scan)):
            
            target = folders_to_scan[i]
            print(f"\n📂 [{i+1}/{len(folders_to_scan)}] Varrendo pasta: {target}")
            logging.info(f"Iniciando varredura da pasta: {target}")
            
            # Configuração da Paginação
            # Só usamos o start_token se estivermos na mesma pasta onde o checkpoint parou.
            # Se já mudamos de pasta, o token deve ser None (começar do inicio daquela pasta).
            pagination_config = {'PageSize': 1000}
            if start_token and i == current_folder_idx:
                pagination_config['StartingToken'] = start_token
                start_token = None # Reseta para as próximas pastas não usarem esse token antigo

            page_iterator = paginator.paginate(
                Bucket=bucket_name, 
                Prefix=target, 
                PaginationConfig=pagination_config
            )

            pages_since_save = 0
            
            # Loop interno percorre as páginas daquela pasta específica (com TQDM e Log)
            with tqdm(page_iterator, desc=f"Lendo {target[:20]}...") as pbar:
                for page in pbar:
                    requests_made += 1
                    pages_since_save += 1
                    
                    # --- AUTO-SAVE (CHECKPOINT) ---
                    # Salva referenciando a PASTA ATUAL (i)
                    if 'NextContinuationToken' in page and pages_since_save >= 500:
                        save_checkpoint(folder_stats, all_known_paths, files_found_paths, page['NextContinuationToken'], total_files, i)
                        pages_since_save = 0

                    # --- TRAVA DE SEGURANÇA ---
                    if MAX_REQUESTS_SAFETY > 0 and requests_made > MAX_REQUESTS_SAFETY:
                        status_final = f"Interrompido (Limite: {MAX_REQUESTS_SAFETY})"
                        logging.warning("Limite de segurança atingido.")
                        raise StopIteration("Limite Atingido") # Força saída dos loops

                    if 'Contents' not in page: continue

                    # Processamento dos arquivos
                    for obj in page['Contents']:
                        try:
                            # --- TRATAMENTO DE ERRO POR ARQUIVO ---
                            key = obj['Key']
                            last_modified = obj['LastModified']
                            
                            if key.endswith('/'):
                                all_known_paths.add(key)
                                continue

                            folder_path = os.path.dirname(key)
                            if not folder_path: folder_path = "Raiz"
                            files_found_paths.add(folder_path)
                            
                            # Hierarquia
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
                            # Se um arquivo falhar, loga e continua o próximo
                            ignored_files_count += 1
                            logging.error(f"Erro ao processar arquivo individual: {e_file}")
                            continue

                    pbar.set_postfix({'Total': total_files})
                    
                    # Guarda token atual para caso de crash/ctrl+c
                    current_next_token = page.get('NextContinuationToken', None)

            # --- FIM DA PASTA ---
            # Se terminou a pasta com sucesso, salvamos checkpoint apontando para a PRÓXIMA (i+1)
            # e com token None (começar do zero na próxima).
            save_checkpoint(folder_stats, all_known_paths, files_found_paths, None, total_files, i + 1)

    except (KeyboardInterrupt, StopIteration):
        print("\n⚠️ Parada solicitada (Ctrl+C ou Limite).")
        if status_final == "Concluído com Sucesso": status_final = "Cancelado pelo Usuário"
        
        # Salva exatamente onde parou
        if 'current_next_token' in locals() and current_next_token:
            # Parou no meio de uma pasta
            save_checkpoint(folder_stats, all_known_paths, files_found_paths, current_next_token, total_files, i) # Usa 'i' atual
        else:
            # Parou entre pastas
            save_checkpoint(folder_stats, all_known_paths, files_found_paths, None, total_files, i)

    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO: {e}")
        logging.critical(f"Erro fatal no loop principal: {e}", exc_info=True)
        status_final = f"Erro: {str(e)}"
        # Tenta salvar emergência
        if 'current_next_token' in locals() and current_next_token:
             save_checkpoint(folder_stats, all_known_paths, files_found_paths, current_next_token, total_files, i)

    finally:
        print(f"\nFinalizado. Total acumulado: {total_files}")
        logging.info(f"Varredura encerrada. Total: {total_files}. Erros Individuais: {ignored_files_count}")
        generate_excel_report(folder_stats, all_known_paths, files_found_paths, status_msg=status_final)

if __name__ == "__main__":
    scan_bucket(BUCKET_NAME, TARGET_FOLDER)