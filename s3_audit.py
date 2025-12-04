# ================= IMPORTS E BIBLIOTECAS =================

import boto3
import os
import pickle
from collections import defaultdict
from datetime import datetime, timedelta, timezone 
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
IGNORED_PREFIX = "000000000000010/000000000099999/" 

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
    Mapeia apenas as pastas imediatas usando Delimiter='/'.
    """
    search_prefix = prefix
    if search_prefix and not search_prefix.endswith('/'):
        search_prefix += '/'
        
    print(f"🗺️  Mapeando estrutura em '{search_prefix}'...")
    logging.info(f"Mapeando estrutura: {bucket}/{search_prefix}")
    
    folders = []
    paginator = s3_client.get_paginator('list_objects_v2')
    iterator = paginator.paginate(Bucket=bucket, Prefix=search_prefix, Delimiter='/')

    for page in iterator:
        if 'CommonPrefixes' in page:
            for p in page['CommonPrefixes']:
                folders.append(p['Prefix'])
    
    folders.sort()
    return folders

def save_checkpoint(folder_stats, recent_files_stats, extension_stats, all_known_paths, files_found_paths, next_token, total_files, current_folder_idx):
    """
    Salva o estado atual.
    ATUALIZADO: Agora salva 'extension_stats' (dicionário genérico de extensões).
    """
    logging.info(f"Salvando Checkpoint... (Total: {total_files}, Pasta Index: {current_folder_idx})")
    
    data_to_save = {
        'stats': folder_stats,
        'recent_stats': recent_files_stats,
        'ext_stats': extension_stats, # Novo campo genérico para todas extensões
        'all_paths': all_known_paths,
        'file_paths': files_found_paths,
        'total': total_files,
        'folder_idx': current_folder_idx 
    }
    
    try:
        with open(CHECKPOINT_STATS_FILE, 'wb') as f:
            pickle.dump(data_to_save, f)
        
        if next_token:
            with open(CHECKPOINT_TOKEN_FILE, 'w') as f:
                f.write(next_token)
        elif os.path.exists(CHECKPOINT_TOKEN_FILE):
             os.remove(CHECKPOINT_TOKEN_FILE)
             
    except Exception as e:
        logging.error(f"Erro ao salvar checkpoint: {e}")

def load_checkpoint():
    """
    Carrega o estado anterior.
    ATUALIZADO: Recupera 'extension_stats'.
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
            
            idx = data.get('folder_idx', 0)
            
            # Recupera os dados
            recent_stats = data.get('recent_stats', defaultdict(int))
            
            # Tenta recuperar ext_stats (novo), se não existir (checkpoint antigo), inicia vazio
            # Se vier de um checkpoint que tinha mp4/mkv hardcoded, eles serão ignorados em prol da nova lógica
            extension_stats = data.get('ext_stats', defaultdict(create_nested_defaultdict))
            
            print(f"   -> Retomando da pasta nº {idx+1}, Arquivos processados: {data['total']}")
            logging.info(f"Retomando varredura. Pasta Index: {idx}, Arquivos: {data['total']}")
            
            return data['stats'], recent_stats, extension_stats, data['all_paths'], data['file_paths'], data['total'], token, idx
        
        except Exception as e:
            msg = f"Erro ao ler checkpoint ({e}). Iniciando do zero."
            print(f"⚠️ {msg}")
            logging.warning(msg)
    
    # Retorna None se não houver checkpoint
    return None, None, None, None, None, 0, None, 0

def generate_excel_report(folder_stats, recent_files_stats, extension_stats, all_known_paths, files_found_paths, status_msg="Concluído com Sucesso"):
    """
    Gera o relatório em formato de MATRIZ.
    ATUALIZADO: Gera colunas dinâmicas para TODAS as extensões encontradas.
    """
    print(f"\n💾 Compilando dados para Excel...")
    logging.info("Iniciando compilação e geração dos arquivos Excel...")
    
    # 1. NORMALIZAÇÃO DOS DADOS PRINCIPAIS (ANO)
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

    # 2. CRIAÇÃO DA MATRIZ PRINCIPAL (ANOS)
    df = pd.DataFrame(data_rows)
    
    if not df.empty:
        df_matrix = df.pivot_table(index='Pasta', columns='Ano', values='Arquivos', fill_value=0)
        df_matrix['Total Geral'] = df_matrix.sum(axis=1)
    else:
        df_matrix = pd.DataFrame(columns=["Pasta", "Total Geral"])

    # 3. COLUNA DE RECENTES
    normalized_recent = defaultdict(int)
    for r_folder, r_count in recent_files_stats.items():
        clean_r = r_folder.rstrip('/')
        if not clean_r: clean_r = "Raiz"
        normalized_recent[clean_r] += r_count
    
    df_matrix['Recentes (90 dias)'] = df_matrix.index.map(normalized_recent).fillna(0).astype(int)

    # 4. COLUNAS DINÂMICAS DE EXTENSÕES
    # Transforma o dicionário extension_stats em um DataFrame e depois faz Pivot
    ext_rows = []
    
    # Normaliza as chaves das extensões
    normalized_exts = defaultdict(create_nested_defaultdict)
    for raw_folder, exts in extension_stats.items():
        clean_folder = raw_folder.rstrip('/')
        if not clean_folder: clean_folder = "Raiz"
        for ext, count in exts.items():
            normalized_exts[clean_folder][ext] += count

    for folder, ext_data in normalized_exts.items():
        for ext, count in ext_data.items():
            ext_rows.append({'Pasta': folder, 'Ext': ext, 'Qtd': count})
    
    if ext_rows:
        df_ext = pd.DataFrame(ext_rows)
        # Cria matriz de extensões: Linha=Pasta, Coluna=Extensão, Valor=Qtd
        df_ext_pivot = df_ext.pivot_table(index='Pasta', columns='Ext', values='Qtd', fill_value=0)
        
        # Junta a matriz de Anos com a matriz de Extensões
        # O join é feito pelo Index (Nome da Pasta)
        df_matrix = df_matrix.join(df_ext_pivot, how='outer').fillna(0)
        
        # Converte tudo para inteiro para ficar bonito no Excel
        # (O join pode introduzir floats devido aos NaNs)
        df_matrix = df_matrix.astype(int)

    # REORDENAÇÃO FINAL DE COLUNAS
    # Queremos: [Anos...] -> [Total Geral] -> [Recentes] -> [Extensões A-Z...]
    all_cols = list(df_matrix.columns)
    
    # Identifica colunas de anos (são números)
    year_cols = [c for c in all_cols if isinstance(c, int) or (isinstance(c, str) and c.isdigit())]
    year_cols.sort()
    
    # Identifica colunas fixas
    fixed_cols = ['Total Geral', 'Recentes (90 dias)']
    
    # Identifica colunas de extensão (tudo que sobrar)
    ext_cols = [c for c in all_cols if c not in year_cols and c not in fixed_cols]
    ext_cols.sort() # Ordena alfabeticamente (.avi, .mkv, .mp4, .txt...)
    
    final_order = year_cols + fixed_cols + ext_cols
    # Reconstrói apenas com as colunas que realmente existem
    final_order = [c for c in final_order if c in df_matrix.columns]
    
    df_matrix = df_matrix[final_order]
    df_matrix = df_matrix.sort_index()

    # 5. TRATAMENTO DE PASTAS VAZIAS
    all_clean_paths = {p.rstrip('/') for p in all_known_paths if p.rstrip('/')}
    stats_clean_paths = set(normalized_stats.keys())
    empty_folders = sorted(list(all_clean_paths - stats_clean_paths))
    
    df_empty = pd.DataFrame()
    if empty_folders:
        df_empty = pd.DataFrame({'Pasta': empty_folders, 'Status': 'Vazia ou Apenas Subpastas'})

    # Resumo
    df_resumo = pd.DataFrame([
        {"Item": "Status da Execução", "Valor": status_msg},
        {"Item": "Data do Relatório", "Valor": datetime.now().strftime('%d/%m/%Y %H:%M:%S')},
        {"Item": "Total Arquivos", "Valor": df_matrix['Total Geral'].sum() if not df_matrix.empty else 0},
        {"Item": "Arquivos Recentes (90d)", "Valor": df_matrix['Recentes (90 dias)'].sum() if 'Recentes (90 dias)' in df_matrix else 0},
        {"Item": "Bucket", "Valor": BUCKET_NAME},
        {"Item": "Pasta Ignorada", "Valor": IGNORED_PREFIX}
    ])

    # 6. SALVAMENTO COM DIVISÃO (SPLIT)
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
                fname = OUTPUT_FILE.replace('.xlsx', f'_parte_{i+1}.xlsx') 
            else:
                fname = OUTPUT_FILE
                
            print(f"      Salvando {fname} (Linhas {start_row}-{min(end_row, total_rows)})...")
            
            with pd.ExcelWriter(fname, engine='openpyxl') as writer:
                df_chunk.to_excel(writer, sheet_name='Matriz de Arquivos')
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
    
    # Data de Corte
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
    print(f"📅 Data de corte para arquivos recentes: {cutoff_date.strftime('%d/%m/%Y')}")
    
    # --- PASSO 1: Mapeamento Inteligente ---
    print("🔍 Analisando estrutura de pastas...")
    root_folders = get_immediate_subfolders(s3, bucket_name, root_prefix)
    
    folders_to_scan = []
    
    for folder in root_folders:
        if folder == IGNORED_PREFIX:
            print(f"🚫 Ignorando pasta exata: {folder}")
            logging.info(f"Pasta ignorada (Exata): {folder}")
            continue
        elif IGNORED_PREFIX.startswith(folder):
            print(f"⚠️  Pasta '{folder}' contém o alvo a ser ignorado. Perfurando (Drill Down)...")
            logging.info(f"Realizando Drill Down em {folder}")
            subfolders = get_immediate_subfolders(s3, bucket_name, folder)
            for sub in subfolders:
                if sub == IGNORED_PREFIX or sub.startswith(IGNORED_PREFIX):
                    print(f"   🚫 Ignorando subpasta: {sub}")
                    logging.info(f"Subpasta ignorada: {sub}")
                    continue
                else:
                    folders_to_scan.append(sub)
        else:
            folders_to_scan.append(folder)
            
    if not folders_to_scan and not root_folders:
        folders_to_scan = [root_prefix]

    print(f"📋 Lista final de varredura: {len(folders_to_scan)} caminhos.")
    logging.info(f"Lista de ataque finalizada. Total: {len(folders_to_scan)}")

    # --- PASSO 2: Preparar Variáveis / Checkpoint ---
    # Carrega dicionários antigos e novos
    stats, recent_stats, ext_stats, paths, files_paths, total_start, start_token, start_folder_idx = load_checkpoint()
    
    if stats:
        folder_stats = stats
        recent_files_stats = recent_stats
        extension_stats = ext_stats # Carrega as estatísticas de extensões
        all_known_paths = paths
        files_found_paths = files_paths
        total_files = total_start
        current_folder_idx = start_folder_idx
    else:
        folder_stats = defaultdict(create_nested_defaultdict)
        recent_files_stats = defaultdict(int)
        extension_stats = defaultdict(create_nested_defaultdict) # Inicializa dicionário de extensões
        all_known_paths = set()
        files_found_paths = set()
        total_files = 0
        current_folder_idx = 0
        start_token = None 

    status_final = "Concluído com Sucesso"
    requests_made = 0
    ignored_files_count = 0
    
    paginator = s3.get_paginator('list_objects_v2')

    # --- PASSO 3: Loop de Ataque ---
    try:
        for i in range(current_folder_idx, len(folders_to_scan)):
            
            target = folders_to_scan[i]
            print(f"\n📂 [{i+1}/{len(folders_to_scan)}] Varrendo: {target}")
            logging.info(f"Iniciando varredura da pasta: {target}")
            
            pagination_config = {'PageSize': 1000}
            if start_token and i == current_folder_idx:
                pagination_config['StartingToken'] = start_token
                start_token = None

            page_iterator = paginator.paginate(
                Bucket=bucket_name, 
                Prefix=target, 
                PaginationConfig=pagination_config
            )

            pages_since_save = 0
            
            with tqdm(page_iterator, desc=f"Lendo {target[:20]}...") as pbar:
                for page in pbar:
                    requests_made += 1
                    pages_since_save += 1
                    
                    if 'NextContinuationToken' in page and pages_since_save >= 500:
                        save_checkpoint(folder_stats, recent_files_stats, extension_stats, all_known_paths, files_found_paths, page['NextContinuationToken'], total_files, i)
                        pages_since_save = 0

                    if MAX_REQUESTS_SAFETY > 0 and requests_made > MAX_REQUESTS_SAFETY:
                        status_final = f"Interrompido (Limite: {MAX_REQUESTS_SAFETY})"
                        logging.warning("Limite de segurança atingido.")
                        raise StopIteration("Limite Atingido")

                    if 'Contents' not in page: continue

                    for obj in page['Contents']:
                        try:
                            key = obj['Key']
                            if key.startswith(IGNORED_PREFIX):
                                ignored_files_count += 1
                                continue

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

                            # --- ESTATÍSTICAS ---
                            # 1. Ano
                            year = last_modified.year
                            folder_stats[folder_path][year] += 1
                            
                            # 2. Recentes
                            if last_modified >= cutoff_date:
                                recent_files_stats[folder_path] += 1
                            
                            # 3. Tipos de Arquivo (GENÉRICO PARA TODOS)
                            # Pega a extensão (ex: .mp4, .txt, .pdf)
                            _, ext = os.path.splitext(key)
                            if ext:
                                ext = ext.lower() # Padroniza para minúsculo
                                extension_stats[folder_path][ext] += 1
                            else:
                                extension_stats[folder_path]['(sem extensão)'] += 1
                            # -------------------

                            total_files += 1

                        except Exception as e_file:
                            ignored_files_count += 1
                            logging.error(f"Erro arq individual: {e_file}")
                            continue

                    pbar.set_postfix({'Total': total_files})
                    current_next_token = page.get('NextContinuationToken', None)

            save_checkpoint(folder_stats, recent_files_stats, extension_stats, all_known_paths, files_found_paths, None, total_files, i + 1)

    except (KeyboardInterrupt, StopIteration):
        print("\n⚠️ Parada solicitada.")
        if status_final == "Concluído com Sucesso": status_final = "Cancelado pelo Usuário"
        
        if 'current_next_token' in locals() and current_next_token:
            save_checkpoint(folder_stats, recent_files_stats, extension_stats, all_known_paths, files_found_paths, current_next_token, total_files, i)
        else:
            save_checkpoint(folder_stats, recent_files_stats, extension_stats, all_known_paths, files_found_paths, None, total_files, i)

    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO: {e}")
        logging.critical(f"Erro fatal: {e}", exc_info=True)
        status_final = f"Erro: {str(e)}"
        if 'current_next_token' in locals() and current_next_token:
             save_checkpoint(folder_stats, recent_files_stats, extension_stats, all_known_paths, files_found_paths, current_next_token, total_files, i)

    finally:
        print(f"\nFinalizado. Total acumulado: {total_files}")
        logging.info(f"Fim. Total: {total_files}. Ignorados/Erros: {ignored_files_count}")
        generate_excel_report(folder_stats, recent_files_stats, extension_stats, all_known_paths, files_found_paths, status_msg=status_final)

if __name__ == "__main__":
    scan_bucket(BUCKET_NAME, TARGET_FOLDER)