# ================= IMPORTS E BIBLIOTECAS =================

# 'boto3': O SDK oficial da AWS. É a ponte entre o Python e a nuvem.
import boto3

# 'os': Permite interagir com o sistema operacional e arquivos.
import os

# 'pickle': Biblioteca nativa do Python para serializar objetos.
# Ela permite salvar dicionários complexos da memória para um arquivo no disco
# e carregá-los de volta exatamente como estavam. Essencial para o Checkpoint.
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

# 'math': Usado para calcular quantos arquivos (partes) serão necessários (arredondamento para cima).
import math

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
CHECKPOINT_STATS_FILE = 'checkpoint_stats.pkl' # Arquivo binário com a contagem atual
CHECKPOINT_TOKEN_FILE = 'checkpoint_token.txt' # Arquivo texto com o "marcador" da AWS

# --- NOVO: CONFIGURAÇÃO DE DIVISÃO DE ARQUIVOS ---
# Define quantas linhas de dados (pastas) cada arquivo Excel terá no máximo.
# Se passar disso, o script cria automaticamente "parte_1", "parte_2", etc.
ROWS_PER_FILE = 20000

# ================= FUNÇÕES DO SISTEMA =================

def get_s3_client():
    """
    Cria e retorna o cliente de conexão com o S3.
    """
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    if not aws_access_key or not aws_secret_key:
        raise ValueError("ERRO: Credenciais AWS não encontradas. Verifique seu arquivo .env")

    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

def save_checkpoint(folder_stats, all_known_paths, files_found_paths, next_token, total_files):
    """
    Salva o 'estado' atual do script no disco.
    Isso permite que, se o computador desligar, possamos voltar exatamente deste ponto.
    """
    # 1. Empacota os dados da memória num dicionário
    data_to_save = {
        'stats': folder_stats,
        'all_paths': all_known_paths,
        'file_paths': files_found_paths,
        'total': total_files
    }
    
    # 2. Salva os dados estatísticos usando Pickle (modo 'wb' = write binary)
    with open(CHECKPOINT_STATS_FILE, 'wb') as f:
        pickle.dump(data_to_save, f)
    
    # 3. Salva o Token da AWS num arquivo texto simples
    # O Token é a "chave" que diz pra AWS: "Comece a listar a partir DO ARQUIVO X"
    if next_token:
        with open(CHECKPOINT_TOKEN_FILE, 'w') as f:
            f.write(next_token)

def load_checkpoint():
    """
    Verifica se existem arquivos de checkpoint salvos e tenta carregá-los.
    Retorna os dados recuperados ou valores vazios se for a primeira vez.
    """
    if os.path.exists(CHECKPOINT_STATS_FILE) and os.path.exists(CHECKPOINT_TOKEN_FILE):
        print("\n🔄 CHECKPOINT ENCONTRADO! Carregando estado anterior...")
        try:
            # Carrega o dicionário de estatísticas
            with open(CHECKPOINT_STATS_FILE, 'rb') as f:
                data = pickle.load(f)
            
            # Carrega o Token da AWS
            with open(CHECKPOINT_TOKEN_FILE, 'r') as f:
                token = f.read().strip()
                
            print(f"   -> Retomando de {data['total']} arquivos já processados.")
            return data['stats'], data['all_paths'], data['file_paths'], data['total'], token
        
        except Exception as e:
            print(f"⚠️ Erro ao ler checkpoint ({e}). O arquivo pode estar corrompido. Começando do zero.")
    
    # Se não houver checkpoint, retorna tudo vazio/zero
    return None, None, None, 0, None

def generate_excel_report(folder_stats, all_known_paths, files_found_paths, status_msg="Concluído com Sucesso"):
    """
    Gera o relatório em formato de MATRIZ (Tabela Dinâmica).
    --- ATUALIZAÇÃO: DIVISÃO DE ARQUIVOS ---
    Se o número de linhas exceder ROWS_PER_FILE, divide em múltiplos arquivos Excel.
    """
    print(f"\n💾 Compilando dados para Excel...")
    
    # 1. PREPARAÇÃO E NORMALIZAÇÃO
    data_rows = []
    
    # Normaliza chaves para garantir unicidade e evitar o bug de linhas duplicadas.
    normalized_stats = defaultdict(lambda: defaultdict(int))
    
    for raw_folder, years in folder_stats.items():
        clean_folder = raw_folder.rstrip('/') # Remove barra final
        if not clean_folder: clean_folder = "Raiz"
        
        # Agrupa os dados na chave limpa
        for year, count in years.items():
            normalized_stats[clean_folder][year] += count

    # Prepara a lista de dados apenas para pastas que TÊM arquivos
    for folder, years_data in normalized_stats.items():
        for year, count in years_data.items():
            data_rows.append({
                'Pasta': folder,
                'Ano': year,
                'Arquivos': count
            })

    # 2. CRIAÇÃO DA MATRIZ (PIVOT TABLE)
    df = pd.DataFrame(data_rows)
    df_matrix = pd.DataFrame()

    if not df.empty:
        # A MÁGICA: Pivot Table transforma linhas (anos) em colunas.
        df_matrix = df.pivot_table(index='Pasta', columns='Ano', values='Arquivos', fill_value=0)
        
        # Adiciona coluna de Total Geral por pasta
        df_matrix['Total Geral'] = df_matrix.sum(axis=1)
        
        # Ordena alfabeticamente pelo nome da pasta
        df_matrix = df_matrix.sort_index()
    else:
        df_matrix = pd.DataFrame(columns=["Pasta", "Total Geral"])

    # 3. TRATAMENTO DE PASTAS VAZIAS (ESTRUTURAIS)
    all_clean_paths = {p.rstrip('/') for p in all_known_paths if p.rstrip('/')}
    stats_clean_paths = set(normalized_stats.keys())
    empty_folders = sorted(list(all_clean_paths - stats_clean_paths))
    
    df_empty = pd.DataFrame()
    if empty_folders:
        df_empty = pd.DataFrame({'Pasta': empty_folders})
        df_empty['Status'] = 'Vazia ou Apenas Subpastas'

    # Cria tabela de Resumo
    df_resumo = pd.DataFrame([
        {"Item": "Status da Execução", "Valor": status_msg},
        {"Item": "Data do Relatório", "Valor": datetime.now().strftime('%d/%m/%Y %H:%M:%S')},
        {"Item": "Total de Arquivos Listados", "Valor": df_matrix['Total Geral'].sum() if not df_matrix.empty else 0},
        {"Item": "Bucket", "Valor": BUCKET_NAME}
    ])

    # 4. LÓGICA DE DIVISÃO (SPLIT) E SALVAMENTO
    total_rows = len(df_matrix)
    
    # Calcula quantos arquivos serão necessários (ex: 25000 linhas / 10000 limit = 3 arquivos)
    num_files = math.ceil(total_rows / ROWS_PER_FILE) if total_rows > 0 else 1

    print(f"   -> Total de linhas na matriz: {total_rows}")
    if num_files > 1:
        print(f"   -> O arquivo será dividido em {num_files} partes (limite: {ROWS_PER_FILE} linhas/arquivo).")

    try:
        for i in range(num_files):
            # Define o início e fim do pedaço (slice) atual
            start_row = i * ROWS_PER_FILE
            end_row = start_row + ROWS_PER_FILE
            
            # Cria o pedaço do DataFrame
            df_chunk = df_matrix.iloc[start_row:end_row]
            
            # Define o nome do arquivo
            if num_files > 1:
                # Se tiver partes: relatorio_parte_1.xlsx, relatorio_parte_2.xlsx
                current_filename = OUTPUT_FILE.replace('.xlsx', f'_parte_{i+1}.xlsx')
            else:
                # Se couber tudo num só, usa o nome original
                current_filename = OUTPUT_FILE

            print(f"      Salvando {current_filename} (Linhas {start_row} a {min(end_row, total_rows)})...")

            with pd.ExcelWriter(current_filename, engine='openpyxl') as writer:
                # Aba Principal (O Pedaço da Matriz)
                df_chunk.to_excel(writer, sheet_name='Matriz de Arquivos')
                
                # Para não ficar repetitivo, salvamos o Resumo e as Pastas Vazias APENAS no arquivo da Parte 1
                # ou no arquivo único se não houver divisão.
                if i == 0:
                    df_resumo.to_excel(writer, sheet_name='Resumo', index=False)
                    if not df_empty.empty:
                        df_empty.to_excel(writer, sheet_name='Pastas Vazias', index=False)
        
        print(f"✅ Todos os relatórios Excel salvos com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao salvar Excel: {e}")
        print("Verifique se o arquivo não está aberto.")

    # LIMPEZA: Se o script terminou com SUCESSO total, deletamos os checkpoints
    if "Sucesso" in status_msg:
        if os.path.exists(CHECKPOINT_STATS_FILE): os.remove(CHECKPOINT_STATS_FILE)
        if os.path.exists(CHECKPOINT_TOKEN_FILE): os.remove(CHECKPOINT_TOKEN_FILE)

def scan_bucket(bucket_name, prefix_folder):
    """
    Varre o bucket com suporte a Checkpoint, Contador Real e Proteção de Erros.
    """
    if not bucket_name:
        print("ERRO CRÍTICO: BUCKET_NAME não definido no arquivo .env")
        return

    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    
    # 1. TENTA CARREGAR O CHECKPOINT
    stats, paths, files_paths, total_start, start_token = load_checkpoint()
    
    # Se carregou algo, usamos. Se não, iniciamos variáveis vazias.
    if stats:
        folder_stats = stats
        all_known_paths = paths
        files_found_paths = files_paths
        total_files = total_start
    else:
        folder_stats = defaultdict(lambda: defaultdict(int))
        all_known_paths = set()
        files_found_paths = set()
        total_files = 0

    start_msg = f"bucket '{bucket_name}'"
    if prefix_folder: start_msg += f" na pasta '{prefix_folder}'"
    
    print(f"Iniciando varredura em: {start_msg}...")

    if MAX_REQUESTS_SAFETY > 0:
        print(f"⚠️  MODO SEGURO ATIVO: Limite de {MAX_REQUESTS_SAFETY} requisições.")

    # Configuração da Paginação
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

    # --- BLOCO TRY/EXCEPT PRINCIPAL ---
    try:
        # Usamos o 'with tqdm...' para ter controle manual da barra (atualizar texto ao lado)
        with tqdm(page_iterator, desc="Lendo AWS") as pbar:
            
            for page in pbar:
                requests_made += 1
                pages_since_checkpoint += 1
                
                # --- AUTO-SAVE (CHECKPOINT) ---
                if 'NextContinuationToken' in page and pages_since_checkpoint >= 500:
                    save_checkpoint(folder_stats, all_known_paths, files_found_paths, page['NextContinuationToken'], total_files)
                    pages_since_checkpoint = 0 # Reseta contador

                # --- TRAVA DE SEGURANÇA ---
                if MAX_REQUESTS_SAFETY > 0 and requests_made > MAX_REQUESTS_SAFETY:
                    status_final = f"Interrompido (Limite: {MAX_REQUESTS_SAFETY})"
                    break 

                if 'Contents' not in page:
                    continue

                # Processamento dos arquivos
                for obj in page['Contents']:
                    key = obj['Key']
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
                
                # --- ATUALIZAÇÃO VISUAL ---
                pbar.set_postfix({'Arquivos': total_files})

                current_next_token = page.get('NextContinuationToken', None)
                
                if MAX_REQUESTS_SAFETY > 0 and requests_made > MAX_REQUESTS_SAFETY:
                    break

    except KeyboardInterrupt:
        print("\n\n⚠️  USUÁRIO INTERROMPEU (Ctrl+C). Salvando estado...")
        status_final = "Cancelado pelo Usuário"
        if 'current_next_token' in locals() and current_next_token:
            save_checkpoint(folder_stats, all_known_paths, files_found_paths, current_next_token, total_files)
    
    except Exception as e:
        print(f"\n\n❌ ERRO INESPERADO: {e}")
        status_final = f"Erro Crítico: {str(e)}"
        if 'current_next_token' in locals() and current_next_token:
            save_checkpoint(folder_stats, all_known_paths, files_found_paths, current_next_token, total_files)

    finally:
        print(f"\nProcesso finalizado. Total acumulado: {total_files} arquivos.")
        generate_excel_report(folder_stats, all_known_paths, files_found_paths, status_msg=status_final)

if __name__ == "__main__":
    scan_bucket(BUCKET_NAME, TARGET_FOLDER)