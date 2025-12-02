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
OUTPUT_FILE = 'relatorio_s3.md'
CHECKPOINT_STATS_FILE = 'checkpoint_stats.pkl' # Arquivo binário com a contagem atual
CHECKPOINT_TOKEN_FILE = 'checkpoint_token.txt' # Arquivo texto com o "marcador" da AWS

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

def generate_markdown_report(folder_stats, all_known_paths, files_found_paths, status_msg="Concluído com Sucesso"):
    """
    Gera o arquivo físico (.md) com os dados organizados.
    """
    print(f"\n💾 Salvando relatório em {OUTPUT_FILE}...")
    
    sorted_folders = sorted(list(all_known_paths | files_found_paths))
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(f"# Relatório de Auditoria S3\n")
        f.write(f"**Status:** {status_msg}\n")
        f.write(f"**Data:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write(f"**Bucket:** `{BUCKET_NAME}`\n")
        
        filter_used = TARGET_FOLDER if TARGET_FOLDER else "(Raiz Total)"
        f.write(f"**Filtro (Prefix):** `{filter_used}`\n")

        if MAX_REQUESTS_SAFETY > 0:
             f.write(f"**⚠️ Limite de Segurança:** Ativo ({MAX_REQUESTS_SAFETY} requisições máx)\n")

        f.write("---\n\n")

        for folder in sorted_folders:
            search_key = folder.rstrip('/')
            if search_key == "": search_key = "Raiz"
            
            f.write(f"### 📂 `{search_key}`\n")

            if search_key in folder_stats:
                years_data = folder_stats[search_key]
                sorted_years = sorted(years_data.keys())
                
                f.write("| Ano | Qtd. Arquivos |\n")
                f.write("| :--- | :--- |\n")
                
                total_local = 0
                for year in sorted_years:
                    count = years_data[year]
                    total_local += count
                    f.write(f"| {year} | {count} |\n")
                
                f.write(f"\n**Total nesta pasta:** {total_local} arquivos\n")
            
            elif folder in all_known_paths and search_key not in files_found_paths:
                 f.write("> *ℹ️ Esta pasta contém apenas subpastas.*\n")
            
            else:
                 f.write("> *Pasta vazia.*\n")

            f.write("\n---\n")

    print("✅ Relatório salvo!")
    
    # LIMPEZA: Se o script terminou com SUCESSO total, deletamos os checkpoints
    # para que na próxima execução ele comece do zero limpo.
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
    # Se tivermos um start_token (do checkpoint), passamos ele para a AWS.
    # A AWS vai pular tudo que já foi lido anteriormente.
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
    # Protege a execução. Qualquer erro aqui dentro aciona o salvamento de emergência.
    try:
        # Usamos o 'with tqdm...' para ter controle manual da barra (atualizar texto ao lado)
        with tqdm(page_iterator, desc="Lendo AWS") as pbar:
            
            for page in pbar:
                requests_made += 1
                pages_since_checkpoint += 1
                
                # --- AUTO-SAVE (CHECKPOINT) ---
                # A cada 500 páginas (500k arquivos), salvamos o progresso.
                # Isso garante que se der erro, perdemos no máximo os últimos minutos.
                if 'NextContinuationToken' in page and pages_since_checkpoint >= 500:
                    save_checkpoint(folder_stats, all_known_paths, files_found_paths, page['NextContinuationToken'], total_files)
                    pages_since_checkpoint = 0 # Reseta contador

                # --- TRAVA DE SEGURANÇA ---
                if MAX_REQUESTS_SAFETY > 0 and requests_made > MAX_REQUESTS_SAFETY:
                    status_final = f"Interrompido (Limite: {MAX_REQUESTS_SAFETY})"
                    # Quebra o loop for
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
                # Atualiza o texto ao lado da barra com o número real de arquivos
                pbar.set_postfix({'Arquivos': total_files})

                # Guarda o token atual para caso precisemos salvar no 'Except' ou 'Break'
                current_next_token = page.get('NextContinuationToken', None)
                
                # Se quebrou por segurança acima, precisamos sair do loop do tqdm também
                if MAX_REQUESTS_SAFETY > 0 and requests_made > MAX_REQUESTS_SAFETY:
                    break

    except KeyboardInterrupt:
        # Captura o Ctrl+C do usuário
        print("\n\n⚠️  USUÁRIO INTERROMPEU (Ctrl+C). Salvando estado...")
        status_final = "Cancelado pelo Usuário"
        # Salva o checkpoint imediatamente com o último token conhecido
        if 'current_next_token' in locals() and current_next_token:
            save_checkpoint(folder_stats, all_known_paths, files_found_paths, current_next_token, total_files)
    
    except Exception as e:
        # Captura erros de internet, memória, etc.
        print(f"\n\n❌ ERRO INESPERADO: {e}")
        status_final = f"Erro Crítico: {str(e)}"
        if 'current_next_token' in locals() and current_next_token:
            save_checkpoint(folder_stats, all_known_paths, files_found_paths, current_next_token, total_files)

    finally:
        # Este bloco SEMPRE roda no final, dando erro ou sucesso.
        print(f"\nProcesso finalizado. Total acumulado: {total_files} arquivos.")
        generate_markdown_report(folder_stats, all_known_paths, files_found_paths, status_msg=status_final)

if __name__ == "__main__":
    scan_bucket(BUCKET_NAME, TARGET_FOLDER)