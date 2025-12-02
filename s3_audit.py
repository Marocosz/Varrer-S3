# ================= IMPORTS E BIBLIOTECAS =================

# 'boto3': O SDK oficial da AWS. É a ponte entre o Python e a nuvem.
import boto3

# 'os': Permite interagir com o sistema operacional (ler variáveis de ambiente, manipular caminhos).
import os

# 'defaultdict': Uma versão inteligente de dicionário que cria valores padrão se a chave não existir.
# Útil para não precisarmos ficar verificando "se a chave existe" antes de somar +1.
from collections import defaultdict

# 'datetime': Usado para pegar a data/hora atual para colocar no cabeçalho do relatório.
from datetime import datetime

# 'tqdm': Cria aquela barra de progresso visual no terminal para você saber que o script não travou.
from tqdm import tqdm

# 'dotenv': Biblioteca externa que lê o arquivo '.env' e carrega as variáveis para o sistema.
from dotenv import load_dotenv

# ================= CARREGAMENTO DE AMBIENTE =================

# Esta linha procura um arquivo chamado '.env' na mesma pasta do script.
# Ela lê o conteúdo e coloca na memória como se fossem variáveis do sistema.
# Isso garante segurança: suas senhas não ficam escritas no código.
load_dotenv()

# Pegamos o nome do bucket. Se não estiver no .env, retornará None.
BUCKET_NAME = os.getenv('BUCKET_NAME')

# --- NOVO ---
# Carrega a pasta alvo definida no .env.
# O segundo parâmetro ('') é um valor padrão: se a variável TARGET_FOLDER não existir no .env,
# assumimos que é uma string vazia. Na AWS, string vazia no prefixo significa "Bucket Inteiro".
TARGET_FOLDER = os.getenv('TARGET_FOLDER', '')

OUTPUT_FILE = 'relatorio_s3.md'

# ================= FUNÇÕES DO SISTEMA =================

def get_s3_client():
    """
    Cria e retorna o cliente de conexão com o S3.
    """
    # Buscamos as credenciais carregadas do arquivo .env
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    # Validação de Segurança: Se as chaves não existirem, paramos o script agora.
    if not aws_access_key or not aws_secret_key:
        raise ValueError("ERRO: Credenciais AWS não encontradas. Verifique seu arquivo .env")

    # Criamos o cliente 's3'. 
    # Diferente de usar 'Session', aqui passamos as chaves explicitamente.
    # Isso garante que o boto3 use O QUE ESTÁ NO .ENV, ignorando qualquer configuração global do PC.
    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

def scan_bucket(bucket_name, prefix_folder):
    """
    Varre o bucket filtrando por uma pasta específica (Prefix).
    """
    
    # Validação inicial
    if not bucket_name:
        print("ERRO CRÍTICO: BUCKET_NAME não definido no arquivo .env")
        return {}, set(), set(), 0

    # Obtém a conexão autenticada
    s3 = get_s3_client()
    
    # --- CONCEITO IMPORTANTE: PAGINAÇÃO ---
    # O endpoint 'list_objects_v2' da AWS retorna no máximo 1.000 arquivos por vez.
    # O 'paginator' automatiza o processo de pedir a página 1, depois a 2, depois a 3...
    paginator = s3.get_paginator('list_objects_v2')
    
    # Estrutura de dados para contagem:
    # { 'caminho/da/pasta': { 2021: 10 arquivos, 2022: 5 arquivos } }
    folder_stats = defaultdict(lambda: defaultdict(int))
    
    # Sets (conjuntos) para armazenar caminhos únicos e evitar duplicatas
    all_known_paths = set()    # Guarda TUDO que parece ser uma pasta
    files_found_paths = set()  # Guarda apenas pastas que TÊM arquivos dentro

    # Lógica apenas para mostrar uma mensagem bonita no terminal
    start_msg = f"bucket '{bucket_name}'"
    if prefix_folder:
        start_msg += f" na pasta '{prefix_folder}'"
    else:
        start_msg += " (RAIZ TOTAL)"

    print(f"Iniciando conexão com a AWS e varredura em: {start_msg}...")
    
    # --- PONTO CRÍTICO: FILTRAGEM POR PREFIXO ---
    # Aqui passamos o argumento 'Prefix'. Isso é crucial para performance.
    # Ao passar o prefixo, a filtragem acontece NOS SERVIDORES DA AWS.
    # O seu script nem chega a receber informações de arquivos fora dessa pasta.
    # Isso economiza banda de internet, processamento local e tempo.
    page_iterator = paginator.paginate(
        Bucket=bucket_name, 
        Prefix=prefix_folder  # Se for vazio, traz tudo. Se tiver texto, filtra.
    )
    
    total_files = 0

    # Loop principal: Itera sobre cada página de 1000 objetos retornada pela AWS
    for page in tqdm(page_iterator, desc="Processando objetos"):
        
        # Se o bucket estiver vazio ou a página não tiver conteúdo, pulamos.
        if 'Contents' not in page:
            continue

        # Loop interno: Itera sobre cada arquivo dentro da página atual
        for obj in page['Contents']:
            key = obj['Key']             # O caminho completo (ex: "planilhas/2023/jan.xlsx")
            last_modified = obj['LastModified'] # Data da última edição
            
            # --- TRATAMENTO DE PASTAS VIRTUAIS ---
            # O S3 não tem pastas reais. Às vezes, softwares de FTP criam objetos vazios terminados em '/'
            # para simular uma pasta. Se encontrarmos um desses, guardamos o caminho e pulamos.
            if key.endswith('/'):
                all_known_paths.add(key)
                continue

            # --- EXTRAÇÃO DE DIRETÓRIO ---
            # 'os.path.dirname' pega "a/b/c.txt" e retorna "a/b"
            folder_path = os.path.dirname(key)
            
            # Se o arquivo estiver na raiz do bucket, o dirname volta vazio. Chamamos de "Raiz".
            if not folder_path:
                folder_path = "Raiz"
            
            # Marcamos: "Esta pasta contém arquivos reais"
            files_found_paths.add(folder_path)
            
            # --- RECONSTRUÇÃO DE HIERARQUIA ---
            # Se temos o arquivo em "a/b/c", precisamos garantir que o relatório saiba
            # que a pasta "a" existe e a pasta "a/b" existe, mesmo que não tenham arquivos diretos.
            parts = folder_path.split('/')
            current_build = ""
            for part in parts:
                if part == "Raiz": continue
                # Reconstrói o caminho passo a passo: "a/", depois "a/b/"
                current_build = f"{current_build}{part}/" if current_build else f"{part}/"
                all_known_paths.add(current_build)

            # --- ESTATÍSTICA ---
            # Extrai apenas o ano (ex: 2022) do objeto datetime
            year = last_modified.year
            
            # Soma +1 na contagem daquela pasta, naquele ano
            folder_stats[folder_path][year] += 1
            total_files += 1

    return folder_stats, all_known_paths, files_found_paths, total_files

def generate_markdown_report(folder_stats, all_known_paths, files_found_paths):
    """
    Gera o arquivo físico (.md) com os dados organizados.
    """
    print(f"\nEscrevendo relatório em {OUTPUT_FILE}...")
    
    # Une os dois conjuntos de caminhos e ordena alfabeticamente
    sorted_folders = sorted(list(all_known_paths | files_found_paths))
    
    # Abre o arquivo para escrita ('w'). O encoding='utf-8' é vital para não quebrar acentos.
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        
        # Cabeçalho do Markdown
        f.write(f"# Relatório de Auditoria S3\n")
        f.write(f"**Data:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write(f"**Bucket:** `{BUCKET_NAME}`\n")
        
        # Mostra qual filtro foi usado no relatório final para evitar confusão
        filter_used = TARGET_FOLDER if TARGET_FOLDER else "(Raiz Total)"
        f.write(f"**Filtro (Prefix):** `{filter_used}`\n")
        f.write("---\n\n")

        # Itera sobre cada pasta identificada
        for folder in sorted_folders:
            # Limpeza visual: remove a barra final para o título ficar bonito
            search_key = folder.rstrip('/')
            if search_key == "": search_key = "Raiz"
            
            # Título da seção da pasta
            f.write(f"### 📂 `{search_key}`\n")

            # CASO 1: A pasta tem arquivos nela (está no dicionário folder_stats)
            if search_key in folder_stats:
                years_data = folder_stats[search_key]
                sorted_years = sorted(years_data.keys()) # Ordena anos (2020, 2021...)
                
                # Tabela Markdown
                f.write("| Ano | Qtd. Arquivos |\n")
                f.write("| :--- | :--- |\n")
                
                total_local = 0
                for year in sorted_years:
                    count = years_data[year]
                    total_local += count
                    f.write(f"| {year} | {count} |\n")
                
                f.write(f"\n**Total nesta pasta:** {total_local} arquivos\n")
            
            # CASO 2: A pasta existe na hierarquia, mas não tem arquivos diretos (só subpastas)
            elif folder in all_known_paths and search_key not in files_found_paths:
                 f.write("> *ℹ️ Esta pasta contém apenas subpastas.*\n")
            
            # CASO 3: Residual (Pasta vazia ou marcador)
            else:
                 f.write("> *Pasta vazia.*\n")

            # Separador visual entre seções
            f.write("\n---\n")

    print("Concluído!")

# ================= EXECUÇÃO PRINCIPAL =================

if __name__ == "__main__":
    # Verifica se a varredura pode começar
    if not BUCKET_NAME:
        print("❌ ERRO: Configure o nome do bucket no arquivo .env antes de rodar.")
    else:
        # 1. Coleta dados
        # Passamos o BUCKET_NAME e agora também o TARGET_FOLDER (que pode ser vazio ou uma pasta)
        stats, all_paths, file_paths, total = scan_bucket(BUCKET_NAME, TARGET_FOLDER)
        
        print(f"Varredura completa. {total} arquivos encontrados.")
        
        # 2. Gera relatório
        generate_markdown_report(stats, all_paths, file_paths)