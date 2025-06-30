import os
import zipfile
import shutil
from flask import Flask, render_template, request, send_file
from werkzeug.utils import secure_filename
import pandas as pd
from datetime import datetime, date # Importe 'date' também

from xml_processor import processar_xmls_e_extrair_para_dataframe

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
GENERATED_FOLDER = 'generated_excel'
ALLOWED_EXTENSIONS = {'zip'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['GENERATED_FOLDER'] = GENERATED_FOLDER

# Garante que as pastas de upload e geração existam
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(GENERATED_FOLDER, exist_ok=True)

def allowed_file(filename):
    """Verifica se o tipo de arquivo é permitido."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Renderiza a página inicial."""
    return render_template('index.html')

@app.route('/processar', methods=['POST'])
def processar_arquivos():
    """
    Processa os arquivos ZIP enviados, extrai XMLs, consolida dados em um Excel
    e retorna o arquivo Excel para download.
    """
    todos_dfs = []
    temp_paths_to_clean = [] # Lista para armazenar caminhos temporários a serem limpos

    try:
        # Verifica se há arquivos ZIP no request
        if 'arquivos_zip[]' not in request.files:
            return "Nenhum arquivo enviado.", 400

        zip_files = request.files.getlist('arquivos_zip[]')

        if not zip_files or all(f.filename == '' for f in zip_files):
            return "Nenhum arquivo selecionado.", 400

        # Itera sobre cada arquivo ZIP enviado
        for i, file in enumerate(zip_files):
            # Valida a extensão do arquivo
            if not allowed_file(file.filename):
                return f"Tipo de arquivo não permitido para '{file.filename}'. Apenas arquivos .{', .'.join(ALLOWED_EXTENSIONS)} são aceitos.", 400

            # Salva o arquivo ZIP temporariamente
            filename = secure_filename(file.filename)
            zip_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(zip_path)
            temp_paths_to_clean.append(zip_path) # Adiciona à lista de limpeza

            print(f"\n--- Processando ZIP: {filename} ({i+1}/{len(zip_files)}) ---")
            print(f"ZIP salvo em: {zip_path}")

            # Cria um diretório temporário para descompactar os XMLs
            # Adiciona timestamp para garantir unicidade do nome da pasta
            temp_xml_dir_name = os.path.splitext(filename)[0] + f"_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            temp_xml_dir_path = os.path.join(app.config['UPLOAD_FOLDER'], temp_xml_dir_name)
            os.makedirs(temp_xml_dir_path, exist_ok=True)
            temp_paths_to_clean.append(temp_xml_dir_path) # Adiciona à lista de limpeza

            print(f"Descompactando para: {temp_xml_dir_path}")

            # Descompacta o ZIP
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_xml_dir_path)
                print(f"Conteúdo do ZIP descompactado (nomes dos membros): {zip_ref.namelist()}")

            # Log para depuração da estrutura de pastas
            print(f"Conteúdo da pasta descompactada '{temp_xml_dir_path}':")
            for root_walk, dirs_walk, files_walk in os.walk(temp_xml_dir_path):
                level = root_walk.replace(temp_xml_dir_path, '').count(os.sep)
                indent = ' ' * 4 * (level)
                print(f'{indent}{os.path.basename(root_walk)}/')
                subindent = ' ' * 4 * (level + 1)
                for f_name in files_walk:
                    print(f'{subindent}{f_name}')
            print("--- Fim da depuração de estrutura ---")

            # Processa os XMLs descompactados e extrai para um DataFrame
            df_resultado_individual = processar_xmls_e_extrair_para_dataframe(temp_xml_dir_path)
            
            if not df_resultado_individual.empty:
                todos_dfs.append(df_resultado_individual)
            else:
                print(f"Aviso: Nenhum dado extraído do ZIP '{filename}'. Verifique se contém XMLs válidos ou a estrutura de pastas.")

        # Verifica se algum dado foi extraído de todos os ZIPs
        if not todos_dfs:
            return "Processamento concluído, mas nenhum dado foi extraído dos XMLs de nenhum ZIP. Verifique o conteúdo dos ZIPs.", 404

        # Concatena todos os DataFrames individuais em um único DataFrame final
        df_final = pd.concat(todos_dfs, ignore_index=True)

        # --- ORDENAÇÃO POR DATA DE EMISSÃO DECRESCENTE E FORMATAÇÃO ---
        # Verifica se a coluna 'DATA' existe e não está vazia
        if 'DATA' in df_final.columns and not df_final['DATA'].empty:
            # Converte a coluna 'DATA' para o tipo datetime do pandas.
            # 'errors='coerce' transforma valores inválidos em NaT (Not a Time).
            df_final['DATA'] = pd.to_datetime(df_final['DATA'], errors='coerce')
            
            # Remove linhas onde a conversão da data falhou (contêm NaT)
            df_final.dropna(subset=['DATA'], inplace=True)
            
            # Ordena o DataFrame pela coluna 'DATA' em ordem decrescente (da mais recente para a mais antiga)
            df_final = df_final.sort_values(by='DATA', ascending=False)
            
            # Opcional: Formata a coluna 'DATA' de volta para string 'DD/MM/YYYY' para exibição no Excel.
            # Se preferir o formato de data nativo do Excel, você pode remover esta linha.
            df_final['DATA'] = df_final['DATA'].dt.strftime('%d/%m/%Y')
        else:
            print("Aviso: Coluna 'DATA' não encontrada ou está vazia no DataFrame final para ordenação.")
            
        # Gera o nome do arquivo Excel com timestamp e salva
        excel_filename = f"resultado_consolidado_xmls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        excel_path = os.path.join(app.config['GENERATED_FOLDER'], excel_filename)

        df_final.to_excel(excel_path, index=False)
        print(f"\nExcel final salvo em: {excel_path}")

        # Envia o arquivo Excel gerado para o cliente
        return send_file(excel_path, as_attachment=True, download_name=excel_filename,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except zipfile.BadZipFile as e:
        print(f"DEBUG: Erro de ZIP inválido: {e}")
        return f"Erro: Um dos arquivos enviados ('{filename}' se disponível) não é um ZIP válido ou está corrompido. Por favor, envie arquivos .zip.", 400
    except Exception as e:
        print(f"DEBUG: Ocorreu um erro inesperado no servidor: {e}")
        return f"Erro interno do servidor durante o processamento: {e}", 500
    finally:
        # Bloco finally para garantir a limpeza de arquivos e diretórios temporários
        for path in temp_paths_to_clean:
            if os.path.exists(path):
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path) # Remove diretórios e seu conteúdo
                        print(f"Diretório temporário removido: {path}")
                    else:
                        os.remove(path) # Remove arquivos
                        print(f"Arquivo temporário removido: {path}")
                except OSError as e:
                    print(f"DEBUG: Erro ao remover caminho temporário {path}: {e}")
        # A remoção do arquivo Excel final é tratada pelo sistema após o send_file.

if __name__ == '__main__':
    # Inicia o servidor Flask em modo de depuração (para desenvolvimento local)
    app.run(debug=True)