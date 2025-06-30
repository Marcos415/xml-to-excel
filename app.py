import os
import zipfile
import shutil
from flask import Flask, render_template, request, send_file
from werkzeug.utils import secure_filename
import pandas as pd
from datetime import datetime # Importar datetime para uso geral

from xml_processor import processar_xmls_e_extrair_para_dataframe

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
GENERATED_FOLDER = 'generated_excel'
ALLOWED_EXTENSIONS = {'zip'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['GENERATED_FOLDER'] = GENERATED_FOLDER

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(GENERATED_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/processar', methods=['POST'])
def processar_arquivos():
    todos_dfs = []
    temp_paths_to_clean = []

    try:
        if 'arquivos_zip[]' not in request.files:
            return "Nenhum arquivo enviado.", 400

        zip_files = request.files.getlist('arquivos_zip[]')

        if not zip_files or all(f.filename == '' for f in zip_files):
            return "Nenhum arquivo selecionado.", 400

        for i, file in enumerate(zip_files):
            if not allowed_file(file.filename):
                return f"Tipo de arquivo não permitido para '{file.filename}'. Apenas arquivos .{', .'.join(ALLOWED_EXTENSIONS)} são aceitos.", 400

            filename = secure_filename(file.filename)
            zip_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(zip_path)
            temp_paths_to_clean.append(zip_path)

            print(f"\n--- Processando ZIP: {filename} ({i+1}/{len(zip_files)}) ---")
            print(f"ZIP salvo em: {zip_path}")

            temp_xml_dir_name = os.path.splitext(filename)[0] + f"_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            temp_xml_dir_path = os.path.join(app.config['UPLOAD_FOLDER'], temp_xml_dir_name)
            os.makedirs(temp_xml_dir_path, exist_ok=True)
            temp_paths_to_clean.append(temp_xml_dir_path)

            print(f"Descompactando para: {temp_xml_dir_path}")

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_xml_dir_path)
                print(f"Conteúdo do ZIP descompactado (nomes dos membros): {zip_ref.namelist()}")

            print(f"Conteúdo da pasta descompactada '{temp_xml_dir_path}':")
            for root_walk, dirs_walk, files_walk in os.walk(temp_xml_dir_path):
                level = root_walk.replace(temp_xml_dir_path, '').count(os.sep)
                indent = ' ' * 4 * (level)
                print(f'{indent}{os.path.basename(root_walk)}/')
                subindent = ' ' * 4 * (level + 1)
                for f_name in files_walk:
                    print(f'{subindent}{f_name}')
            print("--- Fim da depuração de estrutura ---")

            df_resultado_individual = processar_xmls_e_extrair_para_dataframe(temp_xml_dir_path)
            
            if not df_resultado_individual.empty:
                todos_dfs.append(df_resultado_individual)
            else:
                print(f"Aviso: Nenhum dado extraído do ZIP '{filename}'. Verifique se contém XMLs válidos ou a estrutura de pastas.")

        if not todos_dfs:
            return "Processamento concluído, mas nenhum dado foi extraído dos XMLs de nenhum ZIP. Verifique o conteúdo dos ZIPs.", 404

        df_final = pd.concat(todos_dfs, ignore_index=True)

        # --- ORDENAÇÃO CRESCENTE E FORMATAÇÃO ---
        # 1. Ordenar por DATA (crescente)
        if 'DATA' in df_final.columns and not df_final['DATA'].empty:
            df_final['DATA'] = pd.to_datetime(df_final['DATA'], errors='coerce')
            df_final.dropna(subset=['DATA'], inplace=True)
        
        # 2. Ordenar por NUMERO_NF (crescente, após a data)
        # 3. Ordenar por VALOR TOTAL NF (crescente, após data e número da nota)
        # Converte para numérico para ordenar corretamente, se não for, tenta converter para string
        if 'NUMERO_NF' in df_final.columns:
            df_final['NUMERO_NF'] = pd.to_numeric(df_final['NUMERO_NF'], errors='coerce').fillna(df_final['NUMERO_NF'])
        
        if 'VALOR TOTAL NF' in df_final.columns:
            df_final['VALOR TOTAL NF'] = pd.to_numeric(df_final['VALOR TOTAL NF'], errors='coerce').fillna(df_final['VALOR TOTAL NF'])

        # Define a ordem de ordenação
        sort_columns = []
        if 'DATA' in df_final.columns:
            sort_columns.append('DATA')
        if 'NUMERO_NF' in df_final.columns:
            sort_columns.append('NUMERO_NF')
        if 'VALOR TOTAL NF' in df_final.columns:
            sort_columns.append('VALOR TOTAL NF')

        if sort_columns:
            df_final = df_final.sort_values(by=sort_columns, ascending=True) # ORDENAÇÃO CRESCENTE
        else:
            print("Aviso: Nenhuma coluna de ordenação (DATA, NUMERO_NF, VALOR TOTAL NF) encontrada.")

        # --- FORMATAR COLUNAS PARA SAÍDA NO EXCEL ---
        # Formata a DATA para DD/MM/YYYY
        if 'DATA' in df_final.columns:
            df_final['DATA'] = df_final['DATA'].dt.strftime('%d/%m/%Y')
        
        # Formata VALOR TOTAL NF com '$' no início
        if 'VALOR TOTAL NF' in df_final.columns:
            df_final['VALOR TOTAL NF'] = df_final['VALOR TOTAL NF'].apply(
                lambda x: f"$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if isinstance(x, (int, float)) else x
            )
            # A formatação acima é para BR (milhar com '.', decimal com ','). Se o valor for string, mantém.

        # Reorganizar a ordem das colunas no DataFrame final para o Excel
        final_columns_order = []
        if 'DATA' in df_final.columns:
            final_columns_order.append('DATA')
        if 'NUMERO_NF' in df_final.columns:
            final_columns_order.append('NUMERO_NF')
        if 'VALOR TOTAL NF' in df_final.columns:
            final_columns_order.append('VALOR TOTAL NF')
        
        # Adiciona outras colunas que possam existir, mas não estão na ordem desejada
        for col in df_final.columns:
            if col not in final_columns_order:
                final_columns_order.append(col)

        df_final = df_final[final_columns_order]


        excel_filename = f"resultado_consolidado_xmls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        excel_path = os.path.join(app.config['GENERATED_FOLDER'], excel_filename)

        df_final.to_excel(excel_path, index=False)
        print(f"\nExcel final salvo em: {excel_path}")

        return send_file(excel_path, as_attachment=True, download_name=excel_filename,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except zipfile.BadZipFile as e:
        print(f"DEBUG: Erro de ZIP inválido: {e}")
        return f"Erro: Um dos arquivos enviados ('{filename}' se disponível) não é um ZIP válido ou está corrompido. Por favor, envie arquivos .zip.", 400
    except Exception as e:
        print(f"DEBUG: Ocorreu um erro inesperado no servidor: {e}")
        return f"Erro interno do servidor durante o processamento: {e}", 500
    finally:
        for path in temp_paths_to_clean:
            if os.path.exists(path):
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                        print(f"Diretório temporário removido: {path}")
                    else:
                        os.remove(path)
                        print(f"Arquivo temporário removido: {path}")
                except OSError as e:
                    print(f"DEBUG: Erro ao remover caminho temporário {path}: {e}")

if __name__ == '__main__':
    app.run(debug=True)