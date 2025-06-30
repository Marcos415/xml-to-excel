import os
import zipfile
import shutil
from flask import Flask, render_template, request, send_file
from werkzeug.utils import secure_filename
import pandas as pd
from datetime import datetime, date, time # Importar 'date' e 'time' também

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

        # --- ORDENAÇÃO E FORMATAÇÃO ---
        # 1. Preparar colunas para ordenação
        if 'DATA' in df_final.columns and not df_final['DATA'].empty:
            df_final['DATA'] = pd.to_datetime(df_final['DATA'], errors='coerce')
            df_final.dropna(subset=['DATA'], inplace=True)
        
        # Combinar DATA e HORA para uma ordenação mais precisa, se ambas existirem
        # Criamos uma coluna temporária para ordenar
        df_final['DATA_TEMP_ORDENACAO'] = pd.NaT # Inicializa com Not a Time
        if 'DATA' in df_final.columns and 'HORA' in df_final.columns:
            # Concatena a data com a string da hora (00:00:00) para criar um datetime completo
            # pd.to_datetime pode lidar com string de date e time separadamente, mas juntar é mais seguro
            # Certifique-se de que a coluna HORA seja string para evitar problemas de tipo
            df_final['HORA_STR'] = df_final['HORA'].apply(lambda x: x.strftime('%H:%M:%S') if isinstance(x, time) else '00:00:00')
            df_final['DATA_TEMP_ORDENACAO'] = pd.to_datetime(df_final['DATA'].dt.strftime('%Y-%m-%d') + ' ' + df_final['HORA_STR'], errors='coerce')
        elif 'DATA' in df_final.columns:
            df_final['DATA_TEMP_ORDENACAO'] = df_final['DATA'] # Se só tiver data, usa só a data

        # Converter NUMERO_NF e VALOR TOTAL NF para numérico para ordenação correta
        if 'NUMERO_NF' in df_final.columns:
            df_final['NUMERO_NF'] = pd.to_numeric(df_final['NUMERO_NF'], errors='coerce').fillna(df_final['NUMERO_NF'])
        
        if 'VALOR TOTAL NF' in df_final.columns:
            df_final['VALOR TOTAL NF'] = pd.to_numeric(df_final['VALOR TOTAL NF'], errors='coerce').fillna(df_final['VALOR TOTAL NF'])

        # Define a ordem de ordenação (DATA_TEMP_ORDENACAO é a principal)
        sort_columns = []
        if 'DATA_TEMP_ORDENACAO' in df_final.columns:
            sort_columns.append('DATA_TEMP_ORDENACAO')
        if 'NUMERO_NF' in df_final.columns:
            sort_columns.append('NUMERO_NF')
        if 'VALOR TOTAL NF' in df_final.columns: # Mantendo VALOR TOTAL NF na ordenação também
            sort_columns.append('VALOR TOTAL NF')

        if sort_columns:
            df_final = df_final.sort_values(by=sort_columns, ascending=True) # ORDENAÇÃO CRESCENTE
        else:
            print("Aviso: Nenhuma coluna de ordenação válida encontrada.")

        # --- FORMATAR COLUNAS PARA SAÍDA NO EXCEL ---
        # Formata a DATA para DD/MM/YYYY
        if 'DATA' in df_final.columns:
            df_final['DATA'] = df_final['DATA'].dt.strftime('%d/%m/%Y')
        
        # Formata a HORA para HH:MM:SS
        if 'HORA' in df_final.columns:
            df_final['HORA'] = df_final['HORA'].apply(lambda x: x.strftime('%H:%M:%S') if isinstance(x, time) else None)

        # Reorganizar a ordem das colunas no DataFrame final para o Excel
        # Incluindo todas as colunas que agora serão retornadas
        final_columns_order = [
            'MÊS',
            'DATA',
            'HORA',
            'NUMERO_NF',
            'CHAVE DE 44 DÍGITOS',
            'VALOR TOTAL NF'
        ]
        # Remove colunas temporárias
        df_final = df_final.drop(columns=['DATA_TEMP_ORDENACAO', 'HORA_STR'], errors='ignore')
        
        # Garante que todas as colunas existentes no df_final sejam mantidas, mesmo que não estejam em final_columns_order
        # e as coloca no final, se for o caso.
        existing_cols_ordered = [col for col in final_columns_order if col in df_final.columns]
        other_cols = [col for col in df_final.columns if col not in existing_cols_ordered]
        df_final = df_final[existing_cols_ordered + other_cols]

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