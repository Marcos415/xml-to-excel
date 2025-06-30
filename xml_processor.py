import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, date # Importar 'date' novamente para a data sem hora

def processar_xmls_e_extrair_para_dataframe(diretorio_xmls):
    """
    Processa todos os arquivos XML em um diretório (incluindo subpastas)
    e extrai DATA (sem hora), NUMERO_NF e VALOR TOTAL NF.

    Args:
        diretorio_xmls (str): O caminho para o diretório contendo os arquivos XML.

    Returns:
        pd.DataFrame: Um DataFrame do pandas com as colunas DATA, NUMERO_NF,
                      e VALOR TOTAL NF. Retorna um DataFrame vazio se nenhum XML válido for encontrado.
    """
    dados_extraidos = []

    for root, _, files in os.walk(diretorio_xmls):
        for filename in files:
            if filename.lower().endswith('.xml'):
                filepath = os.path.join(root, filename)

                try:
                    tree = ET.parse(filepath)
                    root_element = tree.getroot()

                    namespaces = {
                        'nfe': 'http://www.portalfiscal.inf.br/nfe',
                        'cte': 'http://www.portalfiscal.inf.br/cte'
                    }

                    # --- Extração do NÚMERO DA NF (Número do Documento) ---
                    numero_nf = None
                    nNF_element = root_element.find('.//nfe:ide/nfe:nNF', namespaces)
                    if nNF_element is not None:
                        numero_nf = nNF_element.text
                    
                    if numero_nf is None:
                        nCT_element = root_element.find('.//cte:infCte/cte:ide/cte:nCT', namespaces)
                        if nCT_element is not None:
                            numero_nf = nCT_element.text

                    # --- Extração do VALOR TOTAL DA NOTA (vNF) ---
                    valor_total_nf = None
                    vNF_element = root_element.find('.//nfe:total/nfe:ICMSTot/nfe:vNF', namespaces)
                    if vNF_element is not None and vNF_element.text:
                        try:
                            valor_total_nf = float(vNF_element.text)
                        except ValueError:
                            print(f"Aviso: Não foi possível converter vNF '{vNF_element.text}' para float em {filename}. Armazenando como string.")
                            valor_total_nf = vNF_element.text # Armazena como string se a conversão falhar

                    # --- NOVO: Extração da DATA SEM HORA ---
                    data_emissao_str = None
                    # Tenta nfe:dhEmi
                    dhEmi_element = root_element.find('.//nfe:ide/nfe:dhEmi', namespaces)
                    if dhEmi_element is not None:
                        data_emissao_str = dhEmi_element.text
                    else:
                        # Tenta nfe:dEmi
                        dEmi_element = root_element.find('.//nfe:ide/nfe:dEmi', namespaces)
                        if dEmi_element is not None:
                            data_emissao_str = dEmi_element.text
                    
                    # Tenta cte:dhEmi
                    if data_emissao_str is None:
                        dhEmi_cte_element = root_element.find('.//cte:ide/cte:dhEmi', namespaces)
                        if dhEmi_cte_element is not None:
                            data_emissao_str = dhEmi_cte_element.text
                    
                    data_nf_obj = None # Armazenará o objeto date (apenas data)
                    if data_emissao_str:
                        try:
                            # Pega apenas a parte da data (AAAA-MM-DD), ignorando a hora e o fuso horário
                            date_part = data_emissao_str.split('T')[0]
                            data_nf_obj = datetime.strptime(date_part, '%Y-%m-%d').date() # Converte para objeto date
                        except ValueError:
                            print(f"Aviso: Não foi possível parsear a data '{data_emissao_str}' do arquivo {filename}. Será tratada como nula.")
                            data_nf_obj = None

                    dados_extraidos.append({
                        'DATA': data_nf_obj, # Coluna DATA sem hora, como objeto date
                        'NUMERO_NF': numero_nf,
                        'VALOR TOTAL NF': valor_total_nf # Valor como float ou string
                    })

                except ET.ParseError as e:
                    print(f"Erro ao parsear o XML {filename} em {filepath}: {e}. Arquivo pode estar malformado ou não é um XML válido.")
                except Exception as e:
                    print(f"Erro inesperado ao processar {filename} em {filepath}: {e}")

    df = pd.DataFrame(dados_extraidos)
    return df