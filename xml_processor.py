import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, date # Import date as well

def processar_xmls_e_extrair_para_dataframe(diretorio_xmls):
    """
    Processa todos os arquivos XML em um diretório (incluindo subpastas)
    e extrai informações específicas, incluindo o valor total da NF e datas.

    Args:
        diretorio_xmls (str): O caminho para o diretório contendo os arquivos XML.

    Returns:
        pd.DataFrame: Um DataFrame do pandas com as colunas MÊS, DATA, NUMERO_NF,
                      CHAVE DE 44 DÍGITOS, e VALOR TOTAL NF.
                      Retorna um DataFrame vazio se nenhum XML válido for encontrado ou processado.
    """
    dados_extraidos = []

    # Iterar por todos os arquivos no diretório e subdiretórios
    for root, _, files in os.walk(diretorio_xmls):
        for filename in files:
            if filename.lower().endswith('.xml'): # Verifica a extensão de forma insensível a maiúsculas/minúsculas
                filepath = os.path.join(root, filename)

                try:
                    tree = ET.parse(filepath)
                    root_element = tree.getroot()

                    # Define os namespaces para NF-e e CT-e (crucial para encontrar as tags)
                    namespaces = {
                        'nfe': 'http://www.portalfiscal.inf.br/nfe',
                        'cte': 'http://www.portalfiscal.inf.br/cte'
                    }

                    # --- Extração da CHAVE DE 44 DÍGITOS ---
                    chave_44_digitos = None
                    # Tenta o atributo Id de infNFe primeiro (para NF-e)
                    infNFe_element = root_element.find('.//nfe:infNFe', namespaces)
                    if infNFe_element is not None and 'Id' in infNFe_element.attrib:
                        chave_44_digitos = infNFe_element.attrib['Id'].replace('NFe', '')
                    
                    # Se não encontrado, tenta nfe:chNFe
                    if chave_44_digitos is None:
                        chNFe_element = root_element.find('.//nfe:chNFe', namespaces)
                        if chNFe_element is not None:
                            chave_44_digitos = chNFe_element.text
                    
                    # Se ainda não encontrado, tenta cte:chCTe (para CT-e)
                    if chave_44_digitos is None:
                        chCTe_element = root_element.find('.//cte:chCTe', namespaces)
                        if chCTe_element is not None:
                            chave_44_digitos = chCTe_element.text

                    # --- Extração do NÚMERO DA NF (Número do Documento) ---
                    numero_nf = None
                    # Tenta nfe:nNF
                    nNF_element = root_element.find('.//nfe:ide/nfe:nNF', namespaces)
                    if nNF_element is not None:
                        numero_nf = nNF_element.text
                    
                    # Se não encontrado, tenta cte:nCT
                    if numero_nf is None:
                        nCT_element = root_element.find('.//cte:infCte/cte:ide/cte:nCT', namespaces)
                        if nCT_element is not None:
                            numero_nf = nCT_element.text

                    # --- NOVO FOCO: Extrair APENAS o VALOR TOTAL DA NOTA (vNF) ---
                    # Caminho: <total><ICMSTot><vNF>
                    valor_total_nf = None # Inicializa como None
                    vNF_element = root_element.find('.//nfe:total/nfe:ICMSTot/nfe:vNF', namespaces)
                    if vNF_element is not None and vNF_element.text:
                        try:
                            valor_total_nf = float(vNF_element.text)
                        except ValueError:
                            print(f"Aviso: Não foi possível converter vNF '{vNF_element.text}' para float em {filename}. Armazenando como string.")
                            valor_total_nf = vNF_element.text # Armazena como string se a conversão falhar

                    # --- Extração e Tratamento da DATA e MÊS ---
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
                    
                    # Processa a string da data para um objeto datetime.date
                    data_nf_obj = None
                    mes_nf = None
                    if data_emissao_str:
                        try:
                            # Extrai apenas a parte da data antes de 'T' (ex: '2025-01-02T...')
                            if 'T' in data_emissao_str:
                                date_part = data_emissao_str.split('T')[0]
                            else:
                                date_part = data_emissao_str # Assume formato AAAA-MM-DD

                            data_nf_obj = datetime.strptime(date_part, '%Y-%m-%d').date() # Armazena como objeto date
                            mes_nf = data_nf_obj.strftime('%m/%Y') # Formato MM/AAAA para a coluna 'MÊS'
                        except ValueError:
                            print(f"Aviso: Não foi possível parsear a data '{data_emissao_str}' do arquivo {filename}. Será tratada como nula.")
                            data_nf_obj = None
                            mes_nf = None

                    dados_extraidos.append({
                        'MÊS': mes_nf,
                        'DATA': data_nf_obj, # Armazenado como objeto date para ordenação
                        'NUMERO_NF': numero_nf,
                        'CHAVE DE 44 DÍGITOS': chave_44_digitos,
                        'VALOR TOTAL NF': valor_total_nf # Apenas esta coluna de valor
                    })

                except ET.ParseError as e:
                    print(f"Erro ao parsear o XML {filename} em {filepath}: {e}. Arquivo pode estar malformado ou não é um XML válido.")
                except Exception as e:
                    print(f"Erro inesperado ao processar {filename} em {filepath}: {e}")

    df = pd.DataFrame(dados_extraidos)
    return df