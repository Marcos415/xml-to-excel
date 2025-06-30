import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

def processar_xmls_e_extrair_para_dataframe(diretorio_xmls):
    """
    Processa todos os arquivos XML em um diretório (incluindo subpastas)
    e extrai informações específicas.

    Args:
        diretorio_xmls (str): O caminho para o diretório contendo os arquivos XML.

    Returns:
        pd.DataFrame: Um DataFrame do pandas com as colunas MÊS, DATA, NUMERO_NF, CHAVE DE 44 DIGITOS.
                      Retorna um DataFrame vazio se nenhum XML válido for encontrado ou processado.
    """
    dados_extraidos = []

    # --- CORREÇÃO AQUI: Usar os.walk() para percorrer subpastas ---
    for root, _, files in os.walk(diretorio_xmls):
        for filename in files: # 'files' são os arquivos no diretório 'root' atual
            if filename.endswith(('.xml', '.XML')):
                filepath = os.path.join(root, filename) # Caminho completo para o arquivo XML

                try:
                    tree = ET.parse(filepath)
                    root_element = tree.getroot() # Renomeado para evitar conflito com 'root' do os.walk

                    # Definindo os namespaces para facilitar a busca
                    namespaces = {
                        'nfe': 'http://www.portalfiscal.inf.br/nfe',
                        'cte': 'http://www.portalfiscal.inf.br/cte'
                    }

                    # --- Extração da CHAVE DE 44 DÍGITOS ---
                    chave_44_digitos = None
                    infNFe_element = root_element.find('.//nfe:infNFe', namespaces)
                    if infNFe_element is not None and 'Id' in infNFe_element.attrib:
                        chave_44_digitos = infNFe_element.attrib['Id'].replace('NFe', '')
                    
                    if chave_44_digitos is None:
                        chNFe_element = root_element.find('.//nfe:chNFe', namespaces)
                        if chNFe_element is not None:
                            chave_44_digitos = chNFe_element.text
                    
                    if chave_44_digitos is None:
                        chCTe_element = root_element.find('.//cte:chCTe', namespaces)
                        if chCTe_element is not None:
                            chave_44_digitos = chCTe_element.text

                    # --- Extração do NÚMERO DA NF (Número do Documento) ---
                    numero_nf = None
                    nNF_element = root_element.find('.//nfe:ide/nfe:nNF', namespaces)
                    if nNF_element is not None:
                        numero_nf = nNF_element.text
                    
                    if numero_nf is None:
                        nCT_element = root_element.find('.//cte:infCte/cte:ide/cte:nCT', namespaces)
                        if nCT_element is not None:
                            numero_nf = nCT_element.text

                    # --- Extração da DATA e MÊS ---
                    data_emissao_str = None
                    dhEmi_element = root_element.find('.//nfe:ide/nfe:dhEmi', namespaces)
                    if dhEmi_element is not None:
                        data_emissao_str = dhEmi_element.text
                    else:
                        dEmi_element = root_element.find('.//nfe:ide/nfe:dEmi', namespaces)
                        if dEmi_element is not None:
                            data_emissao_str = dEmi_element.text
                    
                    if data_emissao_str is None:
                        dhEmi_cte_element = root_element.find('.//cte:ide/cte:dhEmi', namespaces)
                        if dhEmi_cte_element is not None:
                            data_emissao_str = dhEmi_cte_element.text

                    data_nf = None
                    mes_nf = None
                    if data_emissao_str:
                        try:
                            if 'T' in data_emissao_str and ('+' in data_emissao_str or '-' in data_emissao_str[data_emissao_str.find('T'):]): # Com fuso horário
                                data_nf = datetime.fromisoformat(data_emissao_str)
                            elif 'T' in data_emissao_str: # Apenas data e hora sem fuso
                                data_nf = datetime.strptime(data_emissao_str.split('T')[0], '%Y-%m-%d')
                            else: # Apenas data
                                data_nf = datetime.strptime(data_emissao_str, '%Y-%m-%d')
                            
                            mes_nf = data_nf.strftime('%m/%Y') # Formato MM/AAAA
                            data_nf = data_nf.strftime('%d/%m/%Y') # Formato DD/MM/YYYY
                        except ValueError:
                            print(f"Aviso: Não foi possível parsear a data '{data_emissao_str}' do arquivo {filename}. Será tratada como nula.")
                            data_nf = None
                            mes_nf = None

                    dados_extraidos.append({
                        'MÊS': mes_nf,
                        'DATA': data_nf,
                        'NUMERO_NF': numero_nf,
                        'CHAVE DE 44 DÍGITOS': chave_44_digitos
                    })

                except ET.ParseError as e:
                    print(f"Erro ao parsear o XML {filename} em {filepath}: {e}. Arquivo pode estar malformado ou não é um XML válido.")
                except Exception as e:
                    print(f"Erro inesperado ao processar {filename} em {filepath}: {e}")

    df = pd.DataFrame(dados_extraidos)
    return df