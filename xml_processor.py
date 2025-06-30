import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, date, time # Importar 'date' e 'time' agora

def processar_xmls_e_extrair_para_dataframe(diretorio_xmls):
    """
    Processa todos os arquivos XML em um diretório (incluindo subpastas)
    e extrai MÊS, DATA, HORA, NUMERO_NF, CHAVE DE 44 DÍGITOS, e VALOR TOTAL NF.

    Args:
        diretorio_xmls (str): O caminho para o diretório contendo os arquivos XML.

    Returns:
        pd.DataFrame: Um DataFrame do pandas com as colunas MÊS, DATA, HORA,
                      NUMERO_NF, CHAVE DE 44 DÍGITOS, e VALOR TOTAL NF.
                      Retorna um DataFrame vazio se nenhum XML válido for encontrado.
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

                    # --- Extração do VALOR TOTAL DA NOTA (vNF) ---
                    valor_total_nf = None
                    vNF_element = root_element.find('.//nfe:total/nfe:ICMSTot/nfe:vNF', namespaces)
                    if vNF_element is not None and vNF_element.text:
                        try:
                            valor_total_nf = float(vNF_element.text)
                        except ValueError:
                            print(f"Aviso: Não foi possível converter vNF '{vNF_element.text}' para float em {filename}. Armazenando como string.")
                            valor_total_nf = vNF_element.text

                    # --- Extração e Tratamento da DATA e HORA (Separadas) ---
                    data_hora_emissao_str = None
                    # Tenta nfe:dhEmi
                    dhEmi_element = root_element.find('.//nfe:ide/nfe:dhEmi', namespaces)
                    if dhEmi_element is not None:
                        data_hora_emissao_str = dhEmi_element.text
                    else:
                        # Tenta nfe:dEmi
                        dEmi_element = root_element.find('.//nfe:ide/nfe:dEmi', namespaces)
                        if dEmi_element is not None:
                            data_hora_emissao_str = dEmi_element.text + 'T00:00:00' # Adiciona hora padrão se só tiver data
                    
                    if data_hora_emissao_str is None:
                        dhEmi_cte_element = root_element.find('.//cte:ide/cte:dhEmi', namespaces)
                        if dhEmi_cte_element is not None:
                            data_hora_emissao_str = dhEmi_cte_element.text
                    
                    data_nf_obj = None # Armazenará o objeto date
                    hora_nf_obj = None # Armazenará o objeto time
                    mes_nf = None
                    
                    if data_hora_emissao_str:
                        try:
                            # Remove o fuso horário para facilitar a conversão
                            if '+' in data_hora_emissao_str:
                                data_hora_emissao_str = data_hora_emissao_str.split('+')[0]
                            elif '-' in data_hora_emissao_str[data_hora_emissao_str.find('T'):]:
                                data_hora_emissao_str = data_hora_emissao_str.rsplit('-', 1)[0]
                                
                            full_datetime_obj = datetime.fromisoformat(data_hora_emissao_str)
                            data_nf_obj = full_datetime_obj.date() # Apenas a data
                            hora_nf_obj = full_datetime_obj.time() # Apenas a hora
                            mes_nf = full_datetime_obj.strftime('%m/%Y')
                        except ValueError:
                            print(f"Aviso: Não foi possível parsear a data/hora '{data_hora_emissao_str}' do arquivo {filename}. Será tratada como nula.")
                            data_nf_obj = None
                            hora_nf_obj = None
                            mes_nf = None

                    dados_extraidos.append({
                        'MÊS': mes_nf,
                        'DATA': data_nf_obj,     # Coluna para a data (objeto date)
                        'HORA': hora_nf_obj,     # Coluna para a hora (objeto time)
                        'NUMERO_NF': numero_nf,
                        'CHAVE DE 44 DÍGITOS': chave_44_digitos,
                        'VALOR TOTAL NF': valor_total_nf
                    })

                except ET.ParseError as e:
                    print(f"Erro ao parsear o XML {filename} em {filepath}: {e}. Arquivo pode estar malformado ou não é um XML válido.")
                except Exception as e:
                    print(f"Erro inesperado ao processar {filename} em {filepath}: {e}")

    df = pd.DataFrame(dados_extraidos)
    return df