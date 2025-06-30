import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, date # Import date as well

def processar_xmls_e_extrair_para_dataframe(diretorio_xmls):
    """
    Processa todos os arquivos XML em um diretório (incluindo subpastas)
    e extrai informações específicas, incluindo valores e datas.

    Args:
        diretorio_xmls (str): The path to the directory containing the XML files.

    Returns:
        pd.DataFrame: A pandas DataFrame with extracted columns.
                      Returns an empty DataFrame if no valid XMLs are found or processed.
    """
    dados_extraidos = []

    # Iterate through all files in the directory and its subdirectories
    for root, _, files in os.walk(diretorio_xmls):
        for filename in files:
            if filename.lower().endswith('.xml'): # Use .lower() for case-insensitive check
                filepath = os.path.join(root, filename)

                try:
                    tree = ET.parse(filepath)
                    root_element = tree.getroot()

                    # Define namespaces for NFe and CTe for easier searching
                    namespaces = {
                        'nfe': 'http://www.portalfiscal.inf.br/nfe',
                        'cte': 'http://www.portalfiscal.inf.br/cte'
                    }

                    # --- Extract CHAVE DE 44 DÍGITOS ---
                    chave_44_digitos = None
                    # Try NFe Id attribute first
                    infNFe_element = root_element.find('.//nfe:infNFe', namespaces)
                    if infNFe_element is not None and 'Id' in infNFe_element.attrib:
                        chave_44_digitos = infNFe_element.attrib['Id'].replace('NFe', '')
                    
                    # If not found, try nfe:chNFe (for NFe cancellation, etc.)
                    if chave_44_digitos is None:
                        chNFe_element = root_element.find('.//nfe:chNFe', namespaces)
                        if chNFe_element is not None:
                            chave_44_digitos = chNFe_element.text
                    
                    # If still not found, try cte:chCTe
                    if chave_44_digitos is None:
                        chCTe_element = root_element.find('.//cte:chCTe', namespaces)
                        if chCTe_element is not None:
                            chave_44_digitos = chCTe_element.text

                    # --- Extract NUMERO DA NF (Número do Documento) ---
                    numero_nf = None
                    # Try nfe:nNF
                    nNF_element = root_element.find('.//nfe:ide/nfe:nNF', namespaces)
                    if nNF_element is not None:
                        numero_nf = nNF_element.text
                    
                    # If not found, try cte:nCT
                    if numero_nf is None:
                        nCT_element = root_element.find('.//cte:infCte/cte:ide/cte:nCT', namespaces)
                        if nCT_element is not None:
                            numero_nf = nCT_element.text

                    # --- Extract VALOR TOTAL DA NOTA (vNF) ---
                    valor_nf = None
                    # Path: <total><ICMSTot><vNF>
                    vNF_element = root_element.find('.//nfe:total/nfe:ICMSTot/nfe:vNF', namespaces)
                    if vNF_element is not None and vNF_element.text:
                        try:
                            valor_nf = float(vNF_element.text)
                        except ValueError:
                            print(f"Warning: Could not convert vNF '{vNF_element.text}' to float in {filename}. Storing as string.")
                            valor_nf = vNF_element.text # Store as string if conversion fails

                    # --- Extract VALOR PAGO (vPag) ---
                    # Path: <pag><detPag><vPag> (as per your example, this is the one you need)
                    valor_pag = None
                    vPag_element = root_element.find('.//nfe:pag/nfe:detPag/nfe:vPag', namespaces)
                    if vPag_element is not None and vPag_element.text:
                        try:
                            valor_pag = float(vPag_element.text)
                        except ValueError:
                            print(f"Warning: Could not convert vPag '{vPag_element.text}' to float in {filename}. Storing as string.")
                            valor_pag = vPag_element.text # Store as string if conversion fails

                    # --- Extract DATA and MÊS ---
                    data_emissao_str = None
                    # Try nfe:dhEmi
                    dhEmi_element = root_element.find('.//nfe:ide/nfe:dhEmi', namespaces)
                    if dhEmi_element is not None:
                        data_emissao_str = dhEmi_element.text
                    else:
                        # Try nfe:dEmi
                        dEmi_element = root_element.find('.//nfe:ide/nfe:dEmi', namespaces)
                        if dEmi_element is not None:
                            data_emissao_str = dEmi_element.text
                    
                    # Try cte:dhEmi
                    if data_emissao_str is None:
                        dhEmi_cte_element = root_element.find('.//cte:ide/cte:dhEmi', namespaces)
                        if dhEmi_cte_element is not None:
                            data_emissao_str = dhEmi_cte_element.text
                    
                    # Process the date string into a datetime.date object
                    data_nf_obj = None
                    mes_nf = None
                    if data_emissao_str:
                        try:
                            # Extract only the date part before 'T' (e.g., '2025-01-02T...')
                            if 'T' in data_emissao_str:
                                date_part = data_emissao_str.split('T')[0]
                            else:
                                date_part = data_emissao_str # Assume YYYY-MM-DD format

                            data_nf_obj = datetime.strptime(date_part, '%Y-%m-%d').date() # Store as date object
                            mes_nf = data_nf_obj.strftime('%m/%Y') # Format MM/YYYY for the 'MÊS' column
                        except ValueError:
                            print(f"Warning: Could not parse date '{data_emissao_str}' from {filename}. It will be null.")
                            data_nf_obj = None
                            mes_nf = None

                    dados_extraidos.append({
                        'MÊS': mes_nf,
                        'DATA': data_nf_obj, # Store as a date object for proper sorting
                        'NUMERO_NF': numero_nf,
                        'CHAVE DE 44 DÍGITOS': chave_44_digitos,
                        'VALOR TOTAL NF': valor_nf, # New column
                        'VALOR PAGO': valor_pag # New column
                    })

                except ET.ParseError as e:
                    print(f"Error parsing XML file {filename} at {filepath}: {e}. File might be malformed or not a valid XML.")
                except Exception as e:
                    print(f"Unexpected error processing {filename} at {filepath}: {e}")

    df = pd.DataFrame(dados_extraidos)
    return df