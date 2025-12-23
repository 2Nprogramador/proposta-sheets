import pandas as pd

import streamlit as st

import plotly.express as px

import plotly

import gspread

from gspread.exceptions import WorksheetNotFound, APIError

import datetime

import numpy as np

import random



# --- FUNÇÕES DE INTERAÇÃO COM GOOGLE SHEETS E GERAÇÃO DE DADOS ---



@st.cache_data(ttl=600)

def load_data_from_gsheets():
    try:
        creds_dict = st.secrets["gcp_service_account"]
        gsheets_url = st.secrets["gsheets"]["url"]
        worksheet_name = st.secrets["gsheets"]["worksheet_name"]

        gc = gspread.service_account_from_dict(creds_dict)
        sh = gc.open_by_url(gsheets_url)
        worksheet = sh.worksheet(worksheet_name)

        data = worksheet.get_all_records()
        df_sheet = pd.DataFrame.from_records(data)

        df_sheet.dropna(how='all', inplace=True)
        
        try:
            df_sheet['Data'] = pd.to_datetime(df_sheet['Data'], errors='coerce')
        except Exception:
             df_sheet['Data'] = pd.to_datetime(df_sheet['Data'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            
        df_sheet['Total'] = pd.to_numeric(df_sheet['Total'], errors='coerce')
        df_sheet['Quantity'] = pd.to_numeric(df_sheet['Quantity'], errors='coerce').astype('Int64')
        
        # --- NOVO: Converter Rating para número ---
        df_sheet['Rating'] = pd.to_numeric(df_sheet['Rating'], errors='coerce') 

        df_sheet.dropna(subset=['Data', 'Total', 'Quantity'], inplace=True)
        df_sheet = df_sheet[df_sheet['Total'] > 0]
        df_sheet.dropna(subset=['Data'], inplace=True)
        
        return df_sheet

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()
        return pd.DataFrame()



def salvar_dados_gsheets(df_novos_dados):

    """

    Recebe um DataFrame com novos dados e adiciona (append) na planilha do Google Sheets.

    """

    try:

        creds_dict = st.secrets["gcp_service_account"]

        gsheets_url = st.secrets["gsheets"]["url"]

        worksheet_name = st.secrets["gsheets"]["worksheet_name"]



        gc = gspread.service_account_from_dict(creds_dict)

        sh = gc.open_by_url(gsheets_url)

        worksheet = sh.worksheet(worksheet_name)



        # Converte as datas para string antes de enviar, pois JSON não aceita datetime

        df_export = df_novos_dados.copy()

        df_export['Data'] = df_export['Data'].dt.strftime('%Y-%m-%d')

        

        # Converte para lista de listas (formato do gspread)

        # O Google Sheets espera tipos nativos do Python (int, float), não numpy types

        dados_lista = df_export.astype(object).values.tolist()

        

        worksheet.append_rows(dados_lista)

        return True

    except Exception as e:

        st.error(f"Erro ao salvar dados no Google Sheets: {e}")

        return False



def gerar_dados_proximo_dia(df_atual):
    """
    Gera transações fictícias baseadas nas regras do usuário para o dia seguinte ao último registro.
    Formatação numérica: Ponto para decimais, 2 casas fixas, sem separador de milhar (ex: 1250.50).
    """
    if df_atual.empty:
        ultimo_dia = datetime.date.today()
    else:
        ultimo_dia = df_atual['Data'].max().date()
    
    proximo_dia = ultimo_dia + datetime.timedelta(days=1)
    
    # Define a quantidade de vendas para o dia (entre 100 e 300 para variar)
    qtd_transacoes = random.randint(100, 300)
    
    novas_linhas = []
    
    # Listas de possibilidades baseadas nas regras
    cidades = ['Rio de Janeiro', 'São Paulo', 'Manaus']
    tipos_cliente = ['Normal', 'Membro']
    generos = ['Homem', 'Mulher']
    linhas_produto = [
        'Saude e Beleza', 'Acessorios Eletronicos', 'Casa e Estilo de Vida',
        'Esportes e Viagens', 'Moda'
    ]
    pagamentos = ['Pix', 'Cartao de Credito', 'Debito']
    
    for _ in range(qtd_transacoes):
        # 1. Invoice ID: XXX-XX-XXXX
        invoice_id = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
        
        # 2. City
        city = random.choice(cidades)
        
        # 3. Customer type
        customer_type = random.choice(tipos_cliente)
        
        # 4. Gender
        gender = random.choice(generos)
        
        # 5. Product line
        product_line = random.choice(linhas_produto)
        
        # 6. Unit price: 10.00 a 130.00 (Formatado como string "0.00")
        unit_price_float = random.uniform(10.00, 130.00)
        unit_price_str = f"{unit_price_float:.2f}"
        
        # 7. Quantity: 1 a 15
        quantity = random.randint(1, 15)
        
        # 8. Total (Formatado como string "0.00")
        total_float = unit_price_float * quantity
        total_str = f"{total_float:.2f}"
        
        # 9. Time: 07:00 a 23:00
        hora = random.randint(7, 23)
        minuto = random.randint(0, 59)
        time_str = f"{hora:02d}:{minuto:02d}"
        
        # 10. Payment
        payment = random.choice(pagamentos)
        
        # 11. Rating: 3.0 a 10.0
        rating = round(random.uniform(3.0, 10.0), 1)
        
        # 12. Data
        data_registro = pd.to_datetime(proximo_dia)
        
        # Adiciona ao dicionário
        linha = {
            "Invoice ID": invoice_id,
            "City": city,
            "Customer type": customer_type,
            "Gender": gender,
            "Product line": product_line,
            "Unit price": unit_price_str, # Vai salvar como "50.00"
            "Quantity": int(quantity),
            "Total": total_str,           # Vai salvar como "1000.00"
            "Time": time_str,
            "Payment": payment,
            "Rating": rating,
            "Data": data_registro
        }
        novas_linhas.append(linha)
        
    return pd.DataFrame(novas_linhas)


# --- CONFIGURAÇÃO DA PÁGINA ---



st.set_page_config(layout="wide", page_title="Dashboard de Vendas")

def relatorio_por_dia_com_variacoes(dia, data_df):
    if isinstance(dia, (pd.Timestamp, datetime.datetime)):
        dia_date = dia.date()
    else:
        dia_date = dia

    dia_timestamp = pd.to_datetime(dia_date)
    dia_anterior_timestamp = dia_timestamp - pd.Timedelta(days=1)

    df_dia = data_df[data_df['Data'].dt.date == dia_date].copy()
    df_dia_anterior = data_df[data_df['Data'].dt.date == dia_anterior_timestamp.date()].copy()

    if df_dia.empty:
        return {}
    
    # Limpeza básica
    for col in ['City', 'Customer type', 'Gender', 'Product line', 'Payment']:
        if col in df_dia.columns:
            df_dia = df_dia[~df_dia[col].astype(str).str.lower().isin(['total', 'quantity'])]
        if col in df_dia_anterior.columns:
            df_dia_anterior = df_dia_anterior[~df_dia_anterior[col].astype(str).str.lower().isin(['total', 'quantity'])]

    if df_dia.empty:
        return {}

    is_first_day_with_data = df_dia_anterior.empty and not df_dia.empty

    # --- HELPER: Soma (já existia) ---
    def calcular_totais_e_variacao(df_atual, df_anterior, coluna_agrupadora):
        total_atual = df_atual.groupby(coluna_agrupadora)[['Total', 'Quantity']].sum()
        
        if is_first_day_with_data:
            variacao = total_atual.copy()
            variacao[:] = pd.NA 
            return total_atual, variacao
        else:
            total_anterior = df_anterior.groupby(coluna_agrupadora)[['Total', 'Quantity']].sum()
            base_index = total_atual.index.union(total_anterior.index)
            total_atual_reindex = total_atual.reindex(base_index, fill_value=0)
            total_anterior_reindex = total_anterior.reindex(base_index, fill_value=0)
            variacao = total_atual_reindex - total_anterior_reindex
            return total_atual_reindex, variacao

    # --- HELPER: Crosstab (já existia) ---
    def calcular_crosstab_e_variacao(df_atual, df_anterior, index_cols, col_cols):
        atual = df_atual.groupby(index_cols)[col_cols].value_counts().unstack(fill_value=0)
        if is_first_day_with_data:
            variacao = atual.applymap(lambda x: pd.NA)
            return atual, variacao
        else:
            anterior = df_anterior.groupby(index_cols)[col_cols].value_counts().unstack(fill_value=0)
            idx = atual.index.union(anterior.index)
            cols = atual.columns.union(anterior.columns)
            atual_reindex = atual.reindex(index=idx, columns=cols, fill_value=0)
            anterior_reindex = anterior.reindex(index=idx, columns=cols, fill_value=0)
            variacao = atual_reindex - anterior_reindex
            return atual, variacao
            
    # --- NOVO HELPER: Média (Rating, Ticket Medio) ---
    def calcular_media_e_variacao(df_atual, df_anterior, coluna_agrupadora, coluna_valor, nome_metrica):
        # Calcula média atual
        media_atual = df_atual.groupby(coluna_agrupadora)[[coluna_valor]].mean()
        media_atual.columns = [nome_metrica]
        
        if is_first_day_with_data:
            variacao = media_atual.copy()
            variacao[:] = pd.NA
            return media_atual, variacao
        else:
            media_anterior = df_anterior.groupby(coluna_agrupadora)[[coluna_valor]].mean()
            media_anterior.columns = [nome_metrica]
            
            base_index = media_atual.index.union(media_anterior.index)
            media_atual_reindex = media_atual.reindex(base_index, fill_value=0)
            media_anterior_reindex = media_anterior.reindex(base_index, fill_value=0)
            
            variacao = media_atual_reindex - media_anterior_reindex
            return media_atual_reindex, variacao

    # --- NOVO HELPER: Hora ---
    def extrair_hora_e_agrupar(df_in):
        if df_in.empty: return pd.DataFrame()
        df_temp = df_in.copy()
        # Extrai a hora do formato "HH:MM"
        df_temp['Hora'] = df_temp['Time'].astype(str).str.split(':').str[0].astype(int)
        return df_temp.groupby('Hora')[['Total']].sum()

    # --- CÁLCULOS ANTIGOS ---
    total_por_cidade, variacao_cidade = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'City')
    total_por_tipo_cliente, variacao_tipo_cliente = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Customer type')
    total_por_genero, variacao_genero = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Gender')
    total_por_linha_produto, variacao_linha_produto = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Product line')
    total_por_payment, variacao_payment = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Payment')

    crosstab_cidade_tipo_cliente, variacao_cidade_tipo_cliente = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, 'City', 'Customer type')
    crosstab_cidade_genero, variacao_cidade_genero = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, ['City', 'Gender'], 'Customer type')
    crosstab_cidade_product, variacao_cidade_product = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, 'City', 'Product line')
    crosstab_cidade_payment, variacao_cidade_payment = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, ['City', 'Payment'], 'Gender')

    # --- CÁLCULOS NOVOS ---
    
    # A. Ticket Médio (Total agrupado por Cidade)
    ticket_medio_cidade, var_ticket_medio_cidade = calcular_media_e_variacao(df_dia, df_dia_anterior, 'City', 'Total', 'Ticket Médio')
    
    # B. Análise Temporal (Soma de Total por Hora)
    vendas_hora_atual = extrair_hora_e_agrupar(df_dia)
    if is_first_day_with_data:
         var_vendas_hora = vendas_hora_atual.copy()
         var_vendas_hora[:] = pd.NA
    else:
         vendas_hora_anterior = extrair_hora_e_agrupar(df_dia_anterior)
         idx_h = vendas_hora_atual.index.union(vendas_hora_anterior.index)
         atual_h = vendas_hora_atual.reindex(idx_h, fill_value=0)
         ant_h = vendas_hora_anterior.reindex(idx_h, fill_value=0)
         vendas_hora_atual = atual_h # Atualiza para ter index completo
         var_vendas_hora = atual_h - ant_h

    # C. Qualidade e Satisfação (Rating por Linha de Produto)
    rating_produto, var_rating_produto = calcular_media_e_variacao(df_dia, df_dia_anterior, 'Product line', 'Rating', 'Média Rating')
    
    # D. Eficiência de Pagamento (Rating por Método de Pagamento)
    rating_pagamento, var_rating_pagamento = calcular_media_e_variacao(df_dia, df_dia_anterior, 'Payment', 'Rating', 'Média Rating')

    return {
        # Antigos
        "total_por_cidade": total_por_cidade, "variacao_cidade": variacao_cidade,
        "total_por_tipo_cliente": total_por_tipo_cliente, "variacao_tipo_cliente": variacao_tipo_cliente,
        "total_por_genero": total_por_genero, "variacao_genero": variacao_genero,
        "total_por_linha_produto": total_por_linha_produto, "variacao_linha_produto": variacao_linha_produto,
        "total_por_payment": total_por_payment, "variacao_payment": variacao_payment,
        "crosstab_cidade_tipo_cliente": crosstab_cidade_tipo_cliente, "variacao_cidade_tipo_cliente": variacao_cidade_tipo_cliente,
        "crosstab_cidade_genero": crosstab_cidade_genero, "variacao_cidade_genero": variacao_cidade_genero,
        "crosstab_cidade_product": crosstab_cidade_product, "variacao_cidade_product": variacao_cidade_product,
        "crosstab_cidade_payment": crosstab_cidade_payment, "variacao_cidade_payment": variacao_cidade_payment,
        
        # Novos
        "ticket_medio_cidade": ticket_medio_cidade, "var_ticket_medio_cidade": var_ticket_medio_cidade,
        "vendas_por_hora": vendas_hora_atual, "var_vendas_por_hora": var_vendas_hora,
        "rating_produto": rating_produto, "var_rating_produto": var_rating_produto,
        "rating_pagamento": rating_pagamento, "var_rating_pagamento": var_rating_pagamento
    }

# Carregando os dados da planilha

df = load_data_from_gsheets()

# BLOCO DE API: INTERCEPTA O n8n E DEVOLVE DATA + VARIAÇÃO

# =================================================================

if "request_type" in st.query_params:
    request_type = st.query_params.get("request_type")
    target_date = st.query_params.get("target_date")
    report_name = st.query_params.get("report_name")

    if request_type == "get_report" and target_date and report_name:
        relatorio_api = relatorio_por_dia_com_variacoes(pd.to_datetime(target_date), df)
        
        if not relatorio_api:
            st.json({"erro": "Nenhum dado encontrado para a data informada."})
            st.stop()

        # MAPEAMENTO COMPLETO
        mapping = {
            # --- Relatórios de Soma (Vendas e Quantidade) ---
            "total_por_cidade": ("total_por_cidade", "variacao_cidade", "sum"),
            "total_por_linha_produto": ("total_por_linha_produto", "variacao_linha_produto", "sum"),
            "total_por_tipo_cliente": ("total_por_tipo_cliente", "variacao_tipo_cliente", "sum"),
            "total_por_payment": ("total_por_payment", "variacao_payment", "sum"),
            "total_por_genero": ("total_por_genero", "variacao_genero", "sum"),
            "vendas_por_hora": ("vendas_por_hora", "var_vendas_por_hora", "sum"),
            
            # --- Relatórios de Distribuição (Crosstabs) ---
            "distribuicao_cidade_tipo": ("crosstab_cidade_tipo_cliente", "variacao_cidade_tipo_cliente", "cross"),
            "distribuicao_cidade_genero_tipo": ("crosstab_cidade_genero", "variacao_cidade_genero", "cross"),
            
            # --- NOVOS: Relatórios de Métrica Única ---
            "ticket_medio_cidade": ("ticket_medio_cidade", "var_ticket_medio_cidade", "metric"),
            "rating_produto": ("rating_produto", "var_rating_produto", "metric"),
            "rating_pagamento": ("rating_pagamento", "var_rating_pagamento", "metric")
        }

        if report_name in mapping:
            key_data, key_var, report_type = mapping[report_name]
            
            df_main = relatorio_api[key_data]
            df_var = relatorio_api[key_var]

            # 1. CONSTRUÇÃO DO DATAFRAME
            if report_type == "sum":
                df_final = pd.concat([
                    df_main, 
                    df_var.rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})
                ], axis=1)
                
            elif report_type == "metric":
                df_final = pd.concat([
                    df_main,
                    df_var.add_prefix("Var. ")
                ], axis=1)
                
            else: # cross
                df_final = pd.concat([
                    df_main, 
                    df_var.add_suffix(" (Var)")
                ], axis=1).fillna(0)

            # 2. NORMALIZAÇÃO (ARREDONDAMENTO) PARA IGUALAR AO APP
            # ====================================================
            if report_type == "sum":
                # Arredonda colunas de Dinheiro (Total) para 2 casas
                cols_money = [c for c in df_final.columns if "Total" in c]
                df_final[cols_money] = df_final[cols_money].round(2)
                
                # Converte colunas de Quantidade para Inteiro (sem decimal)
                cols_qty = [c for c in df_final.columns if "Quantity" in c]
                df_final[cols_qty] = df_final[cols_qty].fillna(0).astype(int)

            elif report_type == "metric":
                # Verifica se é Rating (1 casa decimal) ou Ticket (2 casas decimais)
                # Procura por 'Rating' em qualquer coluna
                is_rating = any("Rating" in c for c in df_final.columns)
                decimals = 1 if is_rating else 2
                df_final = df_final.round(decimals)

            else: # cross
                # Distribuições são sempre números inteiros
                df_final = df_final.fillna(0).astype(int)

            # 3. ENVIO DO JSON
            st.json(df_final.fillna(0).reset_index().to_dict(orient="records"))
            st.stop()
        else:
            st.json({"erro": f"Relatório '{report_name}' não encontrado no mapeamento."})
            st.stop()

# =================================================================



# --- BARRA LATERAL (AÇÕES E SELEÇÃO) ---



st.sidebar.title("Menu de Ações")



# Botão de Gerar Dados

st.sidebar.markdown("### 📅 Simulação")

if st.sidebar.button("Gerar Próximo Dia de Vendas", type="primary"):

    with st.spinner("Gerando dados e salvando no Google Sheets..."):

        # 1. Gerar dados

        novos_dados = gerar_dados_proximo_dia(df)

        

        # 2. Salvar dados

        sucesso = salvar_dados_gsheets(novos_dados)

        

        if sucesso:

            st.sidebar.success(f"Sucesso! Dia {novos_dados['Data'].dt.date.iloc[0]} gerado.")

            # Limpa o cache para forçar o recarregamento dos dados

            st.cache_data.clear()

            st.rerun()

        else:

            st.sidebar.error("Falha ao salvar os dados.")



st.sidebar.markdown("---")





# --- LÓGICA DE RELATÓRIOS (ORIGINAL ADAPTADA) ---






# --- INTERFACE PRINCIPAL ---



st.title("Relatório Diário de Vendas com Alertas no Whatsapp")



if df.empty or 'Data' not in df.columns:

    st.info("O DataFrame está vazio ou a coluna 'Data' não foi encontrada. Verifique sua planilha.")

    st.stop()



# Seleção do dia na Sidebar

dias_unicos = df['Data'].dt.date.unique()

dias_unicos_ordenados = sorted(dias_unicos, reverse=True)



if not dias_unicos_ordenados:

    st.info("Não há datas válidas para seleção.")

    st.stop()



dia_selecionado = st.sidebar.selectbox("Selecione uma data para visualizar", dias_unicos_ordenados)

primeiro_dia_disponivel = dias_unicos_ordenados[-1] 



relatorio = relatorio_por_dia_com_variacoes(dia_selecionado, df)



if not relatorio:

    st.info(f"Não há dados de vendas para o dia {dia_selecionado}.")

    st.stop()



# --- ALERTAS ---

alertas_positivos = []

alertas_negativos = []



cidades_acima_30000 = relatorio['total_por_cidade'][relatorio['total_por_cidade']['Total'] > 30000]

if not cidades_acima_30000.empty:

    cidades_str = ", ".join(cidades_acima_30000.index)

    alertas_positivos.append(f"As cidades **{cidades_str}** ultrapassaram R$30.000 em vendas totais.")



total_atual_cidade = relatorio['total_por_cidade']['Total']

variacao_cidade_abs = relatorio['variacao_cidade']['Total']

total_anterior_cidade = total_atual_cidade - variacao_cidade_abs

valid_indices = total_anterior_cidade[total_anterior_cidade > 0].index



if not valid_indices.empty:

    variacao_percentual_cidade = (variacao_cidade_abs.loc[valid_indices] / total_anterior_cidade.loc[valid_indices]) * 100

    cidades_queda = variacao_percentual_cidade[variacao_percentual_cidade < -30]

    if not cidades_queda.empty:

        cidades_str = ", ".join(cidades_queda.index)

        alertas_negativos.append(f"As cidades **{cidades_str}** tiveram uma queda superior a 30% nas vendas.")



if "Pix" in relatorio['total_por_payment'].index:

    total_pix = relatorio['total_por_payment'].loc["Pix", "Total"]

    if "Pix" in relatorio['variacao_payment'].index:

        variacao_pix = relatorio['variacao_payment'].loc["Pix", "Total"]

    else:

        variacao_pix = 0

    if pd.notna(variacao_pix):

        total_anterior_pix = total_pix - variacao_pix

        if total_anterior_pix > 0:

            variacao_perc = (variacao_pix / total_anterior_pix) * 100

            if variacao_perc > 30:

                alertas_positivos.append(f"O método de pagamento **Pix** apresentou um aumento superior a 30% ({variacao_perc:.1f}%) nas vendas.")



produtos_acima_400 = relatorio['total_por_linha_produto'][relatorio['total_por_linha_produto']['Quantity'] > 400]

if not produtos_acima_400.empty:

    produtos_str = ", ".join(produtos_acima_400.index)

    alertas_positivos.append(f"Os produtos **{produtos_str}** tiveram mais de 400 vendas.")



st.sidebar.subheader("Alertas do Dia")

total_alertas = len(alertas_positivos) + len(alertas_negativos)



if total_alertas > 0:

    st.sidebar.error(f"🚨 {total_alertas} ALERTAS ENCONTRADOS")

else:

    st.sidebar.info("Não há alertas para o dia selecionado.")



with st.expander("Alertas Importantes", expanded=True if total_alertas > 0 else False, icon="🚨"):

    for alerta in alertas_positivos:

        st.success(alerta)

    for alerta in alertas_negativos:

        st.error(alerta)

    if not alertas_positivos and not alertas_negativos:

        st.info("Nenhum alerta foi gerado para o dia selecionado.")



st.subheader(f"Relatório Detalhado de Vendas para o dia {dia_selecionado}")
st.markdown("---")

# Funções de Estilo e Plotagem
def style_dataframe(df_input):
    is_first_day = (dia_selecionado == primeiro_dia_disponivel)
    format_dict = {"Total": "R${:.2f}", "Quantity": "{:.0f}"}
    var_format_dict = {"Var. Total": "R${:+.2f}", "Var. Quantity": "{:+.0f}"}
    if is_first_day:
        var_format_dict = {
            "Var. Total": lambda x: "N/A" if pd.isna(x) else ("R${:+.2f}".format(x) if pd.notna(x) else "-"),
            "Var. Quantity": lambda x: "N/A" if pd.isna(x) else ("{:+.0f}".format(x) if pd.notna(x) else "-")
        }
    format_dict.update(var_format_dict)
    return df_input.style.format(format_dict, na_rep="-")

def style_generic(df_input, col_name, format_str):
    """Estiliza tabelas genéricas (Ticket Medio, Rating, etc)"""
    is_first_day = (dia_selecionado == primeiro_dia_disponivel)
    col_var = f"Var. {col_name}"
    
    format_dict = {col_name: format_str}
    
    if is_first_day:
        format_dict[col_var] = lambda x: "N/A" if pd.isna(x) else (format_str.replace("{:", "{:+").format(x) if pd.notna(x) else "-")
    else:
        format_dict[col_var] = format_str.replace("{:", "{:+") # Adiciona sinal + para variação
        
    return df_input.style.format(format_dict, na_rep="-")

def plot_total_and_variation(df_total, df_var, id_col, title):
    df_var_renamed = df_var.rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})
    df_concat = pd.concat([df_total.round(2), df_var_renamed.round(2)], axis=1).reset_index()
    
    if 'index' in df_concat.columns:
        df_concat.rename(columns={'index': id_col}, inplace=True)
        
    df_plot = df_concat.melt(
        id_vars=id_col, 
        value_vars=['Total', 'Var. Total', 'Quantity', 'Var. Quantity'], 
        var_name='Métrica', 
        value_name='Valor'
    ).dropna(subset=['Valor'])
    
    color_map = {
        'Total': 'rgb(76, 120, 168)', 'Quantity': 'rgb(30, 60, 100)',
        'Var. Total': 'rgb(228, 87, 86)', 'Var. Quantity': 'rgb(190, 40, 40)'
    }

    fig = px.bar(
        df_plot, x=id_col, y='Valor', color='Métrica', barmode='group', 
        title=f"{title}",
        template='plotly_white', color_discrete_map=color_map,
        labels={'Métrica': 'Variável'}
    )
    fig.update_layout(height=400, title_x=0.5, margin=dict(l=20, r=20, t=40, b=20))
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    return fig

def plot_generic(df_main, df_var, id_col, val_col, title, color_main='rgb(50, 168, 82)', color_var='rgb(50, 168, 140)'):
    """Plotter genérico para métricas únicas (Rating, Ticket Médio, etc)"""
    col_var_name = f"Var. {val_col}"
    df_var_renamed = df_var.rename(columns={val_col: col_var_name})
    df_concat = pd.concat([df_main, df_var_renamed], axis=1).reset_index()
    
    if 'index' in df_concat.columns:
        df_concat.rename(columns={'index': id_col}, inplace=True)
        
    df_plot = df_concat.melt(
        id_vars=id_col, 
        value_vars=[val_col, col_var_name], 
        var_name='Métrica', 
        value_name='Valor'
    ).dropna(subset=['Valor'])

    color_map = {val_col: color_main, col_var_name: color_var}

    fig = px.bar(
        df_plot, x=id_col, y='Valor', color='Métrica', barmode='group', 
        title=title, template='plotly_white', color_discrete_map=color_map
    )
    fig.update_layout(height=400, title_x=0.5, margin=dict(l=20, r=20, t=40, b=20))
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    return fig

# --- DISTRIBUIÇÃO DAS COLUNAS ---
col1, col2 = st.columns(2)

with col1:
    # 1. Total por Cidade
    st.markdown("##### Total de Vendas por Cidade e Variação:")
    df_cidade_concat = pd.concat([relatorio['total_por_cidade'].round(2), relatorio['variacao_cidade'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_cidade_concat), use_container_width=True)
    with st.expander("📊 Gráfico: Vendas por Cidade"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_cidade'], relatorio['variacao_cidade'], 'City', "Métricas por Cidade"), use_container_width=True)

    # 2. Total por Tipo de Cliente
    st.markdown("##### Total de vendas por Tipo de Cliente:")
    df_cliente_concat = pd.concat([relatorio['total_por_tipo_cliente'].round(2), relatorio['variacao_tipo_cliente'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_cliente_concat), use_container_width=True)
    with st.expander("📊 Gráfico: Vendas por Tipo de Cliente"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_tipo_cliente'], relatorio['variacao_tipo_cliente'], 'Customer type', "Métricas por Tipo"), use_container_width=True)

    # 3. Total por Gênero
    st.markdown("##### Total de vendas por Gênero:")
    df_genero_concat = pd.concat([relatorio['total_por_genero'].round(2), relatorio['variacao_genero'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_genero_concat), use_container_width=True)
    with st.expander("📊 Gráfico: Vendas por Gênero"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_genero'], relatorio['variacao_genero'], 'Gender', "Métricas por Gênero"), use_container_width=True)

    # --- NOVOS RELATÓRIOS COLUNA 1 ---
    st.markdown("---")
    
    
    # A. Ticket Médio
    st.markdown("##### 💵 Ticket Médio por Cidade (Average Order Value)")
    df_ticket = pd.concat([relatorio['ticket_medio_cidade'], relatorio['var_ticket_medio_cidade'].rename(columns={'Ticket Médio': 'Var. Ticket Médio'})], axis=1)
    st.dataframe(style_generic(df_ticket, "Ticket Médio", "R${:.2f}"), use_container_width=True)
    with st.expander("📊 Gráfico: Ticket Médio por Cidade"):
        st.plotly_chart(plot_generic(relatorio['ticket_medio_cidade'], relatorio['var_ticket_medio_cidade'], 'City', 'Ticket Médio', "Ticket Médio por Cidade", color_main='#FF9900', color_var='#CC7A00'), use_container_width=True)

    # C. Qualidade (Rating por Produto)
    st.markdown("##### ⭐ Qualidade e Satisfação (Rating por Produto)")
    df_rating_prod = pd.concat([relatorio['rating_produto'], relatorio['var_rating_produto'].rename(columns={'Média Rating': 'Var. Média Rating'})], axis=1)
    st.dataframe(style_generic(df_rating_prod, "Média Rating", "{:.1f}"), use_container_width=True)
    with st.expander("📊 Gráfico: Rating por Produto"):
        st.plotly_chart(plot_generic(relatorio['rating_produto'], relatorio['var_rating_produto'], 'Product line', 'Média Rating', "Média de Avaliação por Produto", color_main='#9900FF', color_var='#7A00CC'), use_container_width=True)


with col2:
    # 4. Total por Produto
    st.markdown("##### Total de vendas por Linha de Produto:")
    df_produto_concat = pd.concat([relatorio['total_por_linha_produto'].round(2), relatorio['variacao_linha_produto'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_produto_concat), use_container_width=True)
    with st.expander("📊 Gráfico: Vendas por Produto"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_linha_produto'], relatorio['variacao_linha_produto'], 'Product line', "Métricas por Linha de Produto"), use_container_width=True)

    # 5. Total por Pagamento
    st.markdown("##### Total de vendas por Método de Pagamento:")
    df_payment_concat = pd.concat([relatorio['total_por_payment'].round(2), relatorio['variacao_payment'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_payment_concat), use_container_width=True)
    with st.expander("📊 Gráfico: Vendas por Pagamento"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_payment'], relatorio['variacao_payment'], 'Payment', "Métricas por Pagamento"), use_container_width=True)

    # 6. Distribuição (Crosstabs)
    st.markdown("##### Distribuição: Clientes por Cidade e Tipo:")
    st.dataframe(pd.concat([relatorio["crosstab_cidade_tipo_cliente"], relatorio["variacao_cidade_tipo_cliente"].add_suffix(" (Var)")], axis=1).fillna(0).astype(int), use_container_width=True)
    with st.expander("📊 Gráfico: Distribuição Cruzada"):
        df_plot = relatorio["crosstab_cidade_tipo_cliente"].reset_index().melt(id_vars="City")
        st.plotly_chart(px.bar(df_plot, x="City", y="value", color="Customer type", barmode="group", title="Distribuição de Clientes", labels={'value': 'Clientes'}), use_container_width=True)
    
    # --- NOVOS RELATÓRIOS COLUNA 2 ---
    st.markdown("---")
    st.markdown("#### ⏳ Análise Temporal e Eficiência")

    # B. Horário de Pico
    st.markdown("##### ⏰ Análise Temporal (Vendas por Hora)")
    df_hora = pd.concat([relatorio['vendas_por_hora'], relatorio['var_vendas_por_hora'].rename(columns={'Total': 'Var. Total'})], axis=1)
    st.dataframe(style_generic(df_hora, "Total", "R${:.2f}"), use_container_width=True)
    with st.expander("📊 Gráfico: Horários de Pico"):
        # Usamos um gráfico de linha ou área para tempo, mas barras funcionam bem aqui também
        fig_hora = plot_generic(relatorio['vendas_por_hora'], relatorio['var_vendas_por_hora'], 'Hora', 'Total', "Vendas Totais por Hora do Dia", color_main='#00CC99', color_var='#009973')
        fig_hora.update_xaxes(dtick=1) # Mostrar todas as horas
        st.plotly_chart(fig_hora, use_container_width=True)

    # D. Eficiência Pagamento (Rating)
    st.markdown("##### 💳 Eficiência de Pagamento (Rating por Método)")
    df_rating_pay = pd.concat([relatorio['rating_pagamento'], relatorio['var_rating_pagamento'].rename(columns={'Média Rating': 'Var. Média Rating'})], axis=1)
    st.dataframe(style_generic(df_rating_pay, "Média Rating", "{:.1f}"), use_container_width=True)
    with st.expander("📊 Gráfico: Rating por Pagamento"):
        st.plotly_chart(plot_generic(relatorio['rating_pagamento'], relatorio['var_rating_pagamento'], 'Payment', 'Média Rating', "Satisfação por Forma de Pagamento", color_main='#FF3366', color_var='#CC0033'), use_container_width=True)
