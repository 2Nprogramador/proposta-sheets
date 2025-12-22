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

    """

    Carrega os dados da Google Sheet usando as credenciais da Service Account.

    """

    # st.info("Carregando dados...") # Opcional: manter comentado para não poluir visualmente

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



        df_sheet.dropna(subset=['Data', 'Total', 'Quantity'], inplace=True)

        df_sheet = df_sheet[df_sheet['Total'] > 0]

        df_sheet.dropna(subset=['Data'], inplace=True)

        

        return df_sheet



    except Exception as e:

        st.error(f"Erro ao carregar dados: {e}")

        st.stop()

        return pd.DataFrame()


def processar_insights_criativos(df_dia):
    """
    Gera dataframes específicos para os novos insights baseados no dia selecionado.
    """
    if df_dia.empty:
        return None

    # 1. Tratamento de Hora (Extrair apenas a hora inteira)
    try:
        df_dia['Hora_Int'] = pd.to_datetime(df_dia['Time'], format='%H:%M').dt.hour
    except:
        df_dia['Hora_Int'] = 0
        
    # --- INSIGHT 1: Vendas por Hora ---
    vendas_por_hora = df_dia.groupby('Hora_Int')[['Total', 'Quantity']].sum().reset_index()
    
    # --- INSIGHT 2: Ticket Médio por Tipo de Cliente ---
    ticket_medio_tipo = df_dia.groupby('Customer type').agg(
        Faturamento=('Total', 'sum'),
        Transacoes=('Invoice ID', 'count')
    )
    ticket_medio_tipo['Ticket_Medio'] = ticket_medio_tipo['Faturamento'] / ticket_medio_tipo['Transacoes']
    ticket_medio_tipo = ticket_medio_tipo.reset_index()

    # --- INSIGHT 3: Rating vs Faturamento (Por Linha de Produto) ---
    rating_faturamento = df_dia.groupby('Product line').agg(
        Rating_Medio=('Rating', 'mean'),
        Faturamento=('Total', 'sum')
    ).reset_index()

    return {
        "vendas_por_hora": vendas_por_hora,
        "ticket_medio_tipo": ticket_medio_tipo,
        "rating_faturamento": rating_faturamento
    }


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

    """

    if df_atual.empty:

        ultimo_dia = datetime.date.today()

    else:

        ultimo_dia = df_atual['Data'].max().date()

    

    proximo_dia = ultimo_dia + datetime.timedelta(days=1)

    

    # Define a quantidade de vendas para o dia (entre 20 e 50 para variar)

    qtd_transacoes = random.randint(100, 300)

    

    novas_linhas = []

    

    # Listas de possibilidades baseadas nas regras

    cidades = ['Rio de Janeiro', 'São Paulo', 'Manaus']

    tipos_cliente = ['Normal', 'Membro']

    generos = ['Homem', 'Mulher']

    linhas_produto = [

        'Saude e Beleza', 'Acessorios Eletronicos', 'Casa e Estilo de Vida',

        'Esportes e Viagens', 'Moda', 'Alimentos e Bebidas'

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

        

        # 6. Unit price: 10.00 a 130.00

        unit_price = round(random.uniform(10.00, 130.00), 2)

        

        # 7. Quantity: 1 a 15

        quantity = random.randint(1, 15)

        

        # 8. Total

        total = round(unit_price * quantity, 2)

        

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

            "Unit price": unit_price,

            "Quantity": int(quantity),

            "Total": total,

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

    

    # Filtragem de linhas de 'total'/'quantity' se existirem no texto

    for col in ['City', 'Customer type', 'Gender', 'Product line', 'Payment']:

        if col in df_dia.columns:

            df_dia = df_dia[~df_dia[col].astype(str).str.lower().isin(['total', 'quantity'])]

        if col in df_dia_anterior.columns:

            df_dia_anterior = df_dia_anterior[~df_dia_anterior[col].astype(str).str.lower().isin(['total', 'quantity'])]



    if df_dia.empty:

        return {}



    is_first_day_with_data = df_dia_anterior.empty and not df_dia.empty



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



    total_por_cidade, variacao_cidade = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'City')

    total_por_tipo_cliente, variacao_tipo_cliente = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Customer type')

    total_por_genero, variacao_genero = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Gender')

    total_por_linha_produto, variacao_linha_produto = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Product line')

    total_por_payment, variacao_payment = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Payment')



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



    crosstab_cidade_tipo_cliente, variacao_cidade_tipo_cliente = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, 'City', 'Customer type')

    crosstab_cidade_genero, variacao_cidade_genero = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, ['City', 'Gender'], 'Customer type')

    crosstab_cidade_product, variacao_cidade_product = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, 'City', 'Product line')

    crosstab_cidade_payment, variacao_cidade_payment = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, ['City', 'Payment'], 'Gender')



    return {

        "total_por_cidade": total_por_cidade,

        "variacao_cidade": variacao_cidade,

        "total_por_tipo_cliente": total_por_tipo_cliente,

        "variacao_tipo_cliente": variacao_tipo_cliente,

        "total_por_genero": total_por_genero,

        "variacao_genero": variacao_genero,

        "total_por_linha_produto": total_por_linha_produto,

        "variacao_linha_produto": variacao_linha_produto,

        "total_por_payment": total_por_payment,

        "variacao_payment": variacao_payment,

        "crosstab_cidade_tipo_cliente": crosstab_cidade_tipo_cliente,

        "crosstab_cidade_genero": crosstab_cidade_genero,

        "crosstab_cidade_product": crosstab_cidade_product,

        "crosstab_cidade_payment": crosstab_cidade_payment,

        "variacao_cidade_tipo_cliente": variacao_cidade_tipo_cliente,

        "variacao_cidade_genero": variacao_cidade_genero,

        "variacao_cidade_product": variacao_cidade_product,

        "variacao_cidade_payment": variacao_cidade_payment,

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
        
        # 1. Prepara dados para os Relatórios Padrão
        relatorio_api = relatorio_por_dia_com_variacoes(pd.to_datetime(target_date), df)
        
        # 2. Prepara dados para os Novos Insights (Necessita filtrar o DF bruto para O DIA APENAS)
        target_dt = pd.to_datetime(target_date).date()
        df_api_raw = df[df['Data'].dt.date == target_dt] # AQUI GARANTE QUE É SÓ O DIA
        insights_api = processar_insights_criativos(df_api_raw)

        if not relatorio_api and not insights_api:
            st.json({"erro": "Nenhum dado encontrado para a data informada."})
            st.stop()

        # --- NOVA LÓGICA: Verifica se o pedido é um dos NOVOS insights ---
        novos_endpoints = ["vendas_por_hora", "ticket_medio_tipo", "rating_faturamento"]
        
        if report_name in novos_endpoints:
            if insights_api and report_name in insights_api:
                df_insight = insights_api[report_name]
                st.json(df_insight.to_dict(orient="records"))
                st.stop()
            else:
                st.json({"erro": f"Insight '{report_name}' não disponível para esta data."})
                st.stop()

        # --- LÓGICA ANTIGA: Relatórios Padrão ---
        mapping = {
            "total_por_cidade": ("total_por_cidade", "variacao_cidade", "sum"),
            "total_por_linha_produto": ("total_por_linha_produto", "variacao_linha_produto", "sum"),
            "total_por_tipo_cliente": ("total_por_tipo_cliente", "variacao_tipo_cliente", "sum"),
            "total_por_payment": ("total_por_payment", "variacao_payment", "sum"),
            "total_por_genero": ("total_por_genero", "variacao_genero", "sum"),
            "distribuicao_cidade_tipo": ("crosstab_cidade_tipo_cliente", "variacao_cidade_tipo_cliente", "cross"),
            "distribuicao_cidade_genero_tipo": ("crosstab_cidade_genero", "variacao_cidade_genero", "cross")
        }

        if report_name in mapping:
            key_data, key_var, report_type = mapping[report_name]
            df_main = relatorio_api[key_data]
            df_var = relatorio_api[key_var]

            if report_type == "sum":
                df_final = pd.concat([
                    df_main, 
                    df_var.rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})
                ], axis=1)
            else:
                df_final = pd.concat([
                    df_main, 
                    df_var.add_suffix(" (Var)")
                ], axis=1).fillna(0)

            st.json(df_final.reset_index().to_dict(orient="records"))
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



st.title("Relatório Diário de Vendas Completo com Alertas no Whatsapp")



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



# --- PLOTS ---

# --- PLOTS E INSIGHTS ---

st.subheader(f"Relatório Detalhado de Vendas para o dia {dia_selecionado}")

# 1. CÁLCULO PRÉVIO DOS INSIGHTS (Para estarem disponíveis nas colunas)
df_dia_raw = df[df['Data'].dt.date == dia_selecionado]
insights = processar_insights_criativos(df_dia_raw)

col1, col2 = st.columns(2)

# Função de estilo existente (mantida para os dados padrões)
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

# Função de plotagem existente
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
        title=f"{title} - Total, Quantidade e Variação",
        template='plotly_white', color_discrete_map=color_map,
        labels={'Métrica': 'Variável'}
    )
    fig.update_layout(height=450, title_x=0.5)
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    return fig

# --- COLUNA 1 ---
with col1:
    # 1. Cidade
    st.markdown("##### Total de Vendas por Cidade e Variação:")
    df_cidade_concat = pd.concat([relatorio['total_por_cidade'].round(2), relatorio['variacao_cidade'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_cidade_concat), use_container_width=True)
    with st.expander("Gráfico de Vendas e Quantidades por Cidade"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_cidade'].round(2), relatorio['variacao_cidade'].round(2), 'City', "Métricas por Cidade"), use_container_width=True)

    # 2. Tipo de Cliente
    st.markdown("##### Total de vendas por Tipo de Cliente e Variação:")
    df_cliente_concat = pd.concat([relatorio['total_por_tipo_cliente'].round(2), relatorio['variacao_tipo_cliente'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_cliente_concat), use_container_width=True)
    with st.expander("Gráfico de Vendas e Quantidades por Tipo de Cliente"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_tipo_cliente'].round(2), relatorio['variacao_tipo_cliente'].round(2), 'Customer type', "Métricas por Tipo de Cliente"), use_container_width=True)

    # 3. Gênero
    st.markdown("##### Total de vendas por Gênero e Variação:")
    df_genero_concat = pd.concat([relatorio['total_por_genero'].round(2), relatorio['variacao_genero'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_genero_concat), use_container_width=True)
    with st.expander("Gráfico de Vendas e Quantidades por Gênero"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_genero'].round(2), relatorio['variacao_genero'].round(2), 'Gender', "Métricas por Gênero"), use_container_width=True)

    # 4. Crosstab (Cidade x Gênero x Tipo)
    st.markdown("##### Distribuição de Clientes por Cidade, Gênero e Tipo:")
    st.dataframe(pd.concat([relatorio["crosstab_cidade_genero"], relatorio["variacao_cidade_genero"].add_suffix(" (Var)")], axis=1).fillna(0).astype(int), use_container_width=True)
    with st.expander("Gráfico de Distribuição de Clientes por Cidade, Gênero e Tipo"):
        df_plot = relatorio['crosstab_cidade_genero'].stack(level=0).reset_index().rename(columns={0: 'count'})
        st.plotly_chart(px.bar(df_plot, x='City', y='count', color='Customer type', facet_col='Gender', barmode='group', title='Distribuição de Clientes por Cidade, Gênero e Tipo', labels={'count': 'Número de Clientes'}), use_container_width=True)

    # === NOVO INSIGHT 1: Golden Hours (Adicionado na Coluna 1) ===
    if insights:
        st.markdown("---")
        st.markdown("##### ⏰ Golden Hours (Vendas x Hora)")
        # Tabela Formatada
        df_gh = insights['vendas_por_hora'].set_index('Hora_Int')
        st.dataframe(df_gh.style.format({"Total": "R${:.2f}", "Quantity": "{:.0f}"}), use_container_width=True)
        # Expander com Gráfico
        with st.expander("Ver Gráfico de Horários"):
            fig_hora = px.area(
                insights['vendas_por_hora'], 
                x='Hora_Int', 
                y='Total', 
                markers=True,
                title="Volume de Vendas ao longo do dia",
                labels={'Hora_Int': 'Hora do Dia', 'Total': 'Faturamento (R$)'},
                color_discrete_sequence=['#FF4B4B']
            )
            fig_hora.update_xaxes(tickmode='linear', dtick=1)
            st.plotly_chart(fig_hora, use_container_width=True)

# --- COLUNA 2 ---
with col2:
    # 1. Produto
    st.markdown("##### Total de vendas por Linha de Produto e Variação:")
    df_produto_concat = pd.concat([relatorio['total_por_linha_produto'].round(2), relatorio['variacao_linha_produto'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_produto_concat), use_container_width=True)
    with st.expander("Gráfico de Vendas e Quantidades por Linha de Produto"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_linha_produto'].round(2), relatorio['variacao_linha_produto'].round(2), 'Product line', "Métricas por Linha de Produto"), use_container_width=True)

    # 2. Pagamento
    st.markdown("##### Total de vendas por Método de Pagamento e Variação:")
    df_payment_concat = pd.concat([relatorio['total_por_payment'].round(2), relatorio['variacao_payment'].round(2).rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_payment_concat), use_container_width=True)
    with st.expander("Gráfico de Vendas e Quantidades por Método de Pagamento"):
        st.plotly_chart(plot_total_and_variation(relatorio['total_por_payment'].round(2), relatorio['variacao_payment'].round(2), 'Payment', "Métricas por Método de Pagamento"), use_container_width=True)

    # 3. Crosstab (Cidade x Tipo)
    st.markdown("##### Distribuição de Clientes por Cidade e Tipo:")
    st.dataframe(pd.concat([relatorio["crosstab_cidade_tipo_cliente"], relatorio["variacao_cidade_tipo_cliente"].add_suffix(" (Var)")], axis=1).fillna(0).astype(int), use_container_width=True)
    with st.expander("Gráfico de Distribuição de Clientes por Cidade e Tipo"):
        df_plot = relatorio["crosstab_cidade_tipo_cliente"].reset_index().melt(id_vars="City")
        st.plotly_chart(px.bar(df_plot, x="City", y="value", color="Customer type", barmode="group", title="Distribuição de Clientes por Cidade e Tipo", labels={'value': 'Número de Clientes'}), use_container_width=True)
    
    # 4. Crosstab (Pagamento x Gênero)
    st.markdown("##### Distribuição de Clientes por Cidade, Pagamento e Gênero:")
    st.dataframe(pd.concat([relatorio["crosstab_cidade_payment"], relatorio["variacao_cidade_payment"].add_suffix(" (Var)")], axis=1).fillna(0).astype(int), use_container_width=True)
    with st.expander("Distribuição de Clientes por Cidade, Pagamento e Gênero"):
        df_plot = relatorio['crosstab_cidade_payment'].stack(level=0).reset_index().rename(columns={0: 'count'})
        fig = px.bar(df_plot, x='City', y='count', color='Gender', facet_col='Payment', barmode='group', title='Distribuição de Clientes por Cidade, Gênero e Forma de Pagamento', labels={'count': 'Número de Clientes'}, template='plotly_dark')
        fig.update_xaxes(tickangle=45)
        fig.update_layout(height=445)
        st.plotly_chart(fig, use_container_width=True)

    # === NOVO INSIGHT 2: Ticket Médio (Adicionado na Coluna 2) ===
    if insights:
        st.markdown("---")
        st.markdown("##### 💳 Ticket Médio: Membro vs Normal")
        # Tabela Formatada
        df_ticket = insights['ticket_medio_tipo'].set_index('Customer type')
        st.dataframe(df_ticket.style.format({'Faturamento': 'R${:.2f}', 'Ticket_Medio': 'R${:.2f}'}), use_container_width=True)
        # Expander com Gráfico
        with st.expander("Ver Gráfico de Ticket Médio"):
            fig_ticket = px.bar(
                insights['ticket_medio_tipo'], 
                x='Customer type', 
                y='Ticket_Medio',
                color='Customer type',
                text_auto='.2f',
                title="Quem gasta mais por visita?",
                labels={'Ticket_Medio': 'Ticket Médio (R$)'}
            )
            fig_ticket.update_traces(textposition='outside')
            st.plotly_chart(fig_ticket, use_container_width=True)

# === NOVO INSIGHT 3: Qualidade vs Faturamento (Adicionado ao final da Coluna 1 ou 2, escolhi 1 para balancear) ===
with col1:
    if insights:
        st.markdown("---")
        st.markdown("##### ⭐ Qualidade vs. Faturamento (Matriz)")
        df_rating = insights['rating_faturamento'].set_index('Product line')
        st.dataframe(df_rating.style.format({'Rating_Medio': '{:.2f}', 'Faturamento': 'R${:.2f}'}), use_container_width=True)
        with st.expander("Ver Matriz de Qualidade"):
            fig_qualidade = px.scatter(
                insights['rating_faturamento'], x='Faturamento', y='Rating_Medio',
                size='Faturamento', color='Product line',
                title="Produtos: Avaliação vs. Receita",
                hover_name='Product line'
            )
            media_geral_rating = df_dia_raw['Rating'].mean()
            fig_qualidade.add_hline(y=media_geral_rating, line_dash="dot", annotation_text="Média Geral")
            st.plotly_chart(fig_qualidade, use_container_width=True)
