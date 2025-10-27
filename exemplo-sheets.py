import pandas as pd
import streamlit as st
import plotly.express as px
import plotly
import gspread # Biblioteca para interagir com a Google Sheets API
from gspread_dataframe import get_dataframe # Para converter a planilha em DataFrame
import json # Para lidar com as credenciais (embora gspread faça a conversão do dict)

# --- INSTRUÇÕES PARA CONFIGURAÇÃO DE CREDENCIAIS NO STREAMLIT CLOUD ---
#
# Para funcionar, você DEVE configurar um arquivo chamado '.streamlit/secrets.toml'
# no seu repositório ou diretamente no painel de Configurações do Streamlit Cloud.
#
# 1. Crie uma Conta de Serviço (Service Account) no Google Cloud.
# 2. Compartilhe sua Google Sheet (Planilha) com o e-mail da Service Account.
# 3. Baixe o arquivo JSON das credenciais.
# 4. Crie o arquivo '.streamlit/secrets.toml' e adicione o seguinte conteúdo:
#
# [gsheets]
# url = "URL_COMPLETA_DA_SUA_PLANILHA_DO_GOOGLE_SHEETS" # Ex: https://docs.google.com/spreadsheets/d/.../edit
# worksheet_name = "Sheet1" # Nome da aba/folha que contém os dados (ex: 'Dados de Vendas')
#
# [gcp_service_account]
# type = "service_account"
# project_id = "SEU_PROJECT_ID"
# private_key_id = "SUA_PRIVATE_KEY_ID"
# # A chave privada deve estar em uma única linha, com '\n' para quebras de linha
# private_key = "-----BEGIN PRIVATE KEY-----\nSUA CHAVE PRIVADA AQUI\n-----END PRIVATE KEY-----\n"
# client_email = "SUA_SERVICE_ACCOUNT_EMAIL@SEU_PROJECT_ID.iam.gserviceaccount.com"
# # ... (adicione todos os outros campos do JSON da Service Account aqui)
#
# OBS: O bloco [gcp_service_account] DEVE conter exatamente os mesmos campos do seu arquivo JSON de credenciais.


@st.cache_data(ttl=600) # Cache para recarregar os dados a cada 600 segundos (10 minutos)
def load_data_from_gsheets():
    """
    Carrega os dados da Google Sheet usando as credenciais da Service Account
    armazenadas em st.secrets e retorna um DataFrame.
    """
    try:
        # 1. Obter as credenciais da Service Account e URL do st.secrets
        creds_dict = st.secrets["gcp_service_account"]
        gsheets_url = st.secrets["gsheets"]["url"]
        worksheet_name = st.secrets["gsheets"]["worksheet_name"]

        # 2. Autorizar o gspread usando o dicionário de credenciais
        gc = gspread.service_account_from_dict(creds_dict)

        # 3. Abrir a planilha e selecionar a aba
        sh = gc.open_by_url(gsheets_url)
        worksheet = sh.worksheet(worksheet_name)

        # 4. Ler os dados para um DataFrame
        # OBS: get_dataframe assume a primeira linha como cabeçalho por padrão.
        df_sheet = get_dataframe(worksheet, header=1) 

        # Limpeza e conversão de tipos (essencial após a leitura da planilha)
        df_sheet.dropna(how='all', inplace=True)
        
        # Converte 'Data' para o formato de data
        # Adicionei 'format=...' para ajudar a conversão se o formato for conhecido (opcional, mas bom)
        try:
            df_sheet['Data'] = pd.to_datetime(df_sheet['Data'], errors='coerce')
        except ValueError:
            st.warning("Não foi possível converter a coluna 'Data' automaticamente. Verifique o formato na planilha.")
            df_sheet['Data'] = pd.to_datetime(df_sheet['Data'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        
        # Converte colunas numéricas, lidando com possíveis erros
        df_sheet['Total'] = pd.to_numeric(df_sheet['Total'], errors='coerce')
        # Quantidade pode ser um inteiro que permite NaNs
        df_sheet['Quantity'] = pd.to_numeric(df_sheet['Quantity'], errors='coerce').astype('Int64')

        # Remove linhas com 'Data' inválida após a conversão
        df_sheet.dropna(subset=['Data', 'Total', 'Quantity'], inplace=True)
        
        # Remove linhas onde a data é NaT (Not a Time)
        df_sheet.dropna(subset=['Data'], inplace=True)


        return df_sheet

    except KeyError as e:
        # Erro específico se o st.secrets estiver faltando uma chave
        st.error(f"Erro de Configuração: A chave '{e}' está faltando no arquivo secrets.toml. Verifique suas credenciais e a URL/nome da planilha.")
        st.stop()
    except gspread.exceptions.APIError as e:
        # Erro específico se a Service Account não tiver acesso
        st.error(f"Erro de Permissão do Google Sheets (APIError): Verifique se o e-mail da sua Service Account tem permissão de 'Leitor' na planilha.")
        st.stop()
    except Exception as e:
        # Erro genérico de conexão/processamento
        st.error(f"Erro crítico ao carregar dados. Detalhes: {type(e).__name__}: {e}")
        st.stop()
        return pd.DataFrame()


# Carregando os dados da planilha
df = load_data_from_gsheets()


# Função para gerar o relatório por dia com variações (reutilizando a lógica original)
def relatorio_por_dia_com_variacoes(dia, data_df):
    """
    Gera o relatório diário e calcula variações em relação ao dia anterior.
    """
    # Converter o objeto date para Timestamp para comparação correta
    dia_timestamp = pd.to_datetime(dia)
    dia_anterior_timestamp = dia_timestamp - pd.Timedelta(days=1)

    # Filtragem de dados para o dia atual e o dia anterior
    df_dia = data_df[data_df['Data'].dt.date == dia_timestamp.date()]
    df_dia_anterior = data_df[data_df['Data'].dt.date == dia_anterior_timestamp.date()]

    if df_dia.empty:
        # st.warning(f"Não há dados para o dia {dia_timestamp.date()}.")
        return {}


    # Cálculos de totais
    total_por_cidade = df_dia.groupby('City')[['Total', 'Quantity']].sum()
    total_por_tipo_cliente = df_dia.groupby('Customer type')[['Total', 'Quantity']].sum()
    total_por_genero = df_dia.groupby('Gender')[['Total', 'Quantity']].sum()
    total_por_linha_produto = df_dia.groupby('Product line')[['Total', 'Quantity']].sum()
    total_por_payment = df_dia.groupby('Payment')[['Total', 'Quantity']].sum()

    # Função auxiliar para calcular totais e reindexar para a variação
    def calcular_totais_e_variacao(df_atual, df_anterior, coluna_agrupadora):
        total_atual = df_atual.groupby(coluna_agrupadora)[['Total', 'Quantity']].sum()
        total_anterior = df_anterior.groupby(coluna_agrupadora)[['Total', 'Quantity']].sum()
        
        # Alinha os DataFrames para a subtração, preenchendo novos grupos com 0
        base_index = total_atual.index.union(total_anterior.index)
        total_atual = total_atual.reindex(base_index, fill_value=0)
        total_anterior = total_anterior.reindex(base_index, fill_value=0)
        
        variacao = total_atual - total_anterior
        return total_atual, variacao

    total_por_cidade, variacao_cidade = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'City')
    total_por_tipo_cliente, variacao_tipo_cliente = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Customer type')
    total_por_genero, variacao_genero = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Gender')
    total_por_linha_produto, variacao_linha_produto = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Product line')
    total_por_payment, variacao_payment = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Payment')

    
    # Tabelas cruzadas (Cross-tabs) e Variações
    
    # Função auxiliar para calcular crosstab e variação
    def calcular_crosstab_e_variacao(df_atual, df_anterior, index_cols, col_cols):
        atual = pd.crosstab(df_atual[index_cols], df_atual[col_cols])
        anterior = pd.crosstab(df_anterior[index_cols], df_anterior[col_cols])
        
        # Alinha e subtrai
        idx = atual.index.union(anterior.index)
        cols = atual.columns.union(anterior.columns)
        atual_reindex = atual.reindex(index=idx, columns=cols, fill_value=0)
        anterior_reindex = anterior.reindex(index=idx, columns=cols, fill_value=0)
        return atual, atual_reindex - anterior_reindex

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

# --- LÓGICA STREAMLIT ---

st.set_page_config(layout="wide")
st.title("Relatório Diário de Vendas com Variações (Google Sheets)")

# Verificação de dados
if df.empty or 'Data' not in df.columns:
    # A mensagem de erro da função load_data_from_gsheets já foi exibida se houve falha na conexão.
    st.info("O DataFrame está vazio ou a coluna 'Data' não foi encontrada. Verifique sua planilha e a aba configurada.")
    st.stop()


# Seleção do dia
dias_unicos = df['Data'].dt.date.unique()
dias_unicos_ordenados = sorted(dias_unicos, reverse=True)
if not dias_unicos_ordenados:
    st.info("Não há datas válidas para seleção no seu conjunto de dados.")
    st.stop()

dia_selecionado = st.sidebar.selectbox("Selecione uma data", dias_unicos_ordenados)

# Gerando o relatório para o dia selecionado
relatorio = relatorio_por_dia_com_variacoes(dia_selecionado, df)

# Se o relatório retornar vazio (dia sem dados), paramos a execução aqui
if not relatorio:
    st.info(f"Não há dados para o dia {dia_selecionado}.")
    st.stop()

# Condições para os alertas
alertas_positivos = []
alertas_negativos = []

# Condição 1: Cidades com vendas totais acima de 30.000 (positivo)
cidades_acima_30000 = relatorio['total_por_cidade'][relatorio['total_por_cidade']['Total'] > 30000]
if not cidades_acima_30000.empty:
    cidades_str = ", ".join(cidades_acima_30000.index)
    alertas_positivos.append(f"As cidades **{cidades_str}** ultrapassaram R$30.000 em vendas totais.")

# Condição 2: Cidades com queda de mais de 30% nas vendas totais (negativo)
total_anterior_cidade = relatorio['total_por_cidade']['Total'] - relatorio['variacao_cidade']['Total']
# Evitar divisão por zero e NaNs
valid_indices = total_anterior_cidade[(total_anterior_cidade > 0) & (relatorio['variacao_cidade']['Total'].notna())].index
if not valid_indices.empty:
    variacao_percentual_cidade = (relatorio['variacao_cidade']['Total'].loc[valid_indices] / total_anterior_cidade.loc[valid_indices]) * 100
    cidades_queda = variacao_percentual_cidade[variacao_percentual_cidade < -30]
    if not cidades_queda.empty:
        cidades_str = ", ".join(cidades_queda.index)
        alertas_negativos.append(f"As cidades **{cidades_str}** tiveram uma queda superior a 30% nas vendas.")


# Condição 3: Método de pagamento "Pix" com aumento superior a 30% (positivo)
if "Pix" in relatorio['total_por_payment'].index:
    total_pix = relatorio['total_por_payment'].loc["Pix", "Total"]
    variacao_pix = relatorio['variacao_payment'].loc["Pix", "Total"]
    total_anterior_pix = total_pix - variacao_pix
    if total_anterior_pix > 0:
        variacao_perc = (variacao_pix / total_anterior_pix) * 100
        if variacao_perc > 30:
            alertas_positivos.append(f"O método de pagamento **Pix** apresentou um aumento superior a 30% ({variacao_perc:.1f}%) nas vendas.")

# Condição 4: Produtos vendidos mais de 400 vezes (positivo)
produtos_acima_400 = relatorio['total_por_linha_produto'][relatorio['total_por_linha_produto']['Quantity'] > 400]
if not produtos_acima_400.empty:
    produtos_str = ", ".join(produtos_acima_400.index)
    alertas_positivos.append(f"Os produtos **{produtos_str}** tiveram mais de 400 vendas.")

# Exibir notificações na sidebar
st.sidebar.subheader("Alertas do Dia")
total_alertas = len(alertas_positivos) + len(alertas_negativos)

if total_alertas > 0:
    st.sidebar.error(f"🚨 {total_alertas} ALERTAS 🚨 ENCONTRADOS, ABRA O EXPANDER PARA MAIS DETALHES")
else:
    st.sidebar.info("Não há alertas para o dia selecionado.")



# Exibir os alertas no expander
with st.expander("Alertas Importantes", expanded=False, icon="🚨"):
    for alerta in alertas_positivos:
        st.success(alerta)
    for alerta in alertas_negativos:
        st.error(alerta)
    if not alertas_positivos and not alertas_negativos:
        st.info("Nenhum alerta foi gerado para o dia selecionado.")

# Exibindo o relatório
st.subheader(f"Relatório Detalhado de Vendas para o dia {dia_selecionado}")

col1, col2 = st.columns(2) # Reduzido para 2 colunas para melhor layout

with col1:
    # Exibição com variações
    st.write("**Total de Vendas por Cidade e Variação:**")
    st.dataframe(pd.concat([relatorio['total_por_cidade'].round(2), relatorio['variacao_cidade'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1))

    with st.expander("Gráfico de Total de Vendas por Cidade e Variação"):
        df_plot_cidade = relatorio['total_por_cidade'].reset_index()
        fig = px.bar(df_plot_cidade, x='City', y='Total', title="Total de Vendas por Cidade")
        st.plotly_chart(fig, use_container_width=True)


    st.write("**Total de vendas por Tipo de Cliente e Variação:**")
    st.dataframe(pd.concat([relatorio['total_por_tipo_cliente'].round(2), relatorio['variacao_tipo_cliente'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1))

    with st.expander("Gráfico de Total de vendas por Tipo de Cliente e Variação:"):
        df_plot_cliente = relatorio['total_por_tipo_cliente'].reset_index()
        fig = px.bar(df_plot_cliente, x='Customer type', y='Total', title="Total de Vendas por Tipo de Cliente")
        st.plotly_chart(fig, use_container_width=True)

    st.write("**Total de vendas por Gênero e Variação:**")
    st.dataframe(pd.concat([relatorio['total_por_genero'].round(2), relatorio['variacao_genero'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1))

    with st.expander("Gráfico de Total de vendas por Gênero e Variação"):
        df_plot_genero = relatorio['total_por_genero'].reset_index()
        fig = px.bar(df_plot_genero, x='Gender', y='Total', title="Total de Vendas por Gênero")
        st.plotly_chart(fig, use_container_width=True)


    st.write("**Total de vendas por Método de Pagamento e Variação:**")
    st.dataframe(pd.concat([relatorio['total_por_payment'].round(2), relatorio['variacao_payment'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1))

    with st.expander("Gráfico de vendas por Método de Pagamento e Variação"):
        df_plot_payment = relatorio['total_por_payment'].reset_index()
        fig = px.bar(df_plot_payment, x='Payment', y='Total', title="Total de Vendas por Método de Pagamento")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.write("**Total de vendas por Linha de Produto e Variação:**")
    st.dataframe(pd.concat([relatorio['total_por_linha_produto'].round(2), relatorio['variacao_linha_produto'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1))

    with st.expander("Gráfico de vendas por Linha de Produto e Variação"):
        df_plot_produto = relatorio['total_por_linha_produto'].reset_index()
        fig = px.bar(df_plot_produto, x='Product line', y='Total', title="Total de Vendas por Linha de Produto")
        st.plotly_chart(fig, use_container_width=True)


    st.write("**Distribuição de Clientes por Cidade e Tipo:**")
    st.dataframe(pd.concat(
        [relatorio["crosstab_cidade_tipo_cliente"], relatorio["variacao_cidade_tipo_cliente"].add_suffix(" (Var)")],
        axis=1
    ))
    with st.expander("Gráfico de Distribuição de Clientes por Cidade e Tipo"):
        fig = px.bar(
            relatorio["crosstab_cidade_tipo_cliente"].reset_index().melt(id_vars="City"),
            x="City",
            y="value",
            color="Customer type",
            barmode="group",
            title="Distribuição de Clientes por Cidade e Tipo",
            labels={'value': 'Número de Clientes'}
        )
        st.plotly_chart(fig, use_container_width=True)

    st.write("**Distribuição de Clientes por Cidade, Gênero e Tipo:**")
    st.dataframe(pd.concat(
        [relatorio["crosstab_cidade_genero"], relatorio["variacao_cidade_genero"].add_suffix(" (Var)")], axis=1
    ))

    with st.expander("Gráfico de Distribuição de Clientes por Cidade, Gênero e Tipo"):
        df_plot = relatorio['crosstab_cidade_genero'].stack().reset_index(name='count')
        fig = px.bar(df_plot,
                      x='City',
                      y='count',
                      color='Customer type',
                      facet_col='Gender',
                      barmode='group',
                      title='Distribuição de Clientes por Cidade, Gênero e Tipo',
                      labels={'count': 'Número de Clientes'}
                      )
        st.plotly_chart(fig, use_container_width=True)


    st.write("**Distribuição de Clientes por Cidade, Gênero e Pagamento:**")
    st.dataframe(pd.concat(
        [relatorio["crosstab_cidade_payment"], relatorio["variacao_cidade_payment"].add_suffix(" (Var)")], axis=1
    ))

    with st.expander("Distribuição de Clientes por Cidade, Gênero e Pagamento"):
        df_plot = relatorio['crosstab_cidade_payment'].stack().reset_index(name='count')
        fig = px.bar(df_plot, x='City', y='count', color='Gender',
                      facet_col='Payment',
                      barmode='group',
                      title='Distribuição de Clientes por Cidade, Gênero e Forma de Pagamento',
                      labels={'count': 'Número de Clientes'},
                      template='plotly_dark')

        fig.update_xaxes(tickangle=45)
        fig.update_layout(height=445)

        st.plotly_chart(fig, use_container_width=True)
