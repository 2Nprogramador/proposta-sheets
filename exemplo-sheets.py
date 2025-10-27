import pandas as pd
import streamlit as st
import plotly.express as px
import plotly
import gspread # Biblioteca para interagir com a Google Sheets API
from gspread.exceptions import WorksheetNotFound, APIError # Importações explícitas para tratamento de erros
import datetime # ADICIONADO: Necessário para usar datetime.datetime


# --- INSTRUÇÕES PARA CONFIGURAÇÃO DE CREDENCIAIS NO STREAMLIT CLOUD ---
#
# Para funcionar, você DEVE configurar um arquivo chamado '.streamlit/secrets.toml'
# no seu repositório ou diretamente no painel de Configurações do Streamlit Cloud.
#
# O erro "WorksheetNotFound: proposta-vendas" indica que o valor de `worksheet_name`
# no secrets.toml não corresponde EXATAMENTE ao nome da aba na sua planilha.
# VERIFIQUE A ABA: Se a aba se chamar, por exemplo, "Proposta-vendas" (com 'P' maiúsculo)
# ou "Sheet1", você deve atualizar o secrets.toml com o nome correto.
#
# Exemplo do seu secrets.toml, que precisa de conferência na linha 2:
# [gsheets]
# url = "URL_COMPLETA_DA_SUA_PLANILHA"
# worksheet_name = "Dados" # <-- CONFIRA SE ESTE NOME ESTÁ CORRETO
#
# [gcp_service_account]
# # ... (adicione todos os outros campos do JSON da Service Account aqui)
#
# OBS: O bloco [gcp_service_account] DEVE conter exatamente os mesmos campos do seu arquivo JSON de credenciais.


@st.cache_data(ttl=600) # Cache para recarregar os dados a cada 600 segundos (10 minutos)
def load_data_from_gsheets():
    """
    Carrega os dados da Google Sheet usando as credenciais da Service Account
    armazenadas em st.secrets e retorna um DataFrame.
    """
    st.info("Carregando dados... Isso pode levar alguns segundos na primeira vez.")
    try:
        # 1. Obter as credenciais da Service Account e URL do st.secrets
        creds_dict = st.secrets["gcp_service_account"]
        gsheets_url = st.secrets["gsheets"]["url"]
        worksheet_name = st.secrets["gsheets"]["worksheet_name"]

        # 2. Autorizar o gspread usando o dicionário de credenciais
        gc = gspread.service_account_from_dict(creds_dict)

        # 3. Abrir a planilha e selecionar a aba
        sh = gc.open_by_url(gsheets_url)
        # O método .worksheet() retorna a aba pelo nome. Aqui que ocorre o WorksheetNotFound.
        worksheet = sh.worksheet(worksheet_name)

        # 4. LER OS DADOS E CONVERTER PARA DATAFRAME
        # get_all_records() retorna uma lista de dicionários, usando a primeira linha como cabeçalho.
        data = worksheet.get_all_records() 
        df_sheet = pd.DataFrame.from_records(data)


        # Limpeza e conversão de tipos (essencial após a leitura da planilha)
        df_sheet.dropna(how='all', inplace=True)
        
        # Converte 'Data' para o formato de data (tentativa mais robusta)
        try:
            # Tenta converter o objeto para data, inferindo o formato
            df_sheet['Data'] = pd.to_datetime(df_sheet['Data'], errors='coerce')
        except Exception:
             st.warning("Não foi possível converter a coluna 'Data'. Verifique o formato na planilha (deve ser YYYY-MM-DD ou similar).")
             # Se falhar, tenta um formato padrão explícito
             df_sheet['Data'] = pd.to_datetime(df_sheet['Data'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            
        # Converte colunas numéricas, lidando com possíveis erros
        df_sheet['Total'] = pd.to_numeric(df_sheet['Total'], errors='coerce')
        # Quantidade pode ser um inteiro que permite NaNs (Int64)
        df_sheet['Quantity'] = pd.to_numeric(df_sheet['Quantity'], errors='coerce').astype('Int64')

        # Remove linhas com valores NaT, NaN ou zero inesperado nas colunas chave
        df_sheet.dropna(subset=['Data', 'Total', 'Quantity'], inplace=True)
        df_sheet = df_sheet[df_sheet['Total'] > 0]
        
        # Remove linhas onde a data é NaT (Not a Time)
        df_sheet.dropna(subset=['Data'], inplace=True)
        
        st.success("Dados carregados com sucesso!")
        return df_sheet

    except KeyError as e:
        # Erro específico se o st.secrets estiver faltando uma chave
        st.error(f"Erro de Configuração: A chave '{e}' está faltando no arquivo secrets.toml. Verifique suas credenciais e a URL/nome da planilha.")
        st.stop()
    except WorksheetNotFound:
        # Erro específico para a aba não encontrada
        worksheet_name = st.secrets.get("gsheets", {}).get("worksheet_name", "NOME_DESCONHECIDO")
        st.error(f"Erro de Configuração: A aba/folha de trabalho '{worksheet_name}' não foi encontrada na planilha. Verifique se o nome em `secrets.toml` (`worksheet_name`) está EXATAMENTE igual ao nome da aba na sua planilha do Google Sheets (incluindo letras maiúsculas/minúsculas).")
        st.stop()
    except APIError as e:
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
    if isinstance(dia, (pd.Timestamp, datetime.datetime)):
        dia_date = dia.date()
    else:
        dia_date = dia

    dia_timestamp = pd.to_datetime(dia_date)
    dia_anterior_timestamp = dia_timestamp - pd.Timedelta(days=1)

    # Filtragem de dados para o dia atual e o dia anterior
    df_dia = data_df[data_df['Data'].dt.date == dia_date]
    df_dia_anterior = data_df[data_df['Data'].dt.date == dia_anterior_timestamp.date()]

    if df_dia.empty:
        return {}


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
    
    # FUNÇÃO CORRIGIDA: Usa groupby().value_counts().unstack() que é mais robusta
    def calcular_crosstab_e_variacao(df_atual, df_anterior, index_cols, col_cols):
        # Cria a tabela de contagem de forma robusta
        atual = df_atual.groupby(index_cols)[col_cols].value_counts().unstack(fill_value=0)
        anterior = df_anterior.groupby(index_cols)[col_cols].value_counts().unstack(fill_value=0)
        
        # Alinha os índices (linhas) e colunas
        idx = atual.index.union(anterior.index)
        cols = atual.columns.union(anterior.columns)
        
        atual_reindex = atual.reindex(index=idx, columns=cols, fill_value=0)
        anterior_reindex = anterior.reindex(index=idx, columns=cols, fill_value=0)
        
        variacao = atual_reindex - anterior_reindex
        
        return atual, variacao

    # Usando Quantity (Count de Transações) para os crosstabs
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
    st.info("O DataFrame está vazio ou a coluna 'Data' não foi encontrada. Verifique sua planilha e a aba configurada.")
    st.stop()


# Seleção do dia
dias_unicos = df['Data'].dt.date.unique()
dias_unicos_ordenados = sorted(dias_unicos, reverse=True)
if not dias_unicos_ordenados:
    st.info("Não há datas válidas para seleção no seu conjunto de dados.")
    st.stop()

# Garantir que a seleção do dia está correta para evitar erros no relatorio_por_dia_com_variacoes
dia_selecionado = st.sidebar.selectbox("Selecione uma data", dias_unicos_ordenados)

# Variável para checar se é o primeiro dia do dataset
primeiro_dia_disponivel = dias_unicos_ordenados[-1] 

# Gerando o relatório para o dia selecionado
relatorio = relatorio_por_dia_com_variacoes(dia_selecionado, df)

# Se o relatório retornar vazio (dia sem dados), paramos a execução aqui
if not relatorio:
    st.info(f"Não há dados de vendas para o dia {dia_selecionado}.")
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
total_atual_cidade = relatorio['total_por_cidade']['Total']
variacao_cidade_abs = relatorio['variacao_cidade']['Total']
total_anterior_cidade = total_atual_cidade - variacao_cidade_abs

# Filtrar índices onde o total_anterior é válido e maior que zero
valid_indices = total_anterior_cidade[total_anterior_cidade > 0].index

if not valid_indices.empty:
    # Calcula a variação percentual APENAS para índices válidos
    variacao_percentual_cidade = (variacao_cidade_abs.loc[valid_indices] / total_anterior_cidade.loc[valid_indices]) * 100
    
    cidades_queda = variacao_percentual_cidade[variacao_percentual_cidade < -30]
    if not cidades_queda.empty:
        cidades_str = ", ".join(cidades_queda.index)
        alertas_negativos.append(f"As cidades **{cidades_str}** tiveram uma queda superior a 30% nas vendas em relação ao dia anterior.")


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
with st.expander("Alertas Importantes", expanded=True if total_alertas > 0 else False, icon="🚨"):
    for alerta in alertas_positivos:
        st.success(alerta)
    for alerta in alertas_negativos:
        st.error(alerta)
    if not alertas_positivos and not alertas_negativos:
        st.info("Nenhum alerta foi gerado para o dia selecionado.")

# Exibindo o relatório
st.subheader(f"Relatório Detalhado de Vendas para o dia {dia_selecionado}")

col1, col2 = st.columns(2) # Reduzido para 2 colunas para melhor layout

# Estilo para DataFrames (opcional, mas melhora a visualização)
def style_dataframe(df_input):
    # Verifica se o dia selecionado é o primeiro dia do dataset
    is_first_day = (dia_selecionado == primeiro_dia_disponivel)
    
    # Formatos padrão para colunas que NÃO são variação
    format_dict = {
        "Total": "R${:.2f}", 
        "Quantity": "{:.0f}"
    }

    # Formatos padrão para colunas de variação (incluindo o sinal)
    var_format_dict = {
        "Var. Total": "R${:+.2f}", 
        "Var. Quantity": "{:+.0f}"
    }
    
    # Se for o primeiro dia, sobrescreve o formato de variação com 'N/A' literal.
    if is_first_day:
        # A formatação lambda anterior era complexa e desnecessária, pois já sabemos que é o primeiro dia.
        # Formatamos a variação como uma string literal "N/A"
        var_format_dict = {
            "Var. Total": lambda x: "N/A" if pd.notna(x) else "-",
            "Var. Quantity": lambda x: "N/A" if pd.notna(x) else "-"
        }
    
    # Junta todos os dicionários de formato
    format_dict.update(var_format_dict)

    df_styled = df_input.style.format(format_dict, na_rep="-")
    
    return df_styled

# --- FUNÇÃO AUXILIAR PARA PLOTAGEM COM VARIAÇÃO (AGORA INCLUI QUANTIDADE) ---
def plot_total_and_variation(df_total, df_var, id_col, title):
    """
    Cria um DataFrame combinado e plota o Total, Quantidade e suas Variações.
    """
    # 1. Renomeia as colunas de variação e junta os DataFrames
    df_var_renamed = df_var.rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})
    
    df_concat = pd.concat([df_total.round(2), df_var_renamed.round(2)], axis=1).reset_index()
    
    # 2. Reformatar (melt) para que Total, Var. Total, Quantity, Var. Quantity sejam linhas
    # Usando todas as colunas relevantes no melt
    df_plot = df_concat.melt(
        id_vars=id_col, 
        value_vars=['Total', 'Var. Total', 'Quantity', 'Var. Quantity'], 
        var_name='Métrica', 
        value_name='Valor'
    ).dropna(subset=['Valor'])
    
    # Adicionar coluna para agrupar as barras por tipo de métrica (Vendas vs. Quantidade)
    df_plot['Tipo'] = df_plot['Métrica'].apply(lambda x: 'Vendas (R$)' if 'Total' in x else 'Quantidade')
    df_plot['Medida'] = df_plot['Métrica'].apply(lambda x: 'Total' if 'Var.' not in x else 'Variação')


    # Cria o gráfico de barras interativo (Plotly Express)
    # Usamos `Tipo` para faceting (separação em subplots) e `Medida` para agrupar as barras.
    fig = px.bar(
        df_plot, 
        x=id_col, 
        y='Valor', 
        color='Medida', # Agrupamento principal (Total vs Variação)
        barmode='group', 
        facet_col='Tipo', # Subplots (Vendas vs Quantidade)
        title=f"{title} - Total, Quantidade e Variação",
        template='plotly_white',
        color_discrete_map={
            'Total': '#4C78A8',        # Azul para totais
            'Variação': '#E45756'      # Vermelho para variações
        }
    )
    
    # Configurações do layout
    fig.update_layout(height=450, title_x=0.5)
    
    # Adiciona a linha zero para melhor visualização da variação (apenas no subplot de Quantidade/Vendas)
    fig.for_each_xaxis(lambda axis: axis.update(title_text=''))
    
    # Adiciona linha zero nos dois subplots
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row="all", col="all")
    
    return fig


with col1:
    # Exibição com variações
    st.markdown("##### Total de Vendas por Cidade e Variação:")
    df_cidade_concat = pd.concat([relatorio['total_por_cidade'].round(2), relatorio['variacao_cidade'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_cidade_concat), use_container_width=True)

    with st.expander("Gráfico de Vendas e Quantidades por Cidade (Total e Variação)"):
        fig = plot_total_and_variation(
            relatorio['total_por_cidade'].round(2), 
            relatorio['variacao_cidade'].round(2), 
            'City', 
            "Métricas por Cidade"
        )
        st.plotly_chart(fig, use_container_width=True)


    st.markdown("##### Total de vendas por Tipo de Cliente e Variação:")
    df_cliente_concat = pd.concat([relatorio['total_por_tipo_cliente'].round(2), relatorio['variacao_tipo_cliente'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_cliente_concat), use_container_width=True)

    with st.expander("Gráfico de Vendas e Quantidades por Tipo de Cliente (Total e Variação)"):
        fig = plot_total_and_variation(
            relatorio['total_por_tipo_cliente'].round(2), 
            relatorio['variacao_tipo_cliente'].round(2), 
            'Customer type', 
            "Métricas por Tipo de Cliente"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("##### Total de vendas por Gênero e Variação:")
    df_genero_concat = pd.concat([relatorio['total_por_genero'].round(2), relatorio['variacao_genero'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_genero_concat), use_container_width=True)

    with st.expander("Gráfico de Vendas e Quantidades por Gênero (Total e Variação)"):
        fig = plot_total_and_variation(
            relatorio['total_por_genero'].round(2), 
            relatorio['variacao_genero'].round(2), 
            'Gender', 
            "Métricas por Gênero"
        )
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("##### Total de vendas por Linha de Produto e Variação:")
    df_produto_concat = pd.concat([relatorio['total_por_linha_produto'].round(2), relatorio['variacao_linha_produto'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_produto_concat), use_container_width=True)

    with st.expander("Gráfico de Vendas e Quantidades por Linha de Produto (Total e Variação)"):
        fig = plot_total_and_variation(
            relatorio['total_por_linha_produto'].round(2), 
            relatorio['variacao_linha_produto'].round(2), 
            'Product line', 
            "Métricas por Linha de Produto"
        )
        st.plotly_chart(fig, use_container_width=True)


    st.markdown("##### Total de vendas por Método de Pagamento e Variação:")
    df_payment_concat = pd.concat([relatorio['total_por_payment'].round(2), relatorio['variacao_payment'].round(2).rename(
        columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1)
    st.dataframe(style_dataframe(df_payment_concat), use_container_width=True)

    with st.expander("Gráfico de Vendas e Quantidades por Método de Pagamento (Total e Variação)"):
        fig = plot_total_and_variation(
            relatorio['total_por_payment'].round(2), 
            relatorio['variacao_payment'].round(2), 
            'Payment', 
            "Métricas por Método de Pagamento"
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------ Cross-tabs (Contagem de Clientes) ------------------
    # Estes gráficos permanecem como contagem pura, pois a variação e o total já são mostrados
    # na tabela cruzada.

    st.markdown("##### Distribuição de Clientes por Cidade e Tipo:")
    st.dataframe(pd.concat(
        [relatorio["crosstab_cidade_tipo_cliente"], relatorio["variacao_cidade_tipo_cliente"].add_suffix(" (Var)")],
        axis=1
    ).fillna(0).astype(int), use_container_width=True)
    
    with st.expander("Gráfico de Distribuição de Clientes por Cidade e Tipo"):
        df_plot = relatorio["crosstab_cidade_tipo_cliente"].reset_index().melt(id_vars="City")
        fig = px.bar(
            df_plot,
            x="City",
            y="value",
            color="Customer type",
            barmode="group",
            title="Distribuição de Clientes por Cidade e Tipo",
            labels={'value': 'Número de Clientes'}
        )
        st.plotly_chart(fig, use_container_width=True)

    
    st.markdown("##### Distribuição de Clientes por Cidade, Gênero e Tipo:")
    st.dataframe(pd.concat(
        [relatorio["crosstab_cidade_genero"], relatorio["variacao_cidade_genero"].add_suffix(" (Var)")], axis=1
    ).fillna(0).astype(int), use_container_width=True)

    with st.expander("Gráfico de Distribuição de Clientes por Cidade, Gênero e Tipo"):
        # O crosstab_cidade_genero tem um MultiIndex, é preciso empilhar para o Plotly
        df_plot = relatorio['crosstab_cidade_genero'].stack(level=0).reset_index().rename(columns={0: 'count'})
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


    st.markdown("##### Distribuição de Clientes por Cidade, Pagamento e Gênero:")
    st.dataframe(pd.concat(
        [relatorio["crosstab_cidade_payment"], relatorio["variacao_cidade_payment"].add_suffix(" (Var)")], axis=1
    ).fillna(0).astype(int), use_container_width=True)

    with st.expander("Distribuição de Clientes por Cidade, Pagamento e Gênero"):
        df_plot = relatorio['crosstab_cidade_payment'].stack(level=0).reset_index().rename(columns={0: 'count'})
        fig = px.bar(df_plot, x='City', y='count', color='Gender',
                      facet_col='Payment',
                      barmode='group',
                      title='Distribuição de Clientes por Cidade, Gênero e Forma de Pagamento',
                      labels={'count': 'Número de Clientes'},
                      template='plotly_dark')

        fig.update_xaxes(tickangle=45)
        fig.update_layout(height=445)

        st.plotly_chart(fig, use_container_width=True)
