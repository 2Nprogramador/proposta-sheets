import pandas as pd
import streamlit as st
import plotly.express as px
import gspread
import datetime
import random
import numpy as np

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Dashboard de Vendas")

# --- 2. FUNÇÕES DE DADOS (GOOGLE SHEETS) ---

@st.cache_data(ttl=600)
def load_data_from_gsheets():
    """Carrega os dados da Google Sheet usando credenciais do st.secrets."""
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
        
        # Conversão de tipos
        df_sheet['Data'] = pd.to_datetime(df_sheet['Data'], errors='coerce')
        df_sheet['Total'] = pd.to_numeric(df_sheet['Total'], errors='coerce')
        df_sheet['Quantity'] = pd.to_numeric(df_sheet['Quantity'], errors='coerce').astype('Int64')

        df_sheet.dropna(subset=['Data', 'Total', 'Quantity'], inplace=True)
        df_sheet = df_sheet[df_sheet['Total'] > 0]
        
        return df_sheet
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()
        return pd.DataFrame()

def salvar_dados_gsheets(df_novos_dados):
    """Adiciona novos registros à planilha."""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        gsheets_url = st.secrets["gsheets"]["url"]
        worksheet_name = st.secrets["gsheets"]["worksheet_name"]

        gc = gspread.service_account_from_dict(creds_dict)
        sh = gc.open_by_url(gsheets_url)
        worksheet = sh.worksheet(worksheet_name)

        df_export = df_novos_dados.copy()
        df_export['Data'] = df_export['Data'].dt.strftime('%Y-%m-%d')
        
        dados_lista = df_export.astype(object).values.tolist()
        worksheet.append_rows(dados_lista)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar dados: {e}")
        return False

def gerar_dados_proximo_dia(df_atual):
    """Gera transações fictícias para simulação."""
    if df_atual.empty:
        ultimo_dia = datetime.date.today()
    else:
        ultimo_dia = df_atual['Data'].max().date()
    
    proximo_dia = ultimo_dia + datetime.timedelta(days=1)
    qtd_transacoes = random.randint(100, 300)
    
    novas_linhas = []
    cidades = ['Rio de Janeiro', 'São Paulo', 'Manaus']
    tipos_cliente = ['Normal', 'Membro']
    generos = ['Homem', 'Mulher']
    linhas_produto = ['Saude e Beleza', 'Acessorios Eletronicos', 'Casa e Estilo de Vida', 'Esportes e Viagens', 'Moda']
    pagamentos = ['Pix', 'Cartao de Credito', 'Debito']
    
    for _ in range(qtd_transacoes):
        unit_price = round(random.uniform(10.00, 130.00), 2)
        quantity = random.randint(1, 15)
        linha = {
            "Invoice ID": f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}",
            "City": random.choice(cidades),
            "Customer type": random.choice(tipos_cliente),
            "Gender": random.choice(generos),
            "Product line": random.choice(linhas_produto),
            "Unit price": unit_price,
            "Quantity": int(quantity),
            "Total": round(unit_price * quantity, 2),
            "Time": f"{random.randint(7, 23):02d}:{random.randint(0, 59):02d}",
            "Payment": random.choice(pagamentos),
            "Rating": round(random.uniform(3.0, 10.0), 1),
            "Data": pd.to_datetime(proximo_dia)
        }
        novas_linhas.append(linha)
        
    return pd.DataFrame(novas_linhas)

# --- 3. LÓGICA DE RELATÓRIOS E COMPARAÇÕES ---

def relatorio_por_dia_com_variacoes(dia, data_df):
    """Gera agrupamentos e calcula a variação em relação ao dia anterior."""
    dia_date = dia.date() if isinstance(dia, (pd.Timestamp, datetime.datetime)) else dia
    dia_timestamp = pd.to_datetime(dia_date)
    dia_anterior_timestamp = dia_timestamp - pd.Timedelta(days=1)

    df_dia = data_df[data_df['Data'].dt.date == dia_date].copy()
    df_dia_anterior = data_df[data_df['Data'].dt.date == dia_anterior_timestamp.date()].copy()

    if df_dia.empty:
        return {}

    is_first_day_with_data = df_dia_anterior.empty

    def calcular_totais_e_variacao(df_atual, df_anterior, coluna_agrupadora):
        total_atual = df_atual.groupby(coluna_agrupadora)[['Total', 'Quantity']].sum()
        if is_first_day_with_data:
            variacao = total_atual.copy()
            variacao[:] = np.nan
            return total_atual, variacao
        else:
            total_anterior = df_anterior.groupby(coluna_agrupadora)[['Total', 'Quantity']].sum()
            base_index = total_atual.index.union(total_anterior.index)
            total_atual_reindex = total_atual.reindex(base_index, fill_value=0)
            total_anterior_reindex = total_anterior.reindex(base_index, fill_value=0)
            variacao = total_atual_reindex - total_anterior_reindex
            return total_atual_reindex, variacao

    def calcular_crosstab_e_variacao(df_atual, df_anterior, index_cols, col_cols):
        # Simplificação do crosstab usando pivot_table ou groupby
        atual = df_atual.groupby(index_cols)[col_cols].value_counts().unstack(fill_value=0)
        if is_first_day_with_data:
            return atual, atual.applymap(lambda x: np.nan)
        anterior = df_anterior.groupby(index_cols)[col_cols].value_counts().unstack(fill_value=0)
        idx = atual.index.union(anterior.index)
        cols = atual.columns.union(anterior.columns)
        atual_re = atual.reindex(index=idx, columns=cols, fill_value=0)
        ante_re = anterior.reindex(index=idx, columns=cols, fill_value=0)
        return atual_re, atual_re - ante_re

    # Execução dos cálculos
    t_cid, v_cid = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'City')
    t_cli, v_cli = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Customer type')
    t_gen, v_gen = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Gender')
    t_pro, v_pro = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Product line')
    t_pay, v_pay = calcular_totais_e_variacao(df_dia, df_dia_anterior, 'Payment')

    c_ct, v_ct = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, 'City', 'Customer type')
    c_cg, v_cg = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, ['City', 'Gender'], 'Customer type')
    c_cp, v_cp = calcular_crosstab_e_variacao(df_dia, df_dia_anterior, ['City', 'Payment'], 'Gender')

    return {
        "total_por_cidade": t_cid, "variacao_cidade": v_cid,
        "total_por_tipo_cliente": t_cli, "variacao_tipo_cliente": v_cli,
        "total_por_genero": t_gen, "variacao_genero": v_gen,
        "total_por_linha_produto": t_pro, "variacao_linha_produto": v_pro,
        "total_por_payment": t_pay, "variacao_payment": v_pay,
        "crosstab_cidade_tipo_cliente": c_ct, "variacao_cidade_tipo_cliente": v_ct,
        "crosstab_cidade_genero": c_cg, "variacao_cidade_genero": v_cg,
        "crosstab_cidade_payment": c_cp, "variacao_cidade_payment": v_cp
    }

# --- 4. EXECUÇÃO INICIAL E BLOCO DE API ---

df = load_data_from_gsheets()

# Interceptação para Automações (n8n / Webhooks)
if "request_type" in st.query_params:
    request_type = st.query_params.get("request_type")
    target_date = st.query_params.get("target_date")
    report_name = st.query_params.get("report_name")

    if request_type == "get_report" and target_date and report_name:
        relatorio_api = relatorio_por_dia_com_variacoes(pd.to_datetime(target_date), df)
        
        # VERIFICAÇÃO DE SEGURANÇA: Se o relatório estiver vazio (sem dados para a data)
        if not relatorio_api:
            st.json({"erro": "Nenhum dado encontrado para a data informada."})
            st.stop()

        mapping = {
            "total_por_cidade": ("total_por_cidade", "variacao_cidade"),
            "total_por_tipo_cliente": ("total_por_tipo_cliente", "variacao_tipo_cliente"),
            "total_por_genero": ("total_por_genero", "variacao_genero"),
            "total_por_linha_produto": ("total_por_linha_produto", "variacao_linha_produto"),
            "total_por_payment": ("total_por_payment", "variacao_payment")
        }

        if report_name in mapping:
            k_data, k_var = mapping[report_name]
            
            # Concatena Dados e Variações com tratamento para evitar erros de índice
            df_final = pd.concat([
                relatorio_api[k_data], 
                relatorio_api[k_var].rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})
            ], axis=1)
            
            st.json(df_final.reset_index().to_dict(orient="records"))
            st.stop()

# --- 5. INTERFACE SIDEBAR ---

st.sidebar.title("Menu de Ações")
if st.sidebar.button("Gerar Próximo Dia de Vendas", type="primary"):
    with st.spinner("Simulando vendas..."):
        novos = gerar_dados_proximo_dia(df)
        if salvar_dados_gsheets(novos):
            st.sidebar.success(f"Dia {novos['Data'].dt.date.iloc[0]} salvo!")
            st.cache_data.clear()
            st.rerun()

st.sidebar.markdown("---")
dias_unicos = sorted(df['Data'].dt.date.unique(), reverse=True)
if not dias_unicos:
    st.info("Nenhuma data disponível.")
    st.stop()
dia_selecionado = st.sidebar.selectbox("Data do Relatório", dias_unicos)
primeiro_dia_disponivel = dias_unicos[-1]

# --- 6. PROCESSAMENTO DO RELATÓRIO DO DIA ---

relatorio = relatorio_por_dia_com_variacoes(dia_selecionado, df)

# Lógica de Alertas
alertas_pos, alertas_neg = [], []
if not relatorio['total_por_cidade'].empty:
    cid_30k = relatorio['total_por_cidade'][relatorio['total_por_cidade']['Total'] > 30000]
    if not cid_30k.empty:
        alertas_pos.append(f"Cidades com > R$30k: **{', '.join(cid_30k.index)}**")

st.sidebar.subheader("Alertas do Dia")
with st.expander("🚨 Ver Alertas Importantes", expanded=len(alertas_pos) > 0):
    for a in alertas_pos: st.success(a)
    if not alertas_pos: st.info("Sem alertas críticos hoje.")

# --- 7. DASHBOARD VISUAL ---

st.title("Relatório Diário de Vendas")
col1, col2 = st.columns(2)

def style_df(df_in):
    fmt = {"Total": "R${:.2f}", "Quantity": "{:.0f}", "Var. Total": "R${:+.2f}", "Var. Quantity": "{:+.0f}"}
    return df_in.style.format(fmt, na_rep="-")

def plot_metrica(df_t, df_v, id_col, title):
    df_c = pd.concat([df_t, df_v.rename(columns={"Total": "Var. Total", "Quantity": "Var. Quantity"})], axis=1).reset_index()
    df_c.rename(columns={'index': id_col}, inplace=True)
    df_p = df_c.melt(id_vars=id_col, value_vars=['Total', 'Var. Total'], var_name='Métrica', value_name='Valor')
    fig = px.bar(df_p, x=id_col, y='Valor', color='Métrica', barmode='group', title=title, template='plotly_white')
    return fig

with col1:
    st.markdown("### 🏙️ Por Cidade")
    df_cid = pd.concat([relatorio['total_por_cidade'], relatorio['variacao_cidade'].rename(columns={"Total":"Var. Total","Quantity":"Var. Quantity"})], axis=1)
    st.dataframe(style_df(df_cid), use_container_width=True)
    st.plotly_chart(plot_metrica(relatorio['total_por_cidade'], relatorio['variacao_cidade'], 'City', "Vendas x Variação"), use_container_width=True)

    st.markdown("### 👥 Por Gênero")
    df_gen = pd.concat([relatorio['total_por_genero'], relatorio['variacao_genero'].rename(columns={"Total":"Var. Total","Quantity":"Var. Quantity"})], axis=1)
    st.dataframe(style_df(df_gen), use_container_width=True)

with col2:
    st.markdown("### 📦 Por Linha de Produto")
    df_prod = pd.concat([relatorio['total_por_linha_produto'], relatorio['variacao_linha_produto'].rename(columns={"Total":"Var. Total","Quantity":"Var. Quantity"})], axis=1)
    st.dataframe(style_df(df_prod), use_container_width=True)

    st.markdown("### 💳 Por Pagamento")
    df_pay = pd.concat([relatorio['total_por_payment'], relatorio['variacao_payment'].rename(columns={"Total":"Var. Total","Quantity":"Var. Quantity"})], axis=1)
    st.dataframe(style_df(df_pay), use_container_width=True)

st.divider()
st.markdown("### 📊 Distribuições Cruzadas")
c1, c2 = st.columns(2)
with c1:
    st.write("**Cidade x Tipo Cliente**")
    st.dataframe(relatorio["crosstab_cidade_tipo_cliente"], use_container_width=True)
with c2:
    st.write("**Cidade x Gênero x Tipo**")
    st.dataframe(relatorio["crosstab_cidade_genero"], use_container_width=True)
