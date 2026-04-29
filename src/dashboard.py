import os
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from plotly.subplots import make_subplots

st.set_page_config(
    page_title="Dashboard Financeiro B3",
    page_icon="📈",
    layout="wide",
)

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": "data_warehouse",
    "user": "dados_user",
    "password": "dados_pass",
}

PERIODOS = {
    "1 Mês": 30,
    "3 Meses": 90,
    "6 Meses": 180,
    "1 Ano": 365,
}


def _conectar():
    return psycopg2.connect(**POSTGRES_CONFIG)


@st.cache_data(ttl=300)
def listar_tickers() -> list[str]:
    with _conectar() as pg:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ticker FROM gold.metricas_ativos ORDER BY ticker"
            )
            return [r[0] for r in cur.fetchall()]


@st.cache_data(ttl=300)
def carregar_dados(ticker: str, dias: int) -> pd.DataFrame:
    data_inicio = date.today() - timedelta(days=dias)
    with _conectar() as pg:
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT
                    data, abertura, fechamento, maxima, minima, volume,
                    retorno_pct, media_movel_7d, media_movel_30d
                FROM gold.metricas_ativos
                WHERE ticker = %s
                  AND data >= %s
                ORDER BY data
                """,
                (ticker, data_inicio),
            )
            rows = cur.fetchall()

    return pd.DataFrame(
        rows,
        columns=[
            "data", "abertura", "fechamento", "maxima", "minima",
            "volume", "retorno_pct", "media_movel_7d", "media_movel_30d",
        ],
    )


def _cor_vela(fechamento, abertura) -> str:
    return "rgba(0,200,100,0.7)" if fechamento >= abertura else "rgba(220,50,50,0.7)"


def construir_grafico(df: pd.DataFrame, ticker: str) -> go.Figure:
    datas = pd.to_datetime(df["data"])
    cores_vol = [_cor_vela(c, o) for c, o in zip(df["fechamento"], df["abertura"])]

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.72, 0.28],
        vertical_spacing=0.03,
    )

    fig.add_trace(
        go.Candlestick(
            x=datas,
            open=df["abertura"],
            high=df["maxima"],
            low=df["minima"],
            close=df["fechamento"],
            name=ticker,
            increasing_line_color="#00c864",
            decreasing_line_color="#dc3232",
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=datas,
            y=df["media_movel_7d"],
            name="MM 7d",
            line=dict(color="#f0b429", width=1.5),
            hovertemplate="MM 7d: R$ %{y:.2f}<extra></extra>",
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=datas,
            y=df["media_movel_30d"],
            name="MM 30d",
            line=dict(color="#a78bfa", width=1.5),
            hovertemplate="MM 30d: R$ %{y:.2f}<extra></extra>",
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Bar(
            x=datas,
            y=df["volume"],
            name="Volume",
            marker_color=cores_vol,
            showlegend=False,
            hovertemplate="%{x|%d/%m/%Y}<br>Volume: %{y:,.0f}<extra></extra>",
        ),
        row=2, col=1,
    )

    BG = "#0e1117"
    GRID = "#1e2430"

    fig.update_layout(
        height=600,
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        font=dict(color="#fafafa", size=12),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        margin=dict(l=10, r=10, t=10, b=10),
    )

    fig.update_xaxes(
        rangebreaks=[dict(bounds=["sat", "mon"])],
        gridcolor=GRID,
        showgrid=True,
        tickformat="%d/%m/%y",
    )
    fig.update_yaxes(gridcolor=GRID, showgrid=True, zerolinecolor=GRID)
    fig.update_yaxes(tickprefix="R$ ", row=1, col=1)
    fig.update_yaxes(rangemode="nonnegative", row=2, col=1)

    return fig


def _formatar_tabela(df: pd.DataFrame) -> pd.DataFrame:
    out = df.tail(20).sort_values("data", ascending=False).copy()
    out["data"] = out["data"].astype(str)
    for col in ("abertura", "fechamento", "maxima", "minima", "media_movel_7d", "media_movel_30d"):
        out[col] = out[col].map(lambda x: f"R$ {x:.2f}")
    out["retorno_pct"] = out["retorno_pct"].map(lambda x: f"{x:+.2f}%")
    out["volume"] = out["volume"].map(lambda x: f"{int(x):,}")
    out.columns = [
        "Data", "Abertura", "Fechamento", "Máxima", "Mínima",
        "Volume", "Retorno %", "MM 7d", "MM 30d",
    ]
    return out


def main():
    st.title("📈 Dashboard Financeiro B3")

    try:
        tickers = listar_tickers()
    except Exception as e:
        st.error(f"Não foi possível conectar ao banco de dados: {e}")
        st.info("Execute o pipeline de ingestão primeiro (`python src/bronze.py`).")
        return

    if not tickers:
        st.warning("Nenhum dado disponível. Execute o pipeline de ingestão primeiro.")
        return

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Filtros")
        ticker = st.selectbox("Ativo", tickers)
        periodo_label = st.selectbox("Período", list(PERIODOS.keys()), index=1)
        dias = PERIODOS[periodo_label]
        st.divider()
        st.caption("Dados atualizados via Airflow às 18h (seg–sex)")

    df = carregar_dados(ticker, dias)

    if df.empty:
        st.warning(f"Sem dados para **{ticker}** no período selecionado.")
        return

    ultimo = df.iloc[-1]
    primeiro = df.iloc[0]
    retorno_periodo = (
        (ultimo["fechamento"] - primeiro["fechamento"]) / primeiro["fechamento"] * 100
    )

    # ── Métricas ──────────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Último fechamento", f"R$ {ultimo['fechamento']:.2f}")
    col2.metric(
        f"Retorno ({periodo_label})",
        f"{retorno_periodo:+.2f}%",
        delta=f"{retorno_periodo:.2f}%",
    )
    col3.metric("Máxima no período", f"R$ {df['maxima'].max():.2f}")
    col4.metric("Mínima no período", f"R$ {df['minima'].min():.2f}")
    col5.metric("Volume médio", f"{df['volume'].mean():,.0f}")

    st.divider()

    # ── Gráfico ───────────────────────────────────────────────────────────────
    st.plotly_chart(construir_grafico(df, ticker), use_container_width=True)

    # ── Tabela detalhada ──────────────────────────────────────────────────────
    with st.expander("Dados recentes (últimos 20 pregões)"):
        st.dataframe(_formatar_tabela(df), hide_index=True, use_container_width=True)


if __name__ == "__main__":
    main()
