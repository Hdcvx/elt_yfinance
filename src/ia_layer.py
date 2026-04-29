import psycopg2
import os
from anthropic import Anthropic
from loguru import logger


POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "data_warehouse",
    "user": "dados_user",
    "password": "dados_pass",
}


def buscar_dados(ultimos_dias: int = 30, ticker: str = None) -> str:
    pg = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = pg.cursor()

    if ticker:
        cursor.execute(
            """
            SELECT ticker, data, fechamento, retorno_pct, media_movel_7d, media_movel_30d
            FROM gold.metricas_ativos
            WHERE data >= CURRENT_DATE - make_interval(days => %s)
              AND ticker = %s
            ORDER BY ticker, data DESC
            LIMIT 100
            """,
            (ultimos_dias, ticker),
        )
    else:
        cursor.execute(
            """
            SELECT ticker, data, fechamento, retorno_pct, media_movel_7d, media_movel_30d
            FROM gold.metricas_ativos
            WHERE data >= CURRENT_DATE - make_interval(days => %s)
            ORDER BY ticker, data DESC
            LIMIT 100
            """,
            (ultimos_dias,),
        )

    registros = cursor.fetchall()
    cursor.close()
    pg.close()

    if not registros:
        return "Sem dados disponíveis."

    linhas = ["ticker | data | fechamento | retorno_pct | mm7d | mm30d"]
    linhas.append("-" * 70)
    for r in registros:
        linhas.append(f"{r[0]} | {r[1]} | {r[2]:.2f} | {r[3]:.2f}% | {r[4]:.2f} | {r[5]:.2f}")

    return "\n".join(linhas)


def perguntar(pergunta: str) -> str:
    logger.info(f"Pergunta: {pergunta}")

    dados = buscar_dados(ultimos_dias=30)

    cliente = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    resposta = cliente.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system="""Você é um analista financeiro.
Responda perguntas sobre os ativos com base nos dados fornecidos.
Seja direto e use os números reais dos dados.""",
        messages=[{
            "role": "user",
            "content": f"Dados dos últimos 30 dias:\n\n{dados}\n\nPergunta: {pergunta}"
        }]
    )

    return resposta.content[0].text


if __name__ == "__main__":
    perguntas = [
        "Qual ativo teve o maior retorno nos últimos 30 dias?",
        "O FIIB11 está acima ou abaixo da média móvel de 30 dias?",
        "Compare a volatilidade entre FIIB11 e VALE3",
    ]

    for pergunta in perguntas:
        print(f"\nPergunta: {pergunta}")
        print(f"Resposta: {perguntar(pergunta)}")
        print("-" * 60)