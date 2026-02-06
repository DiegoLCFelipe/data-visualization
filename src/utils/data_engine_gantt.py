
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

ETAPAS = [
    "solicitacao",
    "aprovacao_solicitacao",
    "rfq",
    "geracao_prepedido",
    "aprovacao_prepedido",
    "geracao_ordem_compra",
    "emissao_nota_entrada",
]

N_PROCESSOS = 200
start_base = datetime(2025, 1, 1)
delta_specs = {
    "solicitacao": (1, 24*2),                # 1h a 2 dias
    "aprovacao_solicitacao": (2, 24*3),      # 2h a 3 dias
    "rfq": (4, 24*5),                        # 4h a 5 dias
    "geracao_prepedido": (1, 24*2),
    "aprovacao_prepedido": (2, 24*3),
    "geracao_ordem_compra": (1, 24*2),
    "emissao_nota_entrada": (4, 24*10),      # mais demorado
}

rows = []
counters = {
    "solicitacao": 100000,
    "aprovacao_solicitacao": 200000,
    "rfq": 300000,
    "geracao_prepedido": 400000,
    "aprovacao_prepedido": 500000,
    "geracao_ordem_compra": 600000,
    "emissao_nota_entrada": 700000,
}

for compra_id in range(1, N_PROCESSOS + 1):
    base = start_base + timedelta(days=int(np.random.uniform(0, 365)))
    current_dt = base + timedelta(hours=int(np.random.uniform(8, 17)))  # horário comercial
    ids = {etapa: counters[etapa] for etapa in ETAPAS}

    for etapa in ETAPAS:
        rows.append({
            "compra_id": compra_id,
            "etapa": etapa,
            "etapa_id": ids[etapa],
            "data_etapa": current_dt,
        })

        lo, hi = delta_specs[etapa]
        delta_h = int(np.random.uniform(lo, hi))
        current_dt = current_dt + timedelta(hours=delta_h)

        if current_dt.hour < 8:
            current_dt = current_dt.replace(hour=8, minute=0, second=0, microsecond=0)
        if current_dt.hour > 18:
            current_dt = (current_dt + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)

    for etapa in ETAPAS:
        counters[etapa] += 1

dados = pd.DataFrame(rows)

etapa_order = {e:i for i,e in enumerate(ETAPAS)}
dados["ordem_etapa"] = dados["etapa"].map(etapa_order)
dados = dados.sort_values(["compra_id", "ordem_etapa"]).reset_index(drop=True)

dados["data_proxima_etapa"] = dados.groupby("compra_id")["data_etapa"].shift(-1)
dados["lead_time_h"] = (dados["data_proxima_etapa"] - dados["data_etapa"]).dt.total_seconds() / 3600

inicio = dados[dados["etapa"]=="solicitacao"].set_index("compra_id")["data_etapa"]
fim = dados[dados["etapa"]=="emissao_nota_entrada"].set_index("compra_id")["data_etapa"]
cycle_time_h = ((fim - inicio).dt.total_seconds() / 3600).rename("cycle_time_h")
dados = dados.merge(cycle_time_h, left_on="compra_id", right_index=True)

dados.to_excel("acompanhamento_compras_sintetico.xlsx", index=False)

print(dados.head(14))
print("Linhas:", len(dados), "| Colunas:", dados.shape[1])
