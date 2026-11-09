"""
gerar_catalogo.py — gera catalogo/catalogo_dados.md a partir de catalogo/catalogo.yaml.

O YAML é a fonte única dos metadados: o mesmo arquivo alimenta este documento e as
verificações automáticas de conformidade do notebook 04_qualidade.

Uso:  python scripts/gerar_catalogo.py
"""
import os
import yaml

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
cat = yaml.safe_load(open(os.path.join(RAIZ, "catalogo", "catalogo.yaml"), encoding="utf-8"))


def dominio(d):
    if "min" in d:
        return f"{d['min']} a {d['max']}"
    if "categorias" in d:
        return ", ".join(f"`{c}`" for c in d["categorias"])
    if "regex" in d:
        return f"padrão `{d['regex']}`"
    return "—"


def esc(t):
    return str(t).replace("|", "\\|")


out = []
f = cat["fonte"]
out += [
    "# Catálogo de Dados",
    "",
    "> Documento gerado automaticamente a partir de [`catalogo.yaml`](catalogo.yaml) por "
    "[`scripts/gerar_catalogo.py`](../scripts/gerar_catalogo.py). Os **domínios** abaixo são expectativas de negócio "
    "definidas *a priori* e são verificados automaticamente no notebook "
    "[`04_qualidade`](../notebooks/04_qualidade.ipynb).",
    "",
    "## 1. Fonte e licença",
    "",
    "| Item | Valor |",
    "|---|---|",
    f"| Conjunto de dados | {f['conjunto']} |",
    f"| Origem | <{f['url']}> |",
    f"| Licença | {f['licenca']}: uso não comercial, com atribuição e compartilhamento pela mesma licença |",
    f"| Período coberto | {f['periodo']} |",
    f"| Coleta | {f['coleta']} |",
    "",
    "## 2. Linhagem (visão geral)",
    "",
    "```mermaid",
    "flowchart LR",
    "  K[(Kaggle<br/>9 CSVs)] -->|01_coleta<br/>validação de contrato| B[BRONZE<br/>9 tabelas<br/>texto + linhagem]",
    "  B -->|02_transformacao<br/>R01–R08| S[SILVER<br/>8 tabelas<br/>tipadas e conciliadas]",
    "  S -->|03_carga_gold<br/>G01–G09| G[GOLD<br/>fato_pedido + 4 dimensões]",
    "  G -->|04_qualidade| Q[gold.dq_*]",
    "  G -->|05_analise| R[gold.resultados_indicadores]",
    "```",
    "",
    "| Tabela Gold | Tabelas Silver de origem | Tabelas Bronze / arquivos de origem |",
    "|---|---|---|",
    "| `fato_pedido` | pedidos, itens_pedido, pagamentos, avaliacoes, geolocalizacao_cep | orders, order_items, order_payments, order_reviews, geolocation |",
    "| `dim_cliente` | clientes, geolocalizacao_cep | customers, geolocation |",
    "| `dim_vendedor` | vendedores, geolocalizacao_cep | sellers, geolocation |",
    "| `dim_produto` | produtos | products, category_translation |",
    "| `dim_tempo` | pedidos (limites de datas) | gerada |",
    "",
    "Toda linha da camada Bronze carrega `_arquivo_origem`, `_data_coleta` e `_run_id`. Os códigos R01–R08 "
    "(transformação) e G01–G09 (modelagem) referem-se às regras documentadas nos notebooks "
    "[`02_transformacao`](../notebooks/02_transformacao.ipynb) e [`03_carga_gold`](../notebooks/03_carga_gold.ipynb).",
    "",
    "## 3. Modelo de dados (Esquema Estrela)",
    "",
    "```mermaid",
    "erDiagram",
    "  dim_tempo    ||--o{ fato_pedido : \"sk_data_compra / prometida / entrega\"",
    "  dim_cliente  ||--o{ fato_pedido : sk_cliente",
    "  dim_vendedor ||--o{ fato_pedido : sk_vendedor",
    "  dim_produto  ||--o{ fato_pedido : sk_produto",
]
for t, meta in cat["tabelas"].items():
    out.append(f"  {t} {{")
    for a, s in meta["atributos"].items():
        tipo = {"numérico": "num", "categórico": "cat", "texto": "text", "data": "date",
                "booleano": "bool", "chave": "key"}[s["tipo"]]
        pk = " PK" if a == meta["chave"] else (" FK" if a.startswith("sk_") else "")
        out.append(f"    {tipo} {a}{pk}")
    out.append("  }")
out += ["```", "", "## 4. Dicionário por tabela", ""]

for t, meta in cat["tabelas"].items():
    out += [f"### `{t}`", "", meta["descricao"], "", f"**Chave:** `{meta['chave']}`", "",
            "| Atributo | Tipo | Descrição | Domínio esperado | Obrigatório | Significado do nulo | Linhagem |",
            "|---|---|---|---|---|---|---|"]
    for a, s in meta["atributos"].items():
        out.append(f"| `{a}` | {s['tipo']} | {esc(s['descricao'])} | {esc(dominio(s.get('dominio', {})))} | "
                   f"{'sim' if s['obrigatorio'] else 'não'} | {esc(s.get('nulo', '—'))} | {esc(s['linhagem'])} |")
    out.append("")

with open(os.path.join(RAIZ, "catalogo", "catalogo_dados.md"), "w", encoding="utf-8") as fh:
    fh.write("\n".join(out))
print("catalogo/catalogo_dados.md gerado:", sum(len(m["atributos"]) for m in cat["tabelas"].values()), "atributos")
