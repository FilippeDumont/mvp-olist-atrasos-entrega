# Atrasos de entrega no e-commerce brasileiro: um MVP de pipeline de dados em nuvem

**Universidade de Brasília · Departamento de Engenharia de Produção**
**Aluno:** Filippe Dumont Lana (202024526) · **Professor:** André Luiz Marques Serrano
**Plataforma:** Google Colab (processamento) + Google Drive (persistência) · PySpark 3.5 + Delta Lake 3.2

[![Abrir o pipeline no Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FilippeDumont/mvp-olist-atrasos-entrega/blob/main/notebooks/00_pipeline.ipynb)

> **Resumo.** Este MVP constrói um pipeline de dados ponta a ponta (coleta → Bronze → Silver → Gold →
> qualidade → análise) sobre cerca de 100 mil pedidos do marketplace Olist, para testar uma hipótese
> de negócio: *o atraso na entrega é o principal fator de insatisfação do cliente e se concentra em
> padrões que podem ser gerenciados.* Os resultados confirmam a maior parte da hipótese:
> - o atraso multiplica por **18** a chance de nota baixa;
> - os atrasos se concentram no **Nordeste e no Rio de Janeiro**, em **picos de demanda** e em
>   **choques externos**;
> - **72%** dos atrasos acontecem mesmo com o vendedor postando no prazo, o que aponta o
>   **transporte** como a principal alavanca de gestão.

---

## Sumário

1. [Objetivo](#1-objetivo)
2. [Plataforma e arquitetura](#2-plataforma-e-arquitetura)
3. [Busca e coleta dos dados](#3-busca-e-coleta-dos-dados)
4. [Modelagem](#4-modelagem)
5. [Carga (pipeline ETL)](#5-carga-pipeline-etl)
6. [Análise](#6-análise)
7. [Autoavaliação](#7-autoavaliação)
8. [Como reproduzir](#8-como-reproduzir)
9. [Estrutura do repositório](#9-estrutura-do-repositório)
10. [Ética, licenças e proteção de dados](#10-ética-licenças-e-proteção-de-dados)
11. [Referências](#11-referências)

---

## 1. Objetivo

> Esta seção foi escrita **antes** da construção do pipeline e de qualquer análise. Conforme a
> orientação do trabalho, ela se mantém **íntegra**: as perguntas e hipóteses não foram alteradas
> após os resultados, inclusive as que acabaram refutadas, e o atingimento de cada uma é discutido na
> [Autoavaliação](#7-autoavaliação).

### 1.1 Problema

Em um marketplace, a empresa que vende **não controla** o estoque, que é dos vendedores parceiros,
nem a entrega, que é das transportadoras. Mesmo assim, é ela que o cliente avalia. Quando o pedido
atrasa, quem recebe a nota baixa é o marketplace, o que afeta reputação, recompra e posicionamento
nos canais de venda.

O MVP busca responder se o atraso **de fato** destrói a satisfação e se ele é **aleatório** ou se
**concentra em padrões** (onde, quando, em qual elo da cadeia) que permitam agir.

### 1.2 Hipótese de negócio

> **H:** *O atraso na entrega é o principal fator de insatisfação do cliente e se concentra em rotas,
> períodos e elos da cadeia identificáveis. Portanto, é gerenciável com dados.*

**Métrica-chave:** **taxa de atraso**, o percentual de pedidos entregues depois da data prometida
ao cliente, e o seu efeito sobre a **nota de avaliação** (de 1 a 5).

### 1.3 Perguntas de negócio

| # | Pergunta | Hipótese específica | Métrica | Decisão que apoia |
|---|---|---|---|---|
| **P1** | Quanto o atraso custa em satisfação? | Pedidos atrasados recebem notas significativamente piores | Nota média e % de notas 1–2, com e sem atraso | Priorizar (ou não) o prazo como alavanca de satisfação |
| **P2** | Onde o atraso se concentra (UF, região, rota, distância)? | Rotas longas e destinos no Norte e Nordeste atrasam mais | Taxa de atraso por recorte geográfico | Onde negociar acordos de nível de serviço e trocar transportadoras |
| **P3** | Quando o atraso acontece? Está ligado a picos de demanda? | Meses de alto volume saturam a logística | Taxa de atraso mensal × volume | Planejamento de capacidade para datas comerciais |
| **P4** | Quem causa o atraso: o vendedor (postagem) ou o transporte? | O atraso nasce no vendedor, que posta depois do prazo | Participação de cada elo nos atrasos e no tempo excedente | Cobrar vendedores ou gerir transportadoras |
| **P5** | O prazo prometido ao cliente está calibrado? | A promessa é folgada demais e mal distribuída | Folga (prometido − real); simulação de regra alternativa | Como definir a promessa de prazo |
| **P6** | Dá para prever o risco de atraso no momento da compra? *(exploratória)* | Distância, região, época e frete explicam parte do risco | AUC e *lift* fora da amostra | Triagem de pedidos de risco e alerta proativo |

### 1.4 Desenho do MVP (por que isto é um MVP)

| Dimensão | Pergunta | Evidência neste trabalho |
|---|---|---|
| Viabilidade técnica | A solução pode ser construída com a tecnologia disponível? | Pipeline executado ponta a ponta no Colab, com dados persistidos no Google Drive ([evidências](#57-evidências-de-execução-na-nuvem)) |
| Viabilidade financeira | É economicamente sustentável? | Custo zero de infraestrutura: Colab gratuito e Drive com 15 GB gratuitos para 126 MB brutos e 120 MB no lakehouse |
| Desejabilidade | É o que o "usuário", a operação do marketplace, precisa? | Perguntas ligadas a decisões concretas, respondidas com dados e um plano de ação ([6.3](#63-discussão-geral)) |

---

## 2. Plataforma e arquitetura

O trabalho usa o **Google Colab** como ambiente de processamento, como pedido na disciplina. Como a
máquina do Colab é **apagada** ao fim de cada sessão, o lakehouse fica **persistido no Google Drive**,
em tabelas **Delta Lake**. Cada notebook lê do Drive o que o anterior gravou, o que demonstra na
prática que os dados sobrevivem entre sessões.

```mermaid
flowchart LR
  K[(Kaggle<br/>9 CSVs)] -->|01 Coleta<br/>contrato de dados| B[BRONZE<br/>cópia fiel + linhagem]
  B -->|02 Transformação<br/>regras R01–R08| S[SILVER<br/>tipado e conciliado]
  S -->|03 Carga<br/>regras G01–G09| G[GOLD<br/>esquema estrela]
  G --> Q[04 Qualidade<br/>gold.dq_*]
  G --> A[05 Análise<br/>P1–P6]
  subgraph Drive["Google Drive · MyDrive/mvp_olist (persistência em nuvem)"]
    B
    S
    G
    Q
  end
```

| Componente | Escolha | Justificativa |
|---|---|---|
| Processamento | Google Colab + **PySpark 3.5** | Requisito da disciplina; o Spark mantém o pipeline escalável e com SQL nativo |
| Formato de armazenamento | **Delta Lake 3.2** | Transações ACID, esquema garantido e **histórico de versões** de cada carga (*time travel*) |
| Persistência | **Google Drive** (`MyDrive/mvp_olist/`) | Nuvem gratuita e integrada ao Colab; a persistência é comprovada pelas evidências e pelo histórico de versões Delta |
| Arquitetura | **Medalhão** (Bronze → Silver → Gold) | Separa dado bruto, dado tratado e dado analítico, o que garante rastreabilidade |
| Orquestração | Notebook [`00_pipeline`](notebooks/00_pipeline.ipynb) + log Delta | Executa as etapas em sequência e registra etapa, destino, linhas, duração e status |

**Estratégia de escrita:** cada tabela é gravada primeiro no disco da VM, onde o Delta Lake tem a
renomeação atômica que o protocolo de commit exige, e em seguida sincronizada com o Drive. Os arquivos
Delta são imutáveis, então a sincronização preserva o histórico de versões. Detalhes em
[`scripts/common.py`](scripts/common.py).

---

## 3. Busca e coleta dos dados

### 3.1 Conjunto de dados

| Item | Valor |
|---|---|
| Nome | *Brazilian E-Commerce Public Dataset by Olist* |
| Fonte | Kaggle: <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce> |
| Licença | **CC BY-NC-SA 4.0**: uso não comercial, com atribuição e compartilhamento pela mesma licença |
| Conteúdo | ~100 mil pedidos (set/2016 a out/2018) de vários marketplaces brasileiros, em 9 tabelas relacionadas |
| Formato original | 9 arquivos **CSV** (UTF-8, com cabeçalho) |
| Volume | **126,2 MB**, **1.550.922 linhas** no total |
| Data da coleta | **09/11/2026** (registrada em `raw/manifesto_coleta.json` e na coluna `_data_coleta` da Bronze) |

A base foi escolhida junto com o tema (logística de e-commerce), por ser uma das poucas bases públicas
brasileiras que reúnem, no nível do pedido, a **data prometida**, as **datas de cada etapa logística**
(aprovação, postagem e entrega), a **localização** de cliente e vendedor e a **avaliação** do cliente.
Essas são exatamente as variáveis que as perguntas P1–P6 exigem.

### 3.2 Procedimento de coleta ([`01_coleta`](notebooks/01_coleta.ipynb))

1. **Download:** primeiro tenta a fonte oficial, via biblioteca `kagglehub`. Sem acesso ao Kaggle,
   usa um espelho público dos mesmos arquivos.
2. **Validação contra um contrato de dados:** cada arquivo precisa ter **exatamente** o número de
   linhas e as colunas oficiais. Qualquer divergência interrompe o pipeline.
3. **Manifesto:** registra data da coleta, fonte usada, tamanho, **SHA-256**, linhas e colunas de
   cada arquivo.
4. **Persistência:** os CSVs originais ficam em `raw/` no Drive, e uma cópia fiel vai para a camada
   **Bronze**, com todas as colunas como texto e as colunas de linhagem `_arquivo_origem`,
   `_data_coleta` e `_run_id`.

| Arquivo | Tabela Bronze | Linhas | Conteúdo |
|---|---|---:|---|
| `olist_orders_dataset.csv` | `orders` | 99.441 | Pedidos: status e datas de compra, aprovação, postagem, entrega e promessa |
| `olist_customers_dataset.csv` | `customers` | 99.441 | Clientes: prefixo de CEP, cidade e UF |
| `olist_order_items_dataset.csv` | `order_items` | 112.650 | Itens: produto, vendedor, prazo-limite de postagem, preço e frete |
| `olist_order_payments_dataset.csv` | `order_payments` | 103.886 | Pagamentos: tipo, parcelas e valor |
| `olist_order_reviews_dataset.csv` | `order_reviews` | 99.224 | Avaliações: nota de 1 a 5 e comentário |
| `olist_products_dataset.csv` | `products` | 32.951 | Produtos: categoria, dimensões e peso |
| `olist_sellers_dataset.csv` | `sellers` | 3.095 | Vendedores: prefixo de CEP, cidade e UF |
| `olist_geolocation_dataset.csv` | `geolocation` | 1.000.163 | Prefixo de CEP → latitude e longitude |
| `product_category_name_translation.csv` | `category_translation` | 71 | Tradução das categorias |

---

## 4. Modelagem

### 4.1 Modelo dimensional: Esquema Estrela

O modelo da camada Gold é um **esquema estrela** cujo **grão** é **1 linha por pedido**: os 99.441
pedidos, de todos os status. As análises de prazo filtram os pedidos entregues de forma válida
(`flag_entregue_valido`). A `dim_tempo` é uma dimensão de **papel múltiplo** (*role-playing*): a
mesma tabela atende às datas de compra, de promessa e de entrega.

![Esquema estrela](catalogo/modelo_estrela.png)

| Tabela | Linhas | Papel |
|---|---:|---|
| `fato_pedido` | 99.441 | Medidas de valor, prazo, atraso, distância e satisfação (30 atributos) |
| `dim_cliente` | 99.441 | Cliente do pedido e sua localização (UF, região, coordenadas) |
| `dim_vendedor` | 3.096 | Vendedor principal e sua localização (inclui o membro "desconhecido") |
| `dim_produto` | 32.952 | Produto principal: categoria, peso e dimensões (inclui "desconhecido") |
| `dim_tempo` | 801 | Calendário contínuo de set/2016 a nov/2018 (inclui "desconhecido") |

**Decisões de modelagem (regras G01–G09)**, detalhadas no notebook [`03_carga_gold`](notebooks/03_carga_gold.ipynb):

| Regra | Decisão |
|---|---|
| G01 | Pedidos com vários itens ou vendedores são atribuídos ao item de **maior preço** ("vendedor/produto principal"); o total de vendedores fica em `qtd_vendedores` |
| G02 | Os 775 pedidos sem itens apontam para o **membro desconhecido** (`sk = -1`), o que preserva a integridade referencial |
| G03 | **Atraso** = data da entrega > data prometida (a promessa é uma *data*, sem horário) |
| G04 | **Postagem atrasada** = postagem depois do maior prazo-limite de postagem dos itens do pedido |
| G05 | **Distância** em linha reta (fórmula de Haversine) entre os centroides de CEP do vendedor e do cliente |
| G06 | Durações em dias decimais, nulas quando o pedido tem inconsistência temporal |
| G07 | UF → macrorregião do IBGE |
| G08 | Chaves substitutas determinísticas; a `dim_tempo` usa `aaaammdd` |
| G09 | Pagamento: soma dos valores, tipo de maior valor e máximo de parcelas |

### 4.2 Catálogo de Dados

O catálogo completo, com **69 atributos**, está em **[`catalogo/catalogo_dados.md`](catalogo/catalogo_dados.md)**.
Para cada atributo ele traz tipo, descrição, **domínio esperado** (mínimo e máximo, ou categorias
válidas), obrigatoriedade, significado do nulo e **linhagem** (coluna de origem e regras aplicadas).

O catálogo é escrito uma única vez em [`catalogo/catalogo.yaml`](catalogo/catalogo.yaml) e cumpre
duas funções:
1. gera o documento legível (`scripts/gerar_catalogo.py`);
2. é lido pelo notebook de qualidade, que **verifica automaticamente** cada atributo contra o
   domínio declarado ([6.1](#61-qualidade-dos-dados)).

Assim, o catálogo deixa de ser só documentação e passa a ser um **contrato verificável**.

---

## 5. Carga (pipeline ETL)

### 5.1 Etapas

| Notebook | Etapa | Entrada → Saída | Abrir |
|---|---|---|---|
| [`00_pipeline`](notebooks/00_pipeline.ipynb) | **Orquestrador**: executa 01→05 e registra o log | — | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FilippeDumont/mvp-olist-atrasos-entrega/blob/main/notebooks/00_pipeline.ipynb) |
| [`01_coleta`](notebooks/01_coleta.ipynb) | Extração e validação | Kaggle → `raw/` + **Bronze** (9 tabelas) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FilippeDumont/mvp-olist-atrasos-entrega/blob/main/notebooks/01_coleta.ipynb) |
| [`02_transformacao`](notebooks/02_transformacao.ipynb) | Transformação | Bronze → **Silver** (8 tabelas) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FilippeDumont/mvp-olist-atrasos-entrega/blob/main/notebooks/02_transformacao.ipynb) |
| [`03_carga_gold`](notebooks/03_carga_gold.ipynb) | Modelagem e carga | Silver → **Gold** (estrela) + verificações | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FilippeDumont/mvp-olist-atrasos-entrega/blob/main/notebooks/03_carga_gold.ipynb) |
| [`04_qualidade`](notebooks/04_qualidade.ipynb) | Qualidade dos dados | Silver/Gold → `gold.dq_*` | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FilippeDumont/mvp-olist-atrasos-entrega/blob/main/notebooks/04_qualidade.ipynb) |
| [`05_analise`](notebooks/05_analise.ipynb) | Respostas P1–P6 | Gold → gráficos + `gold.resultados_indicadores` | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FilippeDumont/mvp-olist-atrasos-entrega/blob/main/notebooks/05_analise.ipynb) |

### 5.2 Regras de transformação (Bronze → Silver)

| Regra | Transformação | Por que importa para os resultados |
|---|---|---|
| R01 | **Tipagem**: texto → `timestamp`, `date`, `double` e `int` | Sem tipagem não há como calcular prazos |
| R02 | **Prefixo de CEP** como texto de 5 dígitos, com zeros à esquerda | Evita perder CEPs como `01001` e falhar a geolocalização |
| R03 | **Padronização**: UF em maiúsculas; cidade e categoria em minúsculas e sem espaços extras | Evita grupos duplicados por grafia |
| R04 | Colunas **renomeadas** para o português e erros de digitação da origem corrigidos (`lenght`) | Legibilidade |
| R05 | **Geolocalização**: descarta 42 coordenadas fora do Brasil; **mediana** por prefixo de CEP (1.000.163 → 19.010 linhas) | Distâncias corretas e robustas a valores extremos |
| R06 | **Deduplicação das avaliações**: 1 por pedido, a de resposta mais recente (99.224 → 98.673) | Evita contar notas em dobro na fato |
| R07 | **Produtos**: peso ou dimensão 0 → nulo; categoria ausente → `sem_categoria`; tradução manual de 2 categorias | Acurácia e completude |
| R08 | **Consistência temporal**: 203 pedidos com datas contraditórias são **sinalizados** (`dq_*`), não apagados | Excluídos das métricas de prazo, sem perder rastreabilidade |
| A01 | **Recorte de análise**: compras de jan/2017 a ago/2018, os 20 meses completos | Os meses das pontas têm volume residual ou pedidos ainda em trânsito ([6.1](#61-qualidade-dos-dados)) |

### 5.3 Junções e conciliação entre conjuntos de dados

A tabela fato reúne **seis conjuntos de dados** da origem. Como eles têm grãos diferentes, cada um é
primeiro **agregado ao grão do pedido** e só depois unido, o que evita duplicar linhas:

| Conjunto (Silver) | Grão original | Como entra na `fato_pedido` | Tipo de junção |
|---|---|---|---|
| `pedidos` | 1 por pedido | Base da fato: status, datas e flags de consistência | — |
| `itens_pedido` | 1 por item | Agregado por pedido: nº de itens e vendedores, soma de preço e frete, maior prazo-limite de postagem. O item de maior preço define o vendedor e o produto principais (G01) | `LEFT` (775 pedidos sem itens) |
| `pagamentos` | 1 por pagamento | Agregado por pedido: soma dos valores, tipo de maior valor, máximo de parcelas (G09) | `LEFT` (1 pedido sem pagamento) |
| `avaliacoes` | 1 por avaliação | Já deduplicado para 1 por pedido (R06) | `LEFT` (768 pedidos sem avaliação) |
| `clientes` / `vendedores` | 1 por cliente-pedido / vendedor | Chaves substitutas das dimensões e UF para a flag `mesma_uf` | `LEFT` |
| `geolocalizacao_cep` | 1 por prefixo de CEP | Coordenadas do cliente e do vendedor para a distância (G05) | `LEFT` (CEPs sem coordenada → distância nula) |

Todas as junções são `LEFT` a partir de `pedidos`, para que nenhum pedido se perca. A verificação
"linhas da fato = pedidos da Silver" (abaixo) comprova que nenhuma junção duplicou ou eliminou pedidos.

### 5.4 Verificações automáticas de carga

O notebook 03 **interrompe o pipeline** se alguma das verificações falhar:

| Verificação | Resultado |
|---|---|
| Linhas da fato = pedidos da Silver (unicidade do grão) | ✅ 99.441 = 99.441 |
| `pedido_id` único na fato | ✅ |
| Integridade referencial: 7 chaves estrangeiras (cliente, vendedor, produto e 3 datas) | ✅ 0 órfãos |

### 5.5 Log de execução

Cada etapa grava uma linha na tabela Delta `logs/pipeline_log`, com `run_id`, etapa, destino, linhas,
duração e status. Uma execução completa produz **25 registros**, um por tabela gravada, e leva de
5 a 15 minutos, dependendo da máquina do Colab.

### 5.6 Persistência na nuvem

Estrutura final no Google Drive (`MyDrive/mvp_olist/`):

```
mvp_olist/
├── raw/        (126 MB)  CSVs originais + manifesto_coleta.json
├── bronze/     ( 65 MB)  9 tabelas Delta
├── silver/     ( 40 MB)  8 tabelas Delta
├── gold/       ( 16 MB)  5 tabelas do modelo + 2 de qualidade + 1 de indicadores
├── logs/                 pipeline_log (Delta)
└── evidencias/graficos/  8 gráficos PNG
```

### 5.7 Evidências de execução na nuvem

<!-- EVIDENCIAS -->
*Seção em atualização: as capturas da execução no Google Colab e da persistência no Google Drive serão adicionadas em [`evidencias/colab/`](evidencias/colab/).*

---

## 6. Análise

### 6.1 Qualidade dos dados

A análise completa está em [`04_qualidade`](notebooks/04_qualidade.ipynb). Ela cobre **todos os
atributos** da camada Silver (perfil de completude, cardinalidade e faixa) e verifica
**automaticamente os 69 atributos** da Gold contra o Catálogo de Dados.

| Dimensão | Principais achados | Tratamento |
|---|---|---|
| **Completude** | As datas ausentes correspondem a fases não atingidas (cancelado, em trânsito). Os 610 produtos sem categoria são os mesmos sem nome, descrição e fotos. Título e comentário das avaliações são opcionais (88% e 59% nulos). 279 CEPs de clientes não têm coordenada. A cidade da base de geolocalização foi descartada na Silver (grafia inconsistente e redundante) | Ausências legítimas documentadas no catálogo; categoria `sem_categoria`; distância nula só afeta a P2 |
| **Unicidade** | Todas as chaves das camadas Silver e Gold são únicas. Na origem: 814 `review_id` repetidos, 551 avaliações a mais em pedidos já avaliados e 261.831 linhas duplicadas na geolocalização | R06 (1 avaliação por pedido) e R05 (agregação por CEP) |
| **Consistência** | 0 registros órfãos entre tabelas. 203 pedidos (0,20%) com datas contraditórias, como postagem antes da compra (166) e entrega antes da postagem (23) | R08 sinaliza; G06 exclui das métricas de prazo |
| **Conformidade** | 11 de 69 atributos têm algum valor fora do domínio declarado. Cinco são de conformidade: cidades de vendedor com UF, país, CEP ou até e-mail no lugar do nome; pagamento `not_defined`; parcelas = 0. Os outros seis são prazos fora da faixa esperada (ver Acurácia) | Os atributos com problema de conformidade não entram nas perguntas; documentados |
| **Acurácia** | 42 coordenadas fora do Brasil; peso 0; prazo-limite de postagem em 2020; prazos extremos, como entregas acima de 120 dias (0,04%) | R05, R07; os prazos extremos são atrasos reais e foram mantidos, com uso de **medianas, percentuais e taxas**, robustos a extremos |
| **Atualidade** | As pontas do período estão incompletas: set–dez/2016 com poucos pedidos e set–out/2018 com só 20 pedidos, nenhum entregue. Os dados são de 2016–2018 | **A01:** análise de jan/2017 a ago/2018; atualidade discutida na autoavaliação |

**Conclusão da qualidade:** a base tem problemas reais, mas **localizados e tratáveis**. Os atributos
centrais das perguntas (datas de compra, postagem, entrega e promessa; nota; UF) estão completos e
coerentes em mais de 99,8% dos pedidos. Os problemas de conformidade estão em atributos que não entram
na análise, e os prazos extremos são tratados com estatísticas robustas.

### 6.2 Respostas às perguntas de negócio

Base de análise: **96.019 pedidos entregues** (jan/2017 a ago/2018) · **taxa de atraso geral: 6,80%**.
Todas as tabelas, testes e gráficos estão no notebook [`05_analise`](notebooks/05_analise.ipynb).

#### P1 · Quanto o atraso custa em satisfação? → **hipótese confirmada**

| Entrega | Pedidos avaliados | Nota média | % notas 1–2 | % notas 5 |
|---|---:|---:|---:|---:|
| No prazo | 89.001 | **4,29** | 9,2% | 62,3% |
| Atrasado | 6.376 | **2,27** | **62,4%** | 16,5% |

![P1](evidencias/graficos/p1_nota_baixa_por_atraso.png)

- O atraso multiplica o risco de nota baixa por **6,8**. Os testes de Mann-Whitney e qui-quadrado
  dão p < 0,001.
- A relação é **dose-resposta**: com 1 a 3 dias de atraso, 32% de notas baixas; com 4 a 7 dias, 68%;
  com mais de uma semana, cerca de 80%.
- O cliente **pune o atraso, mas não premia a antecipação**: entregar 15 dias antes rende a mesma
  nota que entregar 1 semana antes, com cerca de 9% de notas baixas.
- Na **regressão logística**, controlando valor, participação do frete, número de itens e de vendedores e região, o
  atraso multiplica a chance de nota baixa por **18,3** (IC 95%: 17,3–19,4). É o maior efeito do
  modelo, à frente de pedidos com vários vendedores (3,5) ou vários itens (2,7).
- Depois de controlar o atraso, a região do cliente **perde relevância**: a insatisfação regional
  passa *pelo* atraso.

#### P2 · Onde o atraso se concentra? → **parcialmente confirmada: a distância importa, mas o destino importa além dela**

![P2 por UF](evidencias/graficos/p2_atraso_por_uf.png)

| Faixa de distância | < 100 km | 100–500 | 500–1.000 | 1.000–2.000 | ≥ 2.000 km |
|---|---:|---:|---:|---:|---:|
| Taxa de atraso | 4,5% | 6,1% | 7,2% | 9,5% | **12,1%** |

- **Por região:** o Nordeste tem **12,8%** de atraso, o dobro do Sudeste (6,1%). O Norte tem 8,6%.
- **Por distância:** o gradiente é claro (qui-quadrado, p < 0,001), mas a correlação individual é
  fraca (ρ = 0,07).
- **Na mesma faixa de distância (1.000–2.000 km)**, o Nordeste atrasa **12,9%** contra **5,8%** do
  Sudeste. O problema está no **destino**, ou seja, na última milha, e não apenas na distância.
- **O Rio de Janeiro surpreende:** mesmo perto do polo vendedor, de onde saem 83,5% dos pedidos, tem
  **12,2%** de atraso e responde sozinho por **22,9% de todos os atrasos**, quase o mesmo que São Paulo
  (27,8%), que recebe três vezes mais pedidos.

![P2 rota](evidencias/graficos/p2_atraso_por_rota.png)

#### P3 · Quando o atraso acontece? → **parcialmente confirmada: o volume explica um pico, mas não todos**

![P3](evidencias/graficos/p3_volume_e_atraso_mensal.png)

- Volume mensal e taxa de atraso têm correlação **moderada** (Spearman ρ = 0,57; p = 0,009).
- **Black Friday 2017:** na semana de 20 a 26/11, foram **416 pedidos por dia, contra 190** no resto de
  novembro. A taxa de atraso subiu para **17,3%, contra 9,2%**, com postagem e transporte mais lentos.
  É saturação de capacidade, como a hipótese previa.
- **Fevereiro e março de 2018** tiveram os maiores picos (**14,1% e 19,0%**) com volume parecido ao de
  janeiro, quando a taxa foi de só 5,7%. O tempo de postagem ficou estável e o de **transporte** subiu de 10,5 para cerca de
  13 dias: um choque externo. Em março, o pico coincide com a **greve nacional dos Correios iniciada
  em 12/03/2018** (Agência Brasil, 2018).

#### P4 · Quem causa o atraso? → **hipótese refutada: o principal elo é o transporte**

| Postagem pelo vendedor | Pedidos | Taxa de atraso | % de todos os atrasos |
|---|---:|---:|---:|
| No prazo | 87.448 | 5,4% | **72,3%** |
| Atrasada | 8.570 | **21,1%** | 27,7% |

![P4](evidencias/graficos/p4_decomposicao_do_prazo.png)

- Em **72%** dos atrasos, o vendedor postou o pacote **no prazo**.
- Dos ~23 dias a mais que um pedido atrasado leva, **19,9 estão no transporte** e só 3,0 na postagem.
  O transporte responde por **87% do tempo excedente**.
- O vendedor ainda importa: postar atrasado **quase quadruplica** o risco de atraso (21,1% contra 5,4%). Esses pedidos são só
  8,9% do total, um alvo pequeno e de alto retorno.

#### P5 · O prazo prometido está calibrado? → **parcialmente confirmada: a folga é grande e mal distribuída, mas não é desperdício**

![P5 folga](evidencias/graficos/p5_distribuicao_folga.png)

- A promessa mediana é de **24 dias** para uma entrega real mediana de **10 dias**. **79%** dos pedidos
  chegam com uma semana ou mais de antecedência.
- A folga está **mal distribuída**: as UFs com menos folga atrasam mais (ρ = −0,56; p = 0,003).
  Alagoas tem 10 dias de folga e **21,5%** de atraso; Amazonas tem 20 dias de folga e **2,8%**.
- **Resultado contra a hipótese.** Numa simulação **fora da amostra** (regra calibrada em 2017 e
  testada em 2018), prometer menos dias **sempre** custa mais atrasos. Uma regra por UF com o mesmo
  prazo médio da Olist (~23 dias) teria praticamente o mesmo atraso: cerca de 7,6%, contra 7,75%.
  A folga alta é o **preço da variabilidade do transporte**.

![P5 fronteira](evidencias/graficos/p5_fronteira_prazo_atraso.png)

#### P6 · Dá para prever o risco de atraso na compra? *(exploratória)* → **resultado moderado: serve para triagem**

| Modelo (treino 2017 → teste jan–ago/2018) | AUC-ROC | Precisão média | Atraso no decil de maior risco | Lift |
|---|---:|---:|---:|---:|
| **Regressão logística** | **0,673** | 0,142 | **15,8%** | **2,0×** |
| Gradient boosting | 0,611 | 0,118 | 14,2% | 1,8× |
| *Taxa-base do teste* | — | 0,077 | 7,7% | 1,0× |

![P6](evidencias/graficos/p6_lift_por_decil.png)

- O decil de maior risco previsto atrasa **o dobro da média**, e o de menor risco atrasa só **1,0%**.
  Isso basta para **priorizar** pedidos, mas não para prever cada um.
- Os sinais mais fortes são a sazonalidade (novembro e dezembro), a rota (mesma UF protege) e o destino
  Nordeste.
- O limite do modelo são os choques externos, como a greve de 2018, que nenhuma variável disponível
  no ato da compra antecipa.

### 6.3 Discussão geral

A hipótese central foi **confirmada em sua maior parte**, com nuances que mudam a recomendação:

1. **O atraso é o principal fator de insatisfação medido.** Ele multiplica por cerca de 18 a chance de
   nota baixa, e a antecipação não compensa (P1).
2. **O atraso não é aleatório.** Ele se concentra em **destinos** (Nordeste e RJ, mesmo com distâncias
   iguais), em **períodos** (Black Friday e choques externos) e em **um elo**, o transporte (P2–P4).
3. **O atraso é gerenciável, mas não pelo caminho intuitivo.** O vendedor não é o principal culpado
   (P4) e a promessa de prazo já está perto do ótimo para uma regra simples (P5). A alavanca é
   **reduzir a variabilidade do transporte** nas rotas críticas, com a previsão de risco (P6) ajudando
   a priorizar onde agir.

| Prioridade | Ação recomendada | Evidência |
|---|---|---|
| 1 | Acordo de nível de serviço com transportadoras por destino, focado em **RJ e Nordeste** | P2, P4 |
| 2 | **Plano de capacidade** para datas comerciais (Black Friday) | P3 |
| 3 | Promessa de prazo **por rota e época**, com folga proporcional à variabilidade | P5, P6 |
| 4 | **Alerta proativo** ao cliente nos pedidos do decil de maior risco | P1, P6 |
| 5 | Cobrança do prazo de postagem aos vendedores reincidentes | P4 |

---

## 7. Autoavaliação

**1. Quais perguntas foram respondidas e quais não foram? Por quê?**
As seis perguntas foram respondidas com dados, mas com graus diferentes de confirmação, e os
resultados contrários estão registrados:

| Pergunta | Situação | Comentário |
|---|---|---|
| P1 | ✅ Confirmada | Efeito forte, robusto a controles e com dose-resposta |
| P2 | ◐ Parcialmente | A distância importa, mas o achado principal (destino, RJ) não estava previsto |
| P3 | ◐ Parcialmente | O volume explica a Black Friday, mas não fev–mar/2018. Esse pico só foi explicado com uma fonte externa, e o de fevereiro segue em aberto |
| P4 | ✗ Refutada | A hipótese apontava o vendedor; os dados apontam o transporte |
| P5 | ◐ Parcialmente | A folga existe e está mal distribuída, mas não é "desperdício" |
| P6 | ◐ Moderada | Útil para triagem (lift 2×), insuficiente para prever pedido a pedido |

**2. Que limitações dos dados condicionaram os resultados?**
- **Não há a transportadora** de cada pedido. Por isso, a P4 identifica o *elo* (transporte), mas não
  qual empresa atrasa. É a principal lacuna para transformar o achado em ação.
- **Atualidade:** os dados são de 2016–2018. O padrão logístico atual pode ser outro.
- **A distância é em linha reta** entre centroides de prefixo de CEP, não a rota rodoviária.
- **A nota mede tudo**, inclusive a qualidade do produto. A associação com o atraso é robusta, mas
  observacional: não prova causalidade.
- **Uma única empresa:** generalizar para o e-commerce brasileiro como um todo exige cautela.
- A regra que a Olist usa para calcular a data prometida não é conhecida, o que limita a P5.

**3. O que faria diferente se recomeçasse?**
- O volume (cerca de 100 mil pedidos) caberia em **pandas ou DuckDB**. O Spark foi mantido pela
  escalabilidade e pela aderência à disciplina, mas acrescenta cerca de 1 a 2 minutos de
  inicialização no Colab.
- Definiria o **recorte temporal (A01)** já na etapa de objetivo, e não depois da análise de qualidade.
- Incluiria desde o início a análise de **texto dos comentários** para confirmar que as notas baixas
  falam de entrega.

**4. Que extensões transformariam este MVP em uma solução de uso contínuo?**
- **Carga incremental** com `MERGE` do Delta Lake e agendamento, em vez da carga completa a cada execução.
- Incorporar **dados de transportadora e rastreio** para atribuir o atraso a cada operador.
- Publicar a **promessa de prazo dinâmica** (por rota e época) e o **escore de risco** da P6 como
  serviço, com monitoramento de *drift*.
- Um **painel** de taxa de atraso por rota, atualizado diariamente, com alertas para choques como greves.
- **Mineração de texto** dos comentários para separar a insatisfação logística da relacionada ao produto.

---

## 8. Como reproduzir

1. Abra o orquestrador no Colab: [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FilippeDumont/mvp-olist-atrasos-entrega/blob/main/notebooks/00_pipeline.ipynb)
2. *Ambiente de execução → Executar tudo.*
3. Autorize o acesso ao Google Drive quando solicitado.
4. Ao final (5 a 15 minutos), o lakehouse estará em `MyDrive/mvp_olist/` e o log e o histórico de
   versões aparecem no fim do notebook.

Cada notebook (01 a 05) também pode ser aberto e executado **isoladamente** em uma sessão nova. Ele
lê do Drive o que a etapa anterior persistiu. Nenhuma credencial é necessária, porque o espelho público
é usado quando o Kaggle não está acessível. O catálogo é regenerado com `python scripts/gerar_catalogo.py`.

---

## 9. Estrutura do repositório

```
├── README.md                    ← este documento (objetivo, método, resultados, autoavaliação)
├── LICENSE                      ← MIT (código) + registro da licença dos dados
├── notebooks/
│   ├── 00_pipeline.ipynb        ← orquestrador (executa 01→05 e mostra log e persistência)
│   ├── 01_coleta.ipynb          ← extração, contrato de dados, Bronze
│   ├── 02_transformacao.ipynb   ← regras R01–R08, Silver
│   ├── 03_carga_gold.ipynb      ← esquema estrela, regras G01–G09, verificações
│   ├── 04_qualidade.ipynb       ← qualidade por atributo e conformidade ao catálogo
│   └── 05_analise.ipynb         ← P1–P6, testes estatísticos, gráficos, discussão
├── scripts/
│   ├── common.py                ← Spark + Delta, persistência no Drive, log do pipeline
│   ├── graficos.py              ← padrão visual dos gráficos
│   └── gerar_catalogo.py        ← gera o catálogo a partir do YAML
├── catalogo/
│   ├── catalogo.yaml            ← fonte única dos metadados (69 atributos)
│   ├── catalogo_dados.md        ← Catálogo de Dados (domínios e linhagem)
│   ├── modelo_estrela.png       ← diagrama do modelo
│   └── linhagem.png             ← diagrama da linhagem
└── evidencias/
    ├── graficos/                ← 8 gráficos das respostas (P1–P6)
    └── colab/                   ← capturas da execução no Colab e da persistência no Drive
```

---

## 10. Ética, licenças e proteção de dados

- **Licença dos dados:** CC BY-NC-SA 4.0. O uso aqui é **acadêmico e não comercial**, com atribuição
  à Olist. Os dados brutos **não são versionados** neste repositório; o notebook `01_coleta` reproduz
  a coleta.
- **Dados pessoais (LGPD, Lei nº 13.709/2018):** a base é **anonimizada na origem**. Clientes,
  vendedores e pedidos aparecem como *hashes*, a localização vai só até o prefixo de 5 dígitos do CEP
  e nomes de empresas foram substituídos na origem. Nenhum procedimento de reidentificação foi tentado,
  e os comentários livres das avaliações não são exibidos.
- **Dados corporativos:** não foram usados.
- **Licença do código:** MIT (ver [`LICENSE`](LICENSE)).

---

## 11. Referências

- OLIST. *Brazilian E-Commerce Public Dataset by Olist.* Kaggle, 2018. Disponível em: <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>.
- AGÊNCIA BRASIL. *Saiba o que fazer para ficar com contas em dia durante greve dos Correios.* 12 mar. 2018. Disponível em: <https://agenciabrasil.ebc.com.br/geral/noticia/2018-03/saiba-o-que-fazer-para-ficar-com-contas-em-dia-durante-greve-dos-correios>.
- KIMBALL, R.; ROSS, M. *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling.* 3. ed. Wiley, 2013.
- ARMBRUST, M. et al. Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores. *Proceedings of the VLDB Endowment*, v. 13, n. 12, 2020.
- RIES, E. *The Lean Startup.* Crown Business, 2011.
- BRASIL. Lei nº 13.709, de 14 de agosto de 2018 (Lei Geral de Proteção de Dados Pessoais).
