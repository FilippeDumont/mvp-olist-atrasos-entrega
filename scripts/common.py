"""
common.py — utilitários compartilhados por todos os notebooks do pipeline.

Responsabilidades:
  * detectar o ambiente (Google Colab ou execução local de teste);
  * montar o Google Drive, que é a camada de PERSISTÊNCIA EM NUVEM do lakehouse;
  * criar a sessão Spark com suporte a Delta Lake;
  * ler e gravar tabelas Delta das camadas bronze / silver / gold;
  * registrar o log de execução do pipeline (linhagem operacional).

Estratégia de persistência
--------------------------
O Colab é efêmero: a máquina virtual é apagada ao fim da sessão. Por isso, toda
tabela é gravada primeiro no disco local da VM (rápido e com renomeação atômica,
exigida pelo protocolo de commit do Delta Lake) e, em seguida, SINCRONIZADA para o
Google Drive. Arquivos Delta são imutáveis, então a sincronização apenas adiciona
os arquivos novos de dados e de log: o histórico de versões (time travel) é
preservado no Drive. Ao ler uma tabela em uma sessão nova, ela é restaurada do
Drive para o disco local antes do uso.
"""
from __future__ import annotations

import glob
import json
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone

# --------------------------------------------------------------------------- #
# Ambiente e caminhos
# --------------------------------------------------------------------------- #
IN_COLAB = "google.colab" in sys.modules

PROJECT = "mvp_olist"
if IN_COLAB:
    DRIVE_ROOT = "/content/drive"
    CLOUD_BASE = f"{DRIVE_ROOT}/MyDrive/{PROJECT}"      # persistência em nuvem
    LOCAL_BASE = f"/content/{PROJECT}_work"              # disco da VM (efêmero)
else:  # execução local de teste: simula o Drive em uma pasta
    CLOUD_BASE = os.environ.get("MVP_CLOUD_BASE", os.path.expanduser(f"~/{PROJECT}_cloud"))
    LOCAL_BASE = os.environ.get("MVP_LOCAL_BASE", os.path.expanduser(f"~/{PROJECT}_work"))

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # raiz do repositório
RAW_DIR = f"{CLOUD_BASE}/raw"         # zona de pouso: CSVs originais + manifesto
LAYERS = ("bronze", "silver", "gold")
LOG_TABLE = ("logs", "pipeline_log")

# Fuso: os timestamps da Olist não têm fuso (horário local do Brasil). Usamos UTC
# na sessão Spark para que sejam interpretados "como estão", sem deslocamentos de
# horário de verão (vigente no Brasil até 2019).
SPARK_TZ = "UTC"
os.environ["TZ"] = SPARK_TZ          # o Python também usa UTC (evita deslocar datas no collect)
time.tzset()


def mount_drive() -> None:
    """Monta o Google Drive no Colab (pede autorização ao usuário na 1ª vez)."""
    if IN_COLAB and not os.path.isdir(f"{DRIVE_ROOT}/MyDrive"):
        from google.colab import drive  # type: ignore
        drive.mount(DRIVE_ROOT)
    os.makedirs(CLOUD_BASE, exist_ok=True)
    os.makedirs(LOCAL_BASE, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)


# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
_SPARK = None


def get_spark(app_name: str = "mvp_olist"):
    """Cria (uma única vez) a SparkSession com Delta Lake habilitado."""
    global _SPARK
    if _SPARK is not None:
        return _SPARK
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", SPARK_TZ)
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )
    local_jars = os.environ.get("MVP_LOCAL_JARS")  # apenas para teste offline
    if local_jars:
        builder = builder.config("spark.jars", ",".join(glob.glob(f"{local_jars}/*.jar")))
        spark = builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    _SPARK = spark
    return spark


# --------------------------------------------------------------------------- #
# Leitura e gravação de tabelas Delta (local -> nuvem)
# --------------------------------------------------------------------------- #
def _paths(layer: str, name: str) -> tuple[str, str]:
    return f"{LOCAL_BASE}/{layer}/{name}", f"{CLOUD_BASE}/{layer}/{name}"


def _sync(src: str, dst: str) -> None:
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copytree(src, dst, dirs_exist_ok=True)


def restore(layer: str, name: str) -> str:
    """Garante a cópia local de uma tabela persistida no Drive; devolve o caminho local."""
    local, cloud = _paths(layer, name)
    if not os.path.isdir(f"{local}/_delta_log"):
        if not os.path.isdir(f"{cloud}/_delta_log"):
            raise FileNotFoundError(
                f"Tabela {layer}.{name} não encontrada na nuvem ({cloud}). "
                "Execute antes a etapa que a produz.")
        _sync(cloud, local)
    return local


def write_table(df, layer: str, name: str, mode: str = "overwrite",
                comment: str | None = None) -> int:
    """Grava um DataFrame como tabela Delta e sincroniza com o Drive. Devolve nº de linhas."""
    local, cloud = _paths(layer, name)
    # Se a tabela já existe na nuvem, restaura antes para continuar o histórico de versões.
    if os.path.isdir(f"{cloud}/_delta_log") and not os.path.isdir(f"{local}/_delta_log"):
        _sync(cloud, local)
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if comment:
        writer = writer.option("userMetadata", comment)
    writer.save(local)
    _sync(local, cloud)
    spark = get_spark()
    n = spark.read.format("delta").load(local).count()
    return n


def read_table(layer: str, name: str):
    """Lê uma tabela Delta (restaurando do Drive se a sessão for nova)."""
    return get_spark().read.format("delta").load(restore(layer, name))


def register_views(layer: str, names: list[str]) -> None:
    """Registra tabelas como views temporárias '<layer>_<name>' para uso em SQL."""
    for n in names:
        read_table(layer, n).createOrReplaceTempView(f"{layer}_{n}")


def table_history(layer: str, name: str):
    """Histórico de versões Delta (evidência de cargas)."""
    return get_spark().sql(f"DESCRIBE HISTORY delta.`{restore(layer, name)}`")


# --------------------------------------------------------------------------- #
# Log de execução do pipeline
# --------------------------------------------------------------------------- #
RUN_ID = os.environ.get("MVP_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
os.environ["MVP_RUN_ID"] = RUN_ID


class Stage:
    """Context manager que mede e registra uma etapa no log do pipeline.

    Uso:
        with Stage("02_transformacao", "silver.orders") as st:
            st.rows = write_table(df, "silver", "orders")
    """

    def __init__(self, stage: str, target: str):
        self.stage, self.target, self.rows = stage, target, None

    def __enter__(self):
        self.t0 = time.time()
        self.started = datetime.now(timezone.utc)
        return self

    def __exit__(self, exc_type, exc, tb):
        from pyspark.sql import Row
        status = "SUCESSO" if exc_type is None else f"FALHA: {exc_type.__name__}"
        row = Row(run_id=RUN_ID, etapa=self.stage, destino=self.target,
                  inicio_utc=self.started.replace(tzinfo=None),
                  duracao_s=round(time.time() - self.t0, 2),
                  linhas=int(self.rows) if self.rows is not None else None,
                  status=status, ambiente="colab" if IN_COLAB else "local")
        spark = get_spark()
        schema = ("run_id string, etapa string, destino string, inicio_utc timestamp, "
                  "duracao_s double, linhas long, status string, ambiente string")
        write_table(spark.createDataFrame([row], schema), *LOG_TABLE, mode="append")
        print(f"[{status}] {self.stage} -> {self.target}"
              + (f" | {self.rows:,} linhas" if self.rows is not None else "")
              + f" | {row.duracao_s}s")
        return False  # não suprime exceções


def save_json(obj, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


def banner(title: str) -> None:
    print("=" * 78)
    print(title)
    print(f"Ambiente: {'Google Colab' if IN_COLAB else 'local'} | run_id: {RUN_ID}")
    print(f"Persistência em nuvem: {CLOUD_BASE}")
    print("=" * 78)
