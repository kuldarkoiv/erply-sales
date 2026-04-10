"""
Erply Sales – budget_erply_sales upsert
========================================
Allikas:  PostgreSQL public.v_erply_sales
          + public.trader_mapper_master_trader_mapper_table
Periood:  alates YEAR_START (env muutuja, vaikimisi 2026-01-01)
Väljund:  public.budget_erply_sales (upsert, võti: invoice_id + product_id)

Käivitamine:
    python main.py

Keskkonnamuutujad (.env või DO App Platform):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
    YEAR_START   – valikuline, vaikimisi 2026-01-01
"""

import os
import re
import datetime
import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── SEADED ────────────────────────────────────────────────────────────────────
YEAR_START  = os.environ.get("YEAR_START", "2026-01-01")
TARGET_TABLE = "public.budget_erply_sales"

# ── ÄRIGRUPI KAARDISTUS ───────────────────────────────────────────────────────
# product_group_id → ärigrupp (algtase, enne ümberklassifitseerimist)
#
# Höövel  – kõik hööveldatud ja saetud puidutooted, sh WP_PACK ja WP_C
# Ferm    – fermid (konstruktsioonid)
# Värv    – Töötlused (21): värvimisteenus; + klapivad Höövel-read (rea-tasandil!)
# Transport – transporditeenused
# Liimpuit  – liimitud puit (eraldi kategooria)
# Muu     – kõik muu (teenused, kinnitusvahendid, immutus jms)
#
# Erand: Terrassilauad (9) ja Tugevussorteeritud höövelpruss (7)
#        jäävad ALATI Höövel alla, isegi kui arvel on Töötlused.

HOOVEL_IDS    = {2, 4, 6, 7, 8, 9, 22, 26, 30}
FERM_IDS      = {11}
VARV_IDS      = {21}
TRANSPORT_IDS = {24}
LIIMPUIT_IDS  = {10}

ALWAYS_HOOVEL = {7, 9}  # Tugevussorteeritud höövelpruss, Terrassilauad

# ── DB ÜHENDUS ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        dbname=os.environ.get("POSTGRES_DB"),
        sslmode="require",
    )

# ── ÄRIGRUPI KAARDISTUS ───────────────────────────────────────────────────────

def map_arigrupp_base(gid: int) -> str:
    if gid in HOOVEL_IDS:    return "Höövel"
    if gid in FERM_IDS:      return "Ferm"
    if gid in VARV_IDS:      return "Värv"
    if gid in TRANSPORT_IDS: return "Transport"
    if gid in LIIMPUIT_IDS:  return "Liimpuit"
    return "Muu"

# ── VÄRVI ÜMBERKLASSIFITSEERIMINE ─────────────────────────────────────────────
# Höövel-rida läheb Värvi alla ainult siis, kui samal arvel on Töötlus-rida
# sama ristlõikega (nt "21x145" ↔ "21x145x5700"). Ristlõige = esimene NxM
# muster product_name-s. Töötluse ristlõige loetakse osast enne sulgusid.

def _first_cross(name: str):
    m = re.search(r"\d+x\d+", name or "")
    return m.group(0) if m else None

def _tootlus_cross(name: str):
    before = (name or "").split("(")[0]
    hits = re.findall(r"\d+x\d+", before)
    return hits[-1] if hits else None

def build_varv_lookup(df: pd.DataFrame) -> dict:
    tootlus = df[df["product_group_id"] == 21].copy()
    tootlus["rc"] = tootlus["product"].apply(_tootlus_cross)
    return (
        tootlus[tootlus["rc"].notna()]
        .groupby("invoice_id")["rc"]
        .apply(set)
        .to_dict()
    )

def reclassify_row(row: pd.Series, varv_lookup: dict) -> str:
    ag = row["business_group"]
    if ag != "Höövel":
        return ag
    if row["product_group_id"] in ALWAYS_HOOVEL:
        return "Höövel"
    rcs = varv_lookup.get(row["invoice_id"])
    if not rcs:
        return "Höövel"
    rc = _first_cross(row["product"])
    return "Värv" if (rc and rc in rcs) else "Höövel"

# ── ANDMETE LAADIMINE ─────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql(
            """
            SELECT
                s.invoice_id,
                s.document_date,
                s.invoice_custom_number   AS invoice_number,
                s.warehouse_value,
                s.sales_profit            AS profit,
                s.profit_percent          AS margin_pct,
                s.product_id,
                s.product_name            AS product,
                s.product_group_id,
                s.product_group_name      AS product_group,
                s.customer_name           AS customer,
                s.sold_qty                AS qty,
                s.net_sales_total         AS revenue,
                s.sold_m3                 AS m3,
                s.m3_sales_price,
                s.m3_warehouse_price,
                s.authorid                AS author_id,
                s.attendant_name          AS attendant,
                s.sold_linear_meters      AS linear_m,
                t.name                    AS trader,
                t.is_export,
                t.country                 AS market
            FROM public.v_erply_sales s
            LEFT JOIN public.trader_mapper_master_trader_mapper_table t
                ON s.authorid = t.erply_author_id
            WHERE s.document_date >= %(year_start)s
            """,
            conn,
            params={"year_start": YEAR_START},
        )
    finally:
        conn.close()
    return df

# ── KLASSIFITSEERIMINE ────────────────────────────────────────────────────────

def classify(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["business_group"] = df["product_group_id"].apply(map_arigrupp_base)
    varv_lookup = build_varv_lookup(df)
    df["business_group"] = df.apply(reclassify_row, axis=1, varv_lookup=varv_lookup)
    df["document_date"] = df["document_date"].dt.date
    # is_export: teisenda boolean-iks (NaN → None)
    df["is_export"] = df["is_export"].apply(
        lambda x: None if pd.isna(x) else bool(x)
    )
    return df

# ── TABEL + GRANT ─────────────────────────────────────────────────────────────

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    invoice_id        INTEGER     NOT NULL,
    product_id        INTEGER     NOT NULL,
    document_date     DATE,
    invoice_number    VARCHAR,
    business_group    VARCHAR,
    product_group_id  INTEGER,
    product_group     VARCHAR,
    product           VARCHAR,
    customer          VARCHAR,
    qty               NUMERIC(18,4),
    revenue           NUMERIC(18,4),
    profit            NUMERIC(18,4),
    margin_pct        NUMERIC(18,4),
    m3                NUMERIC(18,4),
    linear_m          NUMERIC(18,4),
    warehouse_value   NUMERIC(18,4),
    m3_sales_price    NUMERIC(18,4),
    m3_warehouse_price NUMERIC(18,4),
    trader            VARCHAR,
    market            VARCHAR,
    is_export         BOOLEAN,
    author_id         INTEGER,
    attendant         VARCHAR,
    PRIMARY KEY (invoice_id, product_id)
);
GRANT SELECT ON {TARGET_TABLE} TO doadmin;
"""

ALTER_COLS = [
    "qty", "revenue", "profit", "margin_pct", "m3",
    "linear_m", "warehouse_value", "m3_sales_price", "m3_warehouse_price",
]

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
        for col in ALTER_COLS:
            cur.execute(
                f"ALTER TABLE {TARGET_TABLE} ALTER COLUMN {col} TYPE NUMERIC(18,4);"
            )
    conn.commit()

# ── UPSERT ────────────────────────────────────────────────────────────────────

COLS = [
    "invoice_id", "product_id", "document_date", "invoice_number",
    "business_group", "product_group_id", "product_group", "product",
    "customer", "qty", "revenue", "profit", "margin_pct",
    "m3", "linear_m", "warehouse_value", "m3_sales_price", "m3_warehouse_price",
    "trader", "market", "is_export", "author_id", "attendant",
]

UPDATE_COLS = [c for c in COLS if c not in ("invoice_id", "product_id")]

UPSERT_SQL = f"""
    INSERT INTO {TARGET_TABLE} ({", ".join(COLS)})
    VALUES %s
    ON CONFLICT (invoice_id, product_id) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in UPDATE_COLS)}
"""

def upsert(df: pd.DataFrame, conn):
    # Agregeeri üle võimalike duplikaatide (sama invoice_id + product_id võib
    # esineda mitu korda lähteandmetes eri pikkuste/partiidega).
    # Numbrilised veerud summeeritakse, tekst võetakse esimesest reast.
    num_cols  = ["qty", "revenue", "profit", "m3", "linear_m", "warehouse_value",
                 "m3_sales_price", "m3_warehouse_price"]
    text_cols = [c for c in COLS if c not in ["invoice_id","product_id"] + num_cols]

    agg = {c: "sum" for c in num_cols}
    agg.update({c: "first" for c in text_cols})
    df_agg = df.groupby(["invoice_id","product_id"], as_index=False).agg(agg)

    rows = [tuple(row) for row in df_agg[COLS].itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=500)
    conn.commit()
    return len(rows)

# ── PEAFUNKTSIOON ─────────────────────────────────────────────────────────────

def main():
    ts = lambda: datetime.datetime.now().strftime("%H:%M:%S")

    print(f"[{ts()}] Laeb andmeid alates {YEAR_START} ...")
    df = load_data()
    print(f"[{ts()}] {len(df)} rida laetud. Klassifitseerin ...")
    df = classify(df)

    conn = get_conn()
    try:
        print(f"[{ts()}] Loon tabeli (kui pole olemas) ...")
        ensure_table(conn)
        print(f"[{ts()}] Upsert -> {TARGET_TABLE} ...")
        n = upsert(df, conn)
    finally:
        conn.close()

    print(f"[{ts()}] Valmis. {n} unikaalset rida upsertitud ({len(df)} algsest).")

if __name__ == "__main__":
    main()
