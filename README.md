---
repo: erply-sales
description: "Müügiandmete pipeline Erply → PostgreSQL"
tags: [repo, pipeline, erply, müük]
status: active
---

# erply-sales

Automaatne müügiandmete pipeline Erply → PostgreSQL. Laeb `public.v_erply_sales` andmed, klassifitseerib ärigruppidesse ja upsertib `public.budget_erply_sales` tabelisse.

---

## Käivitamine

```bash
pip install -r requirements.txt
python main.py
```

---

## Keskkonnamuutujad

Loo `.env` fail (vt `.env.example`) või sea DO App Platform keskkonnamuutujad:

| Muutuja | Kirjeldus | Vaikimisi |
|---|---|---|
| `POSTGRES_HOST` | DB host | – |
| `POSTGRES_PORT` | DB port | `5432` |
| `POSTGRES_USER` | DB kasutaja | – |
| `POSTGRES_PASSWORD` | DB parool | – |
| `POSTGRES_DB` | Andmebaasi nimi | – |
| `YEAR_START` | Periood alates (YYYY-MM-DD) | `2026-01-01` |

---

## Andmeallikad (PostgreSQL)

### `public.v_erply_sales`
Põhivaade Erply müügiandmetega. Iga rida on üks arve × toode kombinatsioon.

### `public.trader_mapper_master_trader_mapper_table`
Müügimeeste kaardistustabel. Join: `authorid = erply_author_id`.
Kasutatavad veerud: `name` → `trader`, `is_export`, `country` → `market`.

---

## Väljund: `public.budget_erply_sales`

Tabel luuakse automaatselt kui see puudub. Upsert-võti: `(invoice_id, product_id)`.

| Veerg | Tüüp | Kirjeldus |
|---|---|---|
| `invoice_id` | INTEGER | Arve ID (PK) |
| `product_id` | INTEGER | Toote ID (PK) |
| `document_date` | DATE | Arve kuupäev |
| `invoice_number` | VARCHAR | Arve number (custom) |
| `business_group` | VARCHAR | Ärigrupp (vt klassifitseerimise loogika) |
| `product_group_id` | INTEGER | Erply tootegrupi ID |
| `product_group` | VARCHAR | Erply tootegrupi nimi |
| `product` | VARCHAR | Toote nimi |
| `customer` | VARCHAR | Kliendi nimi |
| `qty` | NUMERIC(18,4) | Kogus |
| `revenue` | NUMERIC(18,4) | Käive (€) |
| `profit` | NUMERIC(18,4) | Kasum (€) |
| `margin_pct` | NUMERIC(18,4) | Kasumimarginaal (%) |
| `m3` | NUMERIC(18,4) | Kogus m³ |
| `linear_m` | NUMERIC(18,4) | Jooksvad meetrid |
| `warehouse_value` | NUMERIC(18,4) | Ladu väärtus |
| `m3_sales_price` | NUMERIC(18,4) | Müügihind m³ |
| `m3_warehouse_price` | NUMERIC(18,4) | Laohind m³ |
| `trader` | VARCHAR | Müügimees (trader_mapper.name) |
| `market` | VARCHAR | Turg/riik (trader_mapper.country) |
| `is_export` | BOOLEAN | Kas eksportmüük |
| `author_id` | INTEGER | Erply author ID |
| `attendant` | VARCHAR | Töötaja nimi |
| `finishing` | VARCHAR | Finishing nimetus (Erply v_erply_sales.finishing) |

---

## Ärigrupi klassifitseerimise loogika

### 1. Algtaseme kaardistus (`product_group_id` → `business_group`)

| business_group | product_group_id-d | Sisaldab |
|---|---|---|
| **Höövel** | 2, 4, 6, 7, 8, 9, 22, 26, 30 | Höövelmaterjal, Saematerjal, Välis/Sisevoodrilauad, Terrassilauad, Tugevussorteeritud höövelpruss, WP_PACK, WP_C, II kvaliteet |
| **Ferm** | 11 | Fermid |
| **Värv** | 21 | Töötlused (värvimisteenus) |
| **Transport** | 24 | Transport |
| **Liimpuit** | 10 | Liimpuit |
| **Muu** | 1, 5, 13, 25, 29 | Teenused, Kinnitusvahendid, Immutus jms |

### 2. Värvi rea-tasandi ümberklassifitseerimine

Töötlused (grupp 21) on värvimisteenus, mis müüakse koos Höövel-materjaliga. Höövel-rida klassifitseeritakse ümber `Värv`-iks kui **üks** järgmistest tingimustest on täidetud:

**a) Finishing** – Höövel-real on `finishing` väli täidetud (v.a ALWAYS_HOOVEL erandid)

**b) Ristlõige** – samal arvel (`invoice_id`) on Töötlus-rida ja ristlõiked kattuvad:

1. Samal arvel (`invoice_id`) on Töötlus-rida
2. Töötluse ristlõige (AxB enne sulgusid) = Höövel-rea ristlõige (esimene AxB nimest)

```
Töötlus: "Natural Opaque Premium 21x145 (krunt 4 külge + 2x värv)"  → rc = "21x145"
Höövel:  "Välisvoodrilaud UYS/S saepinnaga 21x145x5700"             → rc = "21x145"
                                                        ↑ MATCH -> Höövel rida -> Värv
```

**Erandid – jäävad alati `Höövel` alla:**
- Tugevussorteeritud höövelpruss (`product_group_id = 7`)
- Terrassilauad (`product_group_id = 9`)

### 3. Duplikaatide käsitlus

Sama `(invoice_id, product_id)` võib lähteandmetes esineda mitu korda (eri pikkused/partii). Numbrilised veerud (qty, revenue, profit, m3, linear_m jms) summeeritakse, tekstveerud võetakse esimesest reast.

---

## DigitalOcean Job seadistus

- **Run command:** `python main.py`
- **Environment variables:** seada DO konsoolis
- **Schedule:** iga päev (nt kell 06:00 UTC)

---

## Projekti struktuur

```
erply-sales/
├── main.py           # Peaprogramm: lae → klassifitseeri → upsert
├── requirements.txt
├── .gitignore
├── .env              # Lokaalne konfiguratsioon (ei lähe gitti)
├── .env.example      # Näidis konfiguratsioon
└── README.md
```
