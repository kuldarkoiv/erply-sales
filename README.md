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

Töötlused (grupp 21) on värvimisteenus, mis müüakse koos Höövel-materjaliga. Höövel-rida klassifitseeritakse ümber `Värv`-iks ainult siis kui:

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


---

## Käivitamine

```bash
pip install -r requirements.txt
python main.py
```

Genereerib faili `erply_myyk_YYYY.xlsx` jooksukausta.

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
| `OUTPUT_PATH` | Exceli väljundfaili tee | `erply_myyk_YYYY.xlsx` |

### `.env.example`

```
POSTGRES_HOST=db-xxx.b.db.ondigitalocean.com
POSTGRES_PORT=25060
POSTGRES_USER=kuldar
POSTGRES_PASSWORD=
POSTGRES_DB=defaultdb
YEAR_START=2026-01-01
```

---

## Andmeallikad (PostgreSQL)

### `public.v_erply_sales`
Põhivaade Erply müügiandmetega. Iga rida on üks arve-toode kombinatsioon.

Olulisemad veerud:

| Veerg | Kirjeldus |
|---|---|
| `invoice_id` / `invoice_custom_number` | Arve identifikaator |
| `document_date` | Arve kuupäev |
| `product_id` / `product_name` | Toode |
| `product_group_id` / `product_group_name` | Erply tootegrupp |
| `customer_name` / `customerid` | Klient |
| `net_sales_total` | Käive (€) |
| `sales_profit` | Kasum (€) |
| `sold_m3` | Müüdud kogus m³ |
| `sold_linear_meters` | Müüdud jooksvad meetrid |
| `authorid` | Müügimehe Erply ID |

### `public.trader_mapper_master_trader_mapper_table`
Müügimeeste kaardistustabel. Joindatakse `authorid = erply_author_id` alusel.

Kasutatavad veerud: `name` (kuvaniminimi), `is_export` (kas eksportmüük).

---

## Ärigrupi klassifitseerimise loogika

### 1. Algtaseme kaardistus (`product_group_id` → ärigrupp)

| Ärigrupp | product_group_id-d | Sisaldab |
|---|---|---|
| **Höövel** | 2, 4, 6, 7, 8, 9, 22, 26, 30 | Höövelmaterjal, Saematerjal, Välis/Sisevoodrilauad, Terrassilauad, Tugevussorteeritud höövelpruss, WP_PACK, WP_C, II kvaliteet |
| **Ferm** | 11 | Fermid |
| **Värv** | 21 | Töötlused (värvimisteenus) |
| **Transport** | 24 | Transport |
| **Liimpuit** | 10 | Liimpuit |
| **Muu** | 1, 5, 13, 25, 29 | Teenused, Kinnitusvahendid, Immutus jms |

### 2. Värvi rea-tasandi ümberklassifitseerimine

Töötlused (grupp 21) on alati **värvimisteenus** mis käib koos Höövel-materjaliga samal arvel. Et aru saada, milline Höövel-rida on seotud Värv-töötlusega, kasutatakse ristlõike matchimist:

```
Töötlus: "Natural Opaque Premium 21x145 (krunt 4 külge + 2x värv)"  → ristlõige = "21x145"
Höövel:  "Välisvoodrilaud UYS/S saepinnaga 21x145x5700"             → ristlõige = "21x145"
                                                        ↑ MATCH → Höövel rida → Värv
```

**Reegel:** Höövel-rida läheb Värvi alla ainult siis, kui:
1. Samal arvel (`invoice_id`) on vähemalt üks Töötlus-rida
2. Töötluse ristlõige (AxB enne sulgusid) = Höövel-rea ristlõige (esimene AxB nimest)

**Erandid – jäävad ALATI Höövel alla:**
- Tugevussorteeritud höövelpruss (`product_group_id = 7`)
- Terrassilauad (`product_group_id = 9`)

---

## Exceli lehed

| Leht | Sisu |
|---|---|
| **1. Kokkuvõte** | Käive, kasum, m³, jooksvad m ärigrupi kaupa + totaal |
| **2. Kuutrend** | Käive ja marginaal kuu × ärigrupp |
| **3. Tootegrupid** | Drill-down ärigrupp → tootegrupp |
| **4. Kliendid** | Kõik kliendid käibe järgi, ärigrupi lõikes |
| **5. Värv-arved** | Pivot: iga Värv-arve juures Höövel_käive vs Värv_käive |
| **6. Värv-detail** | Värv-arved ridade kaupa ärigrupi lõikes |
| **7. Kõik read** | Kogu toortabel koos Trader ja Export väljadega |

---

## DigitalOcean Job seadistus

- **Run command:** `python main.py`
- **Environment variables:** seada DO konsoolis (vt tabel ülal)
- **Schedule:** kord päevas / kord nädalas vastavalt vajadusele
- Väljundfail kirjutatakse `OUTPUT_PATH` asukohta – soovi korral mount DO Spaces bucket

---

## Projekti struktuur

```
erply-sales/
├── main.py           # Peaprogramm
├── requirements.txt  # Python sõltuvused
├── .gitignore
├── .env              # Lokaalne konfiguratsioon (ei lähe gitti)
├── .env.example      # Näidis konfiguratsioon
└── README.md
```
