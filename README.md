# gourmetfast — dbt Project

This project transforms raw e-commerce operational data for **GourmetFast** into a clean, reliable, and analytics-ready **star schema** using dbt. The *staging layer* applies a layered dbt design to make the transformation logic both clear and maintainable. Each raw CSV source is modeled as a `stg_*` view focused purely on data hygiene and standardization. Columns are normalized into snake_case, dates and numeric values are cast to appropriate types, and categorical fields such as `status` are standardized by lowercasing and restricting them to an allowed business set (`delivered`, `shipped`, `pending`, `returned`). Data quality issues commonly found in real production systems are handled here: duplicate customer records are resolved using window functions (SCD‑lite), and invalid order records—such as zero or negative quantities—are filtered out. These staging models become the single trusted source for the **core layer**, which implements a star schema. `dim_customers` and `dim_products` store stable descriptive attributes, while `fct_orders` captures order events at the line‑item grain (one row per `order_id` + `product_id`) and includes both base measures (quantity, price) and derived metrics such as revenue. This clear separation between descriptive attributes and business events makes the model intuitive for analysts and supports core use cases like identifying high‑value customers, understanding product performance, and analyzing sales trends.

From a performance and scalability standpoint, the core models are designed to operate like production‑ready pipelines rather than one‑off transformations. All core tables use **incremental materialization**, ensuring dbt processes only new or updated rows based on business timestamps (such as `order_date`) instead of recomputing entire datasets. This approach dramatically reduces compute cost and runtime as data volume grows, and it mirrors best practices used in enterprise data warehouses like Snowflake, Databricks, and BigQuery. A comprehensive data quality layer supports this structure: generic dbt tests (`unique`, `not_null`) enforce integrity on primary attributes, while a custom status‑validation test ensures all orders fall within the defined business domain. Together, incremental processing, layered modeling, and systematic testing create a pipeline that is resilient, scalable, and ready for production usage.

---

## How to Run This Project

### Install dbt + SQLite adapter
```bash
pip install dbt-core dbt-sqlite
```

### Load seed data (CSV files)
```bash
dbt seed
```

### Run dbt models
```bash
dbt run
```

### Run data tests
```bash
dbt test
```

---

## Project Structure

```bash
gourmetfast/
├── data/
│   └── gourmetfast.db
│
├── macros/
│
├── models/
│   ├── core/
│   │   ├── dim_customers.sql
│   │   ├── dim_products.sql
│   │   └── fct_orders.sql
│   │
│   ├── staging/
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   └── stg_orders.sql
│   │
│   ├── tests/
│   │   ├── schema.yml
│   │   └── test_valid_order_status.sql
│   │
│   └── src_sources.yml
│
├── seeds/
│   ├── raw_customers.csv
│   ├── raw_orders.csv
│   └──raw_products.csv
│
├── dbt_project.yml
└── README.md
```
