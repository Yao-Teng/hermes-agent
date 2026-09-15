# 2026-08-08 PostgreSQL 資料倉庫設計（主題 0：倉庫底座深化）

> 日數 8 mod 8 = 0 → PostgreSQL 資料倉庫設計。今日把 8/7 建置技巧日產出的 raw/staging/marts 分層，
> 回頭對照官方文件深化：正規化 vs 星形模型、JSONB、索引策略（B-tree/BRIN/GIN）、物化視圖、TimescaleDB。
> **重大版本更新：PostgreSQL 18 已為現行 stable**（2025-09-25 發布，現行 minor 18.4 = 2026-05-14）。

---

## 1. 今日主題與概念摘要

1. **PostgreSQL 18 是現行 stable（2026-08-08 檢核）**
   - 2025-09-25 發布；官方 versioning policy：18.4 現行 minor，支援至 2030-11-14。
   - 與倉庫設計相關的新特性：**uuidv7()**（時間有序 UUID，減少 B-tree 頁分裂，適合分佈式主鍵）、
     **virtual generated columns 成預設**（查詢時計算，衍生欄位免儲存；STORED 仍可用於要持久化的熱路徑）、
     **NOT NULL ... NOT VALID**（加約束跳過全表掃描、ShareUpdateExclusiveLock 驗證，多 TB 表加約束不再卡 ACCESS EXCLUSIVE）、
     **async I/O**（`io_method` GUC：worker/io_uring/sync，讀取可達 3× 加速，雲端網路儲存受益最大）、
     **EXPLAIN ANALYZE 預設含 BUFFERS**、**skip scan**（多欄 B-tree 跳過前綴欄位直接查後欄）、
     **md5 認證已棄用**（改用 SCRAM）、FTS 改用叢集預設 collation（pg_upgrade 後需重建 FTS/pg_trgm 索引）。
2. **資料倉庫建模：正規化（3NF） vs 星形/雪花模型**
   - 3NF 適合 OLTP（交易寫入、避免重複）；星形模型（事實表 + 維度表）適合 OLAP（分析查詢少 JOIN、可預聚合）。
   - 本平台策略：**raw 原封不動 → staging 正規化清洗 → marts 星形/寬表 → metrics 時間序列**，
     即「寫入用正規化、讀取用星形」的混合倉庫設計。
3. **JSONB：半結構化的正確姿勢（官方 8.14 章）**
   - jsonb = 分解二進位儲存、可索引（GIN/btree/hash）、處理快；json = 原文字、不可索引、每次重解析。
   - 官方建議「多數應用用 jsonb」；文件結構宜固定可預期；**任何 UPDATE 鎖整列**，文件別塞太大。
   - **containment（@>）與 existence（?）運算子只在 jsonb**；GIN 索引加速 `@>` / `?` / `?|` / `?&`。
4. **索引策略三分法（對應 8/7 schema 實作）**
   - **B-tree**：等值/範圍/排序預設選擇；uuidv7() 讓 UUID PK 也享有 B-tree 熱快取。
   - **BRIN（Block Range Index）**：超大表 + 欄位值與物理位置相關（時間戳/流水號）→ 極小索引、區塊範圍跳掃；
     重要參數 **`pages_per_range`**（越小越精確越大顆）與 **`autosummarize`**（預設關閉；開啟後 autovacuum 自動補摘要，
     否則靠 vacuum 或手動 `brin_summarize_new_values()`）。
   - **GIN**：JSONB（`@>`/`?`）、全文檢索 tsvector、陣列；PG18 支援 **平行 GIN 建索引**。
   - 時間序列表另加 **BRIN on (time)** 幾乎是標準配備（比 B-tree 省 99% 空間）。
5. **物化視圖：預聚合的正確用法**
   - `REFRESH MATERIALIZED VIEW [CONCURRENTLY]`：CONCURRENTLY 不鎖讀取，但**必須先有「純欄位名 + 無 WHERE + 含全列」的 UNIQUE 索引**、且視圖需已填充；不可與 WITH NO DATA 並用；同一視圖同時只允許一個 REFRESH。
   - 全量重建 vs 增量：TimescaleDB continuous aggregate 才是「背景增量更新」的方案（見第 6 點）。
6. **TimescaleDB → 公司更名 Tiger Data，v2.29.x（2026-08-06 檢核 GitHub）**
   - **新最佳實務語法（2.20+）：`CREATE TABLE ... WITH (tsdb.hypertable)`** 直接建 hypertable，
     取代舊的 `create_hypertable()` 呼叫；自動依時間切 chunk + **columnstore 列存（壓縮 90%+、向量化查詢）**。
   - 新映像 `timescale/timescaledb-ha:pg18`（支援 PG18）；本地開發一鍵 `curl -sL https://tsdb.co/start-local | sh`（port 6543，僅限開發）。
   - **continuous aggregates** = 背景增量刷新的「物化視圖」，是監控指標層（metrics）的關鍵元件；
     SkipScan 讓「每感測器最新值」查詢 <100ms（無索引 >5s）。
   - 授權：Apache-2.0 與 Timescale License 雙軌（與 PG 寬鬆授權不同，商用注意）。

## 2. 架構說明（文字圖）

```
外部來源（POS/ROS/API）
   │ ① Airbyte EL（CDC 增量，原封不動）
   ▼
PostgreSQL 數位資料倉庫（18.x）
   ├─ raw      原始 JSONB 整包（GIN 索引）       ← 保留真相、可重處理
   ├─ staging  正規化拆表（FK/CHECK/型別化）     ← 3NF 清洗層
   ├─ marts    星形/寬表 + 樞軸/SLA 視圖          ← 分析層（JOIN 少、預聚合）
   └─ metrics  時間序列（hypertable + columnstore + continuous aggregate）← TimescaleDB
        │
        ├─ Grafana：秒級監控 + 告警（時間序列查詢）
        ├─ Superset：BI 分析/報表（星形表 SQL）
        └─ n8n：告警 Webhook → 自動行動（閉環）
```

**本機對照（8/7 產物）**：`warehouse_schema.sql` 已實作 raw（JSONB+GIN）/ staging（summary/station_util/sla_violation）/ marts（fact + 樞軸視圖）三層；
今日補上第四層 **metrics** 藍圖（TimescaleDB）與 PG18 升級。

## 3. 實用指令

```sql
-- JSONB 查詢（官方 8.14 範例式）
SELECT * FROM raw.ros_sim_report
WHERE payload @> '{"scenario": "lunch_peak"}' ;            -- containment（需 GIN）
SELECT payload -> 'summary' ->> 'total_orders' FROM raw.ros_sim_report;

-- BRIN：時間序列標準配備（metrics 層）
CREATE INDEX idx_metric_time_brin ON metrics.station_util USING BRIN (ts) WITH (pages_per_range = 32, autosummarize = on);
-- 手動補摘要（autosummarize 關閉時）
SELECT brin_summarize_new_values('idx_metric_time_brin');

-- 物化視圖 + CONCURRENTLY 刷新（marts 預聚合）
CREATE MATERIALIZED VIEW marts.daily_sla AS
  SELECT scenario, date_trunc('day', ts) AS day, avg(sla_achievement_rate) AS sla
  FROM staging.sla_violation GROUP BY 1,2;
CREATE UNIQUE INDEX ON marts.daily_sla (scenario, day);     -- CONCURRENTLY 必要條件
REFRESH MATERIALIZED VIEW CONCURRENTLY marts.daily_sla;

-- PG18 新語法（2026-08-08 PGlite PG18.3 實證；注意 NOT NULL NOT VALID 語法**無括號**）
SELECT uuidv7();                                           -- 時間有序 UUID（熱索引）
ALTER TABLE big_table ADD CONSTRAINT c_not_null NOT NULL col NOT VALID;  -- 免全表掃描（無括號！）
ALTER TABLE big_table VALIDATE CONSTRAINT c_not_null;      -- 低鎖驗證（ShareUpdateExclusiveLock）
EXPLAIN ANALYZE SELECT ...;                                -- 18 起預設含 BUFFERS

-- TimescaleDB 2.20+ 新語法（hypertable + 列存，需擴充環境）
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE metrics.station_util (
  ts TIMESTAMPTZ NOT NULL, station_id TEXT NOT NULL, util_pct DOUBLE PRECISION
) WITH (tsdb.hypertable);                                  -- 取代 create_hypertable()
-- continuous aggregate（背景增量，取代手動 REFRESH）
CREATE MATERIALIZED VIEW metrics.hourly_util WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 hour', ts) AS hour, station_id, avg(util_pct) AS util
  FROM metrics.station_util GROUP BY 1, 2;
```

## 4. 與「餐廳營運監控」及「數位資料倉庫」的關聯

- **metrics 層藍圖成形**：ROS 的爐口利用率/單量本就是時間序列 → TimescaleDB hypertable + continuous aggregate
  正是 Grafana 秒級查詢的底層（BRIN on ts + columnstore 壓縮 90%+）。
- **raw JSONB 已驗證**：8/7 PGlite 測試的 `@>` containment 查詢即官方 8.14 機制的實戰應用（GIN 索引）。
- **PG18 升級路徑明確**：compose 映像已升 `postgres:18-alpine`（本棧 SQL 純標準語法，相容）；
  未來分佈式多店 PK 可考慮 uuidv7()；大表加約束用 NOT VALID 兩段式。
- **dbt 銜接**：marts 星形表 + unique 索引 = dbt `materialized='materialized_view'` 的輸出目標，
  REFRESH CONCURRENTLY 對應 dbt on-run-end hook 或排程刷新。

## 4.1 PG18 相容性實證（2026-08-08，PGlite 0.5.4）

- **重大發現：PGlite 0.5.4 的核心就是 PostgreSQL 18.3**（`SELECT version()` 實測），
  代表 8/7 以來的「PGlite 驗證」一直跑在 PG18 上 — 今日升級 compose 映像到 18 的相容性疑慮直接消除。
- 今日新增 `scripts/verify_pg18_warehouse_design.mjs`：7 項概念全部在 PG18.3 核心實證通過：
  uuidv7()（時間有序）、BRIN（pages_per_range=32 + autosummarize=on + brin_summarize_new_values）、
  物化視圖 + UNIQUE + REFRESH CONCURRENTLY、JSONB containment @> + GIN、
  **NOT NULL ... NOT VALID**、virtual generated column（PG18 預設）。
- **誠實更正**：Bytebase 文章寫 `ADD CONSTRAINT ... NOT NULL (column) NOT VALID`（帶括號）是**錯的**；
  實測正確語法為 `ADD CONSTRAINT name NOT NULL column_name NOT VALID`（**無括號**，Neon/depesz 一致）。
- 8/7 的 `warehouse_schema.sql` + `ros_import.py --sql-only`（201 條 SQL）在 PG18.3 完整重跑通過
  （10 報告 × 9 場景、JSONB containment ✅）— postgres:18-alpine 升級安全。

## 5. 明日預告

日數 9 mod 8 = **1 → Grafana 概念**（資料源/面板/模板變數/告警/Explore；對照 8/5 筆記深化，
重點可放 PostgreSQL 資料源的時間序列 macro 與 alerting 條件設計）。

## 6. 來源連結（2026-08-08 檢核）

- PostgreSQL 18 發布新聞（2025-09-25）：https://www.postgresql.org/about/news/postgresql-18-released-3142/
- PostgreSQL 版本支援政策（18.4 現行）：https://www.postgresql.org/support/versioning/
- PostgreSQL 18 Release Notes：https://www.postgresql.org/docs/18/release-18.html
- JSON 型別（8.14，含 jsonb containment/索引建議）：https://www.postgresql.org/docs/18/datatype-json.html
- BRIN 索引（65.5，pages_per_range/autosummarize/運算子類）：https://www.postgresql.org/docs/18/brin.html
- REFRESH MATERIALIZED VIEW（含 CONCURRENTLY 條件）：https://www.postgresql.org/docs/18/sql-refreshmaterializedView.html
- PostgreSQL 18 新特性（Bytebase DBA 視角，2026-04-30 更新）：https://www.bytebase.com/blog/what-is-new-in-postgres-18/
- TimescaleDB（Tiger Data）GitHub README（v2.29.1、tsdb.hypertable、columnstore、pg18 映像）：https://github.com/timescale/timescaledb
- 本機產物：`E:\Hermes Agent\digital-monitoring-platform\scripts\docker-compose.yml`（已升 postgres:18-alpine）
