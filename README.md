
# 🛡️ 安全電子商務客戶 360 平台 (Secure E-commerce Customer 360 Platform)

> **一個展示現代數據堆疊 (MDS) 最佳實踐的端對端專案。**
> 本專案整合了容器化技術、基礎設施即程式碼 (IaC)、資料合約 (Data Contracts) 以及自動化 CI/CD 流水線，旨在建構一個安全、可擴展且高品質的電子商務分析平台。

## 🏗️ 系統架構 (System Architecture)

本專案採用 **ELT (擷取、載入、轉換)** 架構，在確保原始資料完整性的同時，提供資料倉儲內靈活的轉換邏輯。

```mermaid
graph LR
    subgraph Sources [資料來源]
        A["外部 API<br>(匯率資料)"]
        B["靜態檔案<br>(CSV 資料集)"]
    end

    subgraph Orchestration [編排調度 (Airflow & Docker)]
        C[Airflow DAGs]
        D[Worker 節點]
    end

    subgraph Data_Lake [Google Cloud Storage]
        E[原始資料湖]
    end

    subgraph Data_Warehouse [BigQuery]
        F[("Raw 層<br>(原始層)")]
        G[("Staging 層<br>(暫存層)")]
        H[("Marts 層<br>(市集層)")]
        I[("Restricted 層<br>(受限層)")]
    end

    subgraph Transformation [dbt]
        J[資料清洗]
        K[PII 遮罩]
        L[商業邏輯]
        M[資料合約]
    end

    A --> C
    B --> C
    C -->|上傳| E
    E -->|載入| F
    F --> J
    J --> G
    G --> K
    G --> L
    K --> I
    L --> H
    M -.->|驗證| G

```

## 🚀 關鍵工程亮點

### 1. 企業級資料安全與隱私

設計了嚴格的 PII (個人識別資訊) 處理機制，以符合資料隱私標準：

* **雜湊與遮罩 (Hash & Masking)**：在 `Intermediate` (中介) 層對電子郵件/電話號碼實作 SHA256 雜湊，並對真實姓名進行完全遮罩，僅保留分析所需的特徵。
* **架構隔離 (Schema Segregation)**：強制執行嚴格的存取控制。一般分析師僅能存取 `olist_marts`，而 `restricted` (受限) 架構則需要授權人員才能存取。

### 2. 資料品質與合約

實作 **dbt Model Contracts (模型合約)**，在建置階段 (Build time) 強制執行資料規範：

* **Staging 層強制執行**：嚴格的 Schema 檢核，防止上游資料變更導致下游模型損壞。
* **自動化測試**：包含唯一性 (Unique)、非空值 (Not Null) 和參照完整性 (Referential Integrity) 的全面測試套件。

### 3. CI/CD 與自動化 (DevOps)

利用 **GitHub Actions** 進行持續整合與部署：

* **CI (持續整合)**：
* 自動化 Python Linting 以檢查 DAG 語法。
* 在 Pull Requests 上觸發 `dbt build --target ci`，在隔離的 Schema 中驗證 SQL 邏輯。


* **CD (持續部署)**：自動化部署至生產環境的流水線（規劃中）。

### 4. 基礎設施即程式碼 (IaC)

使用 **Terraform** 管理 GCP 資源，確保環境的可重現性：

* 定義 GCS Bucket 生命週期規則以優化成本。
* 使用「最小權限原則 (Principle of Least Privilege)」管理 Service Account 權限。

## 🛠️ 技術堆疊 (Tech Stack)

| 領域 | 技術 | 應用場景 |
| --- | --- | --- |
| **編排調度** | **Apache Airflow 3.1.3** | 使用 TaskGroups 管理複雜依賴，並透過 CeleryExecutor 處理並發任務。 |
| **運算環境** | **Docker & Docker Compose** | 封裝 Airflow 和 dbt 環境，消除「在我的機器上可以跑」的問題。 |
| **資料倉儲** | **Google BigQuery** | 儲存分層資料 (Raw, Staging, Marts) 並利用分區 (Partitioning) 優化查詢。 |
| **資料轉換** | **dbt Core** | 處理資料清洗、建模、測試及文件生成。 |
| **儲存服務** | **Google Cloud Storage (GCS)** | 作為原始 CSV 和 API 歷史紀錄的資料湖 (Data Lake)。 |
| **IaC** | **Terraform** | 自動化配置 GCP 專案資源與 IAM 角色。 |

## 📂 專案結構

```bash
.
├── .github/workflows/        # CI/CD 流水線 (GitHub Actions)
├── dags/                     # Airflow DAGs (Python)
│   └── upload_to_gcs.py      # 主要 ETL 流水線定義
├── dbt_ecommerce/            # 核心 dbt 專案
│   ├── models/
│   │   ├── staging/          # 清洗層 (強制執行合約)
│   │   ├── intermediate/     # 邏輯層 (PII 遮罩)
│   │   └── marts/            # 服務層 (商業價值)
│   └── profiles.yml          # 連線設定 (CI/Dev/Prod)
├── terraform/                # GCP 基礎設施定義
├── scripts/                  # 輔助 Python 腳本 (API 抓取)
├── docker-compose.yaml       # 容器編排設定
└── Dockerfile                # 自定義 Airflow 映像檔 (內含 dbt)

```

## 📊 資料建模架構

本專案遵循嚴謹的 **獎章式架構 (Medallion Architecture)** (銅/銀/金)，透過三個不同的層級將原始資料轉化為可供商業使用的洞察。

### 🥉 Bronze Layer - 銅級 (Staging / 暫存層)

**核心重點：** 清洗、標準化與 Schema 強制執行。
此層級處理原始資料的攝取，不涉及複雜的商業邏輯，專注於確保嚴格的型別安全與資料整潔度。

* **型別轉換與格式化 (Type Casting & Formatting)**：
* 將金額欄位標準化為 `Numeric`，避免浮點數運算誤差。
* 強制將 ID 與 Zip Code 轉為 `String` 型別，防止資料遺失（例如：保留郵遞區號開頭的 0）。


* **深度資料清洗 (Deep Data Cleaning)**：
* **字串正規化**：應用 `normalize(..., NFD)` 與 Regex 去除重音符號並標準化文字（例如：將 "São Paulo" 正規化為 "sao paulo"），確保跨資料集的 Join 可靠性。
* **空值處理**：將字串 `'NaN'` 還原為 SQL `NULL`，並將缺失的產品類別填補為 `'unknown'`。


* **品質防護網 (Quality Assurance)**：
* **代理鍵 (Surrogate Keys)**：使用 `dbt_utils.generate_surrogate_key` 為 `order_items` 和 `payments` 產生唯一識別碼。
* **模型合約 (Model Contracts)**：在關鍵模型（如 `stg_geolocation`）上強制執行 `contract: {enforced: true}`，作為防止上游 Schema 漂移的第一道防線。



### 🥈 Silver Layer - 銀級 (Intermediate / 中介層)

**核心重點：** 邏輯運算、粒度管理與隱私保護。
這是專案的 **邏輯核心 (Logic Core)**，為 Gold 層準備模組化的積木。

* **豐富化 (Enrichment)**：
* **翻譯**：將產品類別與英文翻譯進行關聯 (`int_products_enriched`)。
* **匯率轉換**：透過將訂單與每日匯率關聯，計算美金指標 (`price_usd`) (`int_order_items_enriched`)。


* **粒度管理與去重 (Grain Management & De-duplication)**：
* **防止資料發散 (Fan-out Prevention)**：在 `int_order_payments_pivoted` 中將支付方式在訂單層級進行 Pivot（樞紐分析），防止下游 Join 時造成營收重複計算。
* **地理質心計算**：針對每個 Zip Code 計算平均緯度/經度，解決 GPS 漂移問題，確保地理位置資料的一對一關係 (`int_geolocation_average`)。


* **隱私設計 (Privacy by Design)**：
* **個資分流**：在 `int_customer_security` 中集中處理客戶資料，為電子郵件/電話生成 **SHA256 雜湊**，並對分析用的資料流遮蔽真實姓名。



### 🥇 Gold Layer - 金級 (Marts / 市集層)

**核心重點：** 星狀模型 (Star Schema)、資料消費與存取控制。
資料被組織成事實表 (Fact) 與維度表 (Dimension)，針對 BI 工具 (Tableau/Looker) 進行優化。

#### 1. Core Marts 核心市集 (`olist_marts`)

*供所有分析師進行營運報表分析。*

* **`fct_order_items`**：核心交易事實表，包含營收指標 (USD/BRL) 與訂單狀態。它強制執行 **參照完整性 (Referential Integrity)** 測試，確保所有訂單都能對應到有效的客戶與產品。
* **`dim_products`**：豐富化的產品維度表，包含英文類別名稱與物理屬性。
* **`dim_customers`**：去識別化的客戶維度表，僅包含雜湊 ID (Hash IDs) 與概括性的地理位置資料（城市/州），適合進行一般性分析。

#### 2. Marketing Marts 行銷市集 (`restricted`)

*隔離的 Schema，僅供授權的 CRM/行銷人員使用。*

* **`dim_customers_pii`**：受限的維度表，包含原始 PII（真實姓名、Email、地址）。
* **治理 (Governance)**：透過 dbt schema 設定強制執行，將敏感資料與核心分析倉儲在物理上隔離。

## ⚡ 效能優化 (Performance Optimization)

針對本地 Docker 開發環境進行優化，以處理資源限制：

* **防止 OOM (記憶體溢出)**：在 Python 腳本中實作 **分塊上傳 (Chunked Upload)**（10MB 分塊），防止大檔案傳輸時記憶體溢出。
* **並發控制**：調整 `AIRFLOW__CORE__PARALLELISM` 與 Worker 並發數，防止容器過載。
* **Docker 映像檔優化**：建立自定義 `Dockerfile`，使用 `no-install-recommends` 並清除 apt-cache 以最小化映像檔大小。

## 🏃‍♂️ 快速開始 (Quick Start)

### 前置需求

* Docker 與 Docker Compose
* GCP Service Account Key (JSON 格式)

### 1. 環境設定

```bash
# Clone 專案庫
git clone https://github.com/PoChaoWang/Secure-E-commerce-Customer-360-Platform.git
cd Secure-E-commerce-Customer-360-Platform

# 設定 GCP 憑證
mkdir -p .secrets
cp /path/to/your/key.json .secrets/gcp-key.json

# 設定環境變數
echo "GCP_PROJECT_ID=your-project-id" > .env

```

### 2. 啟動服務

```bash
# 建置並啟動容器 (包含安裝 dbt 依賴)
docker-compose up -d --build

```

### 3. 執行流水線

1. 進入 Airflow UI (http://localhost:8080)。
2. 啟用 `olist_etl_pipeline` DAG。
3. 觸發 DAG (可選參數：`{"full_refresh": true}` 以重建 Schema)。

---

## 📝 授權條款

Apache License 2.0