# BitStream: 實時比特幣交易流式分析平台

這是一個基於 **Lambda Architecture** 簡化設計的實時大數據處理平台。系統能夠實時採集幣安 (Binance) 成交數據，透過分散式運算框架進行清洗與 5 秒窗口聚合分析，最終提供即時更新的 K 線儀表板。

## 🏗 系統架構



1.  **數據源 (Data Source)**: 透過 WebSocket 接入 Binance API 實時交易流。
2.  **訊息隊列 (Message Queue)**: 使用 **Apache Kafka** 作為緩衝層，落實數據解耦並確保系統具備削峰填谷的能力。
3.  **流式計算 (Stream Processing)**:
    * **ETL Job**: 負責數據清洗、格式轉換與原始成交數據持久化。
    * **Analytics Job**: 實作 **Sliding Window Aggregation**，計算 5 秒窗口的 OHLC K 線指標。
4.  **存儲層 (Serving Layer)**: 使用 **PostgreSQL** 存儲結構化指標。
5.  **可視化 (Visualization)**: **Streamlit** 即時呈現交易趨勢圖與統計報表。

## 🛠 技術棧

- **開發語言**: Python 3.11
- **流處理框架**: Spark Structured Streaming 3.5.1
- **分散式通訊**: Apache Kafka & Zookeeper
- **資料庫**: PostgreSQL 16
- **基礎設施**: Docker & Docker Compose
- **數據可視化**: Streamlit, Plotly

## 🚀 核心亮點與實務問題解決 (Interview Highlights)

### 1. 分散式資源調度與隔離 (Resource Isolation)
在 Docker 環境下啟動多個 Spark 任務時，遇到了 **Resource Starvation (資源飢餓)** 問題。
* **挑戰**: 其中一個 Spark Job 預設會佔用 Worker 的所有核心 (Cores)，導致另一個 Job 持續處於 `WAITING` 狀態。
* **解決方案**: 透過配置 `--total-executor-cores 2` 實作**靜態資源隔離**，手動劃分運算資源，確保「數據清洗」與「聚合分析」兩個串流任務能同時穩定運行。

### 2. 流式聚合與延遲處理 (Watermarking)
* **Watermarking**: 設定 10 秒的水位線機制，有效處理分散式環境中可能發生的**數據延遲 (Late Data)** 問題，保證 K 線聚合結果的準確性。
* **高效寫入**: 透過 `foreachBatch` 結合 **JDBC Batching** 技術，顯著降低高頻數據入庫時的 I/O 壓力。

### 3. 動態環境依賴注入
* 針對 Spark 官方鏡像缺乏 `kafka-python` 等 Python 依賴的問題，設計了啟動 Entrypoint 腳本在容器運行時實施 **Runtime pip install**，並透過 `--packages` 自動處理 Java Connector 依賴。

## 📦 快速啟動

1. **環境需求**: 確保已安裝 Docker 與 Docker Compose。
2. **啟動系統**:
   ```bash
   docker-compose up -d