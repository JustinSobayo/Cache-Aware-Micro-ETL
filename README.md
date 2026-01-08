# Cache-Aware Micro-ETL Benchmark

A high-performance "Systems-Level" Data Engineering project designed to bridge the gap between low-level hardware constraints (CPU Cache) and high-level Python data pipelines.


---<img width="744" height="425" alt="Screenshot 2026-01-08 013625" src="https://github.com/user-attachments/assets/8437df2c-f737-4082-b961-047811022991" />


## The Goal
The primary objective of this project is to demonstrate and measure how **Memory Hierarchy** (L1/L2/L3 Cache) and **Memory Layout** (Row-based vs. Columnar) impact the performance of data processing pipelines. 

While most data engineers focus on *orchestration* (Airflow, dBT), this project focuses on the **Compute Layer**—understanding the "physics" of data processing to build the fastest, most cost-effective systems possible.

---

## Technical Impact & Problem Solved
Modern CPUs are incredibly fast, but RAM is relatively slow. This creates the **"Memory Wall."**

*   **Variant A (Baseline)**: Demonstrates the "Pointer Chasing" problem where Python objects are scattered in memory, causing the CPU to stall while waiting for data.
*   **Variant C (Columnar)**: Showcases **Vectorization** and **SIMD**, where packed data allows the CPU to process millions of records in a single "gulp."

### Real-World Business Value
| Stakeholder | The Problem | The Micro-ETL Solution |
| :--- | :--- | :--- |
| **Cloud FinOps** | Rising compute costs for ETL. | Reducing CPU time by 10x-100x through cache efficiency. |
| **FinTech/HFT** | Millisecond latencies in market data. | Using contiguous memory layouts to avoid cache misses. |
| **Data Engineers** | Scaling pipelines for "Big Data." | Knowing when to switch from in-memory (Pandas) to streaming (Polars). |

---

##  Project Architecture
The system compares several processing variants to prove performance shifts:
1.  **Row-Based**: Pure Python overhead.
2.  **Batched**: NumPy/Pandas vectorization.
3.  **Columnar**: Modern Arrow-based performance (Polars/DuckDB).
4.  **Semi-Structured**: Why JSON is the biggest performance killer in data.
5.  **Out-of-Core**: Processing data larger than available RAM.

---

## Evidence & Further Reading
This project is built upon decades of research in database internals and high-performance computing.

*   **[S2024 #06 - Vectorized Query Execution (CMU)](https://www.youtube.com/watch?v=yU1S8gwjGEw)**: Why the CPU cache is the most important part of a database.
*   **[Latency Numbers Every Programmer Should Know](https://gist.github.com/jboner/2841832)**: Visualizing the massive speed gap between Cache and RAM.
*   **[What is Apache Arrow?](https://arrow.apache.org/docs/python/index.html)**: The standard for contiguous, columnar memory.
*   **[The "Memory Wall" Problem](https://medium.com/p/8a8b1b8b1b8)**: A deep dive into why CPU speed outpaces memory bandwidth.

---

*Built with love for High-Performance Data Engineering.*
