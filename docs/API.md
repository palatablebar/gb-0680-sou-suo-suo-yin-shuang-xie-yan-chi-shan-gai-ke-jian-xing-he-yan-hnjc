# 接口

模块GET使用仓内样例，模块POST可传csv或rows与context，直接计算当前规则。/api/runs使用完整流水线并原子保存。每个阶段返回rows、quarantine、metrics；后续阶段还返回累计隔离。/api/upload接收multipart文件file。/api/runs/<id>/archive只导出一份带清单与证据的CSV。

## 核验水位（小时增量扫描）

水位把"到达时刻"与"事件时间"分离，采用双水位裁决，避免倒序到达下把迟到事件永久跳过：

- `GET /api/watermark`：当前水位快照（双水位、事件台账、空档/未覆盖、隔离、扫描批次）。
- `POST /api/watermark/scan`：body `{csv|sample, parameters}`。参数 `scan_time`（扫描时刻，缺省取当前UTC）、`observed_at`（输入无 observed_at 列时的默认到达时刻）、`bucket_seconds`（桶宽，默认3600）、`allowed_lateness_seconds`（允许迟到，默认3600）。
- `POST /api/watermark/upload`：multipart 文件，参数走 query string。
- `POST /api/watermark/reset`：清空水位台账（测试/重建用）。
- `GET /api/watermark/archive`：与页面同源的证据CSV，含水位元数据、事件、空档、隔离与计数摘要。
- 页面入口 `/watermark`。

裁决语义：`observed_frontier`=最大到达时刻（只进不退）；`coverage_frontier`=观测前沿回退 `max(桶宽,允许迟到)` 后对齐桶边界（只进不退、不越观测前沿），区间采用 `[start,end)`。事件所在桶已关闭且事件在关闭时点之后到达记 `LATE_BACKFILL`（补见不跳过）；已覆盖空桶记 `GAP_EMPTY`；观测窗口内未成熟空桶记 `UNCOVERED_PENDING`；无有效到达记 `no_arrival` 且水位不前移。扫描以 `(输入摘要,scan_time,参数)` 为幂等键，重放同一批只回显既有结论。
