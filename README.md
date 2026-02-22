# RuralRevitalizationBonds

本项目用于抓取并整理上交所、深交所 `2024-2025` 年“乡村振兴”相关债券公开信息，输出：

- 债券名称
- 日期（尽量追溯到发行结果日期）
- 发行金额（按优先级抽取）
- 月度发行金额统计
- 趋势图

---

## 1. 运行方式

```powershell
conda activate rural_bonds
python rural_bonds_pipeline.py --start-date 2024-01-01 --end-date 2025-12-31 --keyword 乡村振兴
```

可选参数：

- `--output-dir` 默认 `output`
- `--cache-dir` 默认 `.cache`
- `--max-workers` 默认 `8`
- `--sse-page-size` 默认 `100`
- `--szse-page-size` 默认 `100`

---

## 2. 输出文件

- `output/bond_detail_raw.csv`：原始候选明细（抓到的公告记录）
- `output/bond_detail_clean.csv`：清洗、去重后明细（用于统计）
- `output/monthly_total.csv`：按月总发行金额（亿元）
- `output/monthly_by_exchange.csv`：按交易所分月发行金额（亿元）
- `output/monthly_total.png`：月度发行金额趋势图（横轴时间，纵轴发行金额）

---

## 3. 为什么这个任务并不“直接抓网页”就能完成

这个任务的实际难点不在“能不能下载页面”，而在“能不能稳定拿到结构化、可统计的数据”：

- 两个交易所页面大多是前端壳页面，真实数据来自接口或二次加载资源。
- 深交所 `bond_news` 频道大量是“上市/转让通知”，不是严格意义上的“发行结果公告”。
- 上交所与深交所公告内容结构差异很大：一个以 PDF 为主，一个以 JSON/HTML 为主。
- 同一只债可能出现多条公告（发行公告、结果公告、更正公告、票面利率公告等），需要去重和优先级策略。
- 同一条文本中的金额表述不统一：`实际发行金额`、`发行规模`、`发行总额` 等需统一口径。

因此项目设计成“多阶段策略 + 可追溯字段”的方式，而不是单一规则。

---

## 4. 数据源与抓取策略（核心）

### 4.1 上交所（SSE）

使用上交所公开接口：

- `https://query.sse.com.cn/commonSoaQuery.do`
- 参数核心：`sqlId=BS_ZQ_GGLL`、`bondType=COMPANY_BOND_BULLETIN`
- 按关键词、日期区间分页抓取公告列表

抓取后策略：

- 先筛 `orgBulletinType=1101`（发行公告类）
- 详情多为 PDF，下载后文本抽取
- 从文本中提取发行结果日期和金额

### 4.2 深交所（SZSE）第一阶段

使用深交所检索接口（频道限定）：

- `https://www.szse.cn/api/search/content`
- 参数核心：`channelCode=bond_news`、`keyword=乡村振兴`

抓到的是“本所公告”频道，优点是稳定、公开；缺点是很多是“上市/转让通知”而非结果公告。

### 4.3 深交所（SZSE）第二阶段：二次追溯（本项目关键增强）

针对第一阶段中 `issue_date_source=szse_listing_start_sentence` 的记录，自动进行“全站反查”：

- 不再限定 `bond_news` 频道
- 组合检索词（证券简称、发行人、证券代码 + `发行结果公告/发行情况公告`）
- 命中候选后评分选最优（标题关键词、代码简称匹配、频道权重、时间邻近度）
- 下载结果公告（PDF/HTML），重新抽取日期与金额并覆盖原值

效果：

- 一部分“上市/转让日期”可替换为更接近真实发行结果日期
- 一部分金额可从 `total/scale` 提升为 `actual`

为保证可追溯，输出中保留了：

- `trace_result_url`
- `trace_result_title`
- `trace_result_publish_date`

---

## 5. 文本解析与字段抽取策略

### 5.1 日期抽取策略

优先级如下：

1. 文本中明确“发行工作已于 XX 年 XX 月 XX 日结束/完成”
2. 深交所通知中的“定于 XX 年 XX 月 XX 日起”（标记为上市/转让起始）
3. 中文落款日期（如“二〇二四年六月五日”）
4. 公告发布日期兜底

并输出来源字段 `issue_date_source`，用于后续核查与筛选。

### 5.2 金额抽取策略

金额统一换算为“亿元”，并按优先级取值：

1. `actual`：实际发行金额/规模
2. `scale`：发行规模
3. `total`：发行总额

来源字段为 `amount_source`。

---

## 6. 去重与统计口径

### 6.1 去重逻辑

优先键：

- `交易所 + 证券代码`

兜底键（缺代码时）：

- `交易所 + 标题规范化 + 日期`

同组冲突时，优先保留：

- 金额来源优先级高的记录
- 日期来源可信度高的记录（结果句 > 其他）

### 6.2 统计口径

- 时间窗口：`--start-date` 到 `--end-date`
- 统计维度：按 `issue_result_date` 映射到 `YYYY-MM`
- 指标：`amount_billion`（亿元）按月求和

---

## 7. 可靠性与工程化处理

为了让脚本可长期复跑，本项目还做了这些工程策略：

- 请求重试：网络/接口失败自动重试
- 缓存机制：PDF落地到 `.cache`，避免重复下载
- 并发解析：线程池并行解析详情，提高速度
- 日志可观测：每阶段打印抓取数量、追溯命中数量、更新数量
- 输出分层：raw/clean/aggregate/chart 分离，便于回溯

---

## 8. 已知局限（必须了解）

- 深交所部分债券无法稳定追溯到结果公告，仍可能保留 `szse_listing_start_sentence` 或 `publish_date_fallback`。
- PDF文本质量受原始文件影响，少数文件可能提取不完整。
- 个别“更正公告/补充公告”与结果公告并存时，虽有评分策略，仍可能需要业务复核。
- 关键词检索口径决定覆盖范围；如果要覆盖更广标签（如乡村振兴+绿色+科创组合），需扩展关键词策略。

---

## 9. 字段说明（clean 明细）

`output/bond_detail_clean.csv` 关键字段：

- `exchange`：交易所（SSE/SZSE）
- `security_code`：证券代码
- `security_abbr`：证券简称
- `bond_name` / `title`：债券名称/公告标题
- `issue_result_date`：用于统计的日期
- `issue_date_source`：日期来源标签
- `amount_billion`：发行金额（亿元）
- `amount_source`：金额来源标签
- `trace_result_url`：深交所二次追溯命中的结果公告链接（如有）
- `issue_month`：统计月份

---

## 10. 推荐使用顺序

1. 先看 `monthly_total.png` 把握趋势。
2. 再看 `monthly_total.csv` / `monthly_by_exchange.csv` 做数值分析。
3. 如需解释某个月波动，回查 `bond_detail_clean.csv` 明细。
4. 如需审计追溯链，重点看 `issue_date_source` 与 `trace_result_*` 字段。
