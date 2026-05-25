---
name: evi-market-sizing
description: "EVI 市场空间推演：用 TAM → SAM → SOM 方法论自下而上推导各分部收入预期。支持 Top-Down / Bottom-Up / Analogy 三种方法交叉验证。主输出 reports/segments/{seg_id}_market_sizing.md + valuation/{seg_id}/market_sizing.json"
---

# EVI Market Sizing — 市场空间推演

## 1. 为什么需要这个 Skill？

传统假设构建（evi-assumption-builder）的增长率来源：
- 管理层指引（最不可靠）
- 历史趋势外推（滞后）
- 分析师一致预期（跟风）

**真正有说服力的增长率**必须建立在**市场空间推演**之上：
> "3 年后收入 = 市场总量 × 渗透率 × 市占率 × ASP"

这让假设有了**物理量约束**——不是拍脑袋，而是"如果每 3 个美国家庭有 1 个割草机器人，那么..."

## 2. 职责定位

```
evi-data-orchestrator（产业调研）
         ↓ 发现"缺少市场空间数据"
evi-market-sizing（本 Skill）
         ↓ 输出 TAM/SAM/SOM 推演
evi-assumption-builder
         ↓ 用推演结果构建增长率三场景
evi-valuation-dcf / ps / ...
```

**触发时机**：
- Phase 1 调研时发现"这个分部没有可靠的增长率依据"
- Phase 2 估值时 assumption-builder 发现 facts 不足以支撑增长率
- 用户/监控要求"推演一下新市场的空间"

---

## 3. 三种推演方法

### 3.1 Top-Down（自上而下）

```
行业总市场（TAM）
    × 可服务比例（SAM/TAM）
    × 可获得比例（SOM/SAM）
    × 该公司份额
    = 收入预测
```

**适用**：行业有权威报告（Gartner / IDC / MarketsAndMarkets / Yole 等）

**示例（速腾聚创 - 机器人 LiDAR）**：
```
全球机器人 LiDAR TAM（2030E）= $8.5B（来源：Yole 2025 报告）
  × 速腾可服务市场比例 = 60%（排除工业级/军用级）= SAM $5.1B
  × 速腾可获得比例 = 40%（排除已被竞品锁定的客户）= SOM $2.0B
  × 速腾当前份额 = 35%（全球 #1 per GGII）
  → 速腾 2030E 机器人 LiDAR 收入 ≈ $700M
```

### 3.2 Bottom-Up（自下而上）— 最有说服力

```
终端用户基数 × 渗透率 × 每终端零部件数量 × ASP × 复购周期
= 年化市场规模
× 公司份额
= 收入
```

**适用**：产品有明确的"量 × 价"关系

**示例（速腾 - 割草机器人 LiDAR）**：

| 变量 | 数据 | 来源 |
|---|---|---|
| 美国独栋住宅（yard > 500sqft） | ~7000 万户 | US Census Bureau |
| 欧洲花园家庭 | ~6000 万户 | Eurostat |
| 割草机器人渗透率（2025） | 5-8% | GMInsights / Husqvarna 年报 |
| 割草机器人渗透率（2030E） | 15-25% | Bottom-up: 类比扫地机器人渗透曲线 |
| 需要 LiDAR 的割草机器人比例 | 30-50% | 高端款需要避障（vs 低端用随机碰撞） |
| 每台 LiDAR 颗数 | 1-2 颗 | 产品规格 |
| LiDAR ASP（机器人级） | $30-60 | 速腾产品定价 |

**推演**：
```
2030E 割草机器人 LiDAR TAM（美国+欧洲）:
  (7000万 + 6000万) × 20%渗透率 × 40%需LiDAR × 1.5颗/台 × $45/颗
  = 1.3亿 × 0.20 × 0.40 × 1.5 × $45
  = $702M

速腾份额（机器人 LiDAR 全球 #1，假设 25-35%）:
  Base: $702M × 30% = $211M
  Bear: $702M × 20% = $140M（渗透率只到 15%）
  Bull: $702M × 40% = $281M（渗透率到 25% + 份额更高）
```

### 3.3 Analogy（类比法）

```
参照品类的渗透曲线 → 映射到当前品类
```

**适用**：新品类缺直接数据时

**示例**：
```
扫地机器人（iRobot）从 2015→2020 渗透率：3% → 12%（美国）
  → 5 年渗透率增长 4 倍

割草机器人当前渗透率：~6%（2025）
  → 按同等曲线：2030E 渗透率 ≈ 18-24%
```

**类比品类池**：
- 扫地机器人（Roomba 渗透曲线）
- 洗碗机（1960-1990 从 5% → 60%）
- 家用摄像头（2015-2022 从 10% → 40%）
- L2+ ADAS（2020-2025 从 5% → 30%）

---

## 4. 输入

```text
data/{symbol_dir}/business_segments.json          ← 有哪些分部
data/{symbol_dir}/information/indexed_facts.json  ← 已有的行业数据
data/{symbol_dir}/reports/segments/{seg_id}.md    ← 分部调研（如有）
data/{symbol_dir}/reports/company_overview.md     ← 公司概况
```

**如果现有数据不足**：
- 用 WebSearch 搜索："[行业名] market size 2025 2030 forecast"
- 用 WebSearch 搜索："[终端品类] penetration rate households [地区]"
- 引用第三方报告（MarketsAndMarkets / Yole / GMInsights / Grand View Research）

---

## 5. 主输出

### 5.1 人类可读报告

`reports/segments/{seg_id}_market_sizing.md`

```markdown
# 市场空间推演 — {segment_name}

## 1. 市场定义与边界

**TAM**：全球 [品类] 市场，包含 [A / B / C] 应用场景
**SAM**：公司可服务市场 = [排除条件]
**SOM**：公司可获得市场 = [竞争格局约束]

## 2. Top-Down 推演

| 数据项 | 2025A | 2027E | 2030E | 来源 |
|---|---|---|---|---|
| 全球 TAM（$M） | 3,270 | 6,500 | 12,790 | MarketsAndMarkets [1] |
| SAM（$M） | 2,000 | 4,200 | 8,500 | 排除军用+工业 |
| SOM（$M） | 800 | 1,800 | 3,500 | 排除已锁定客户 |
| 公司份额 | 25% | 28% | 30% | GGII 2025 报告 [2] |
| 公司收入（$M） | 200 | 504 | 1,050 | 推算 |

## 3. Bottom-Up 推演

### 3.1 终端数量估算

| 应用场景 | 终端基数 | 渗透率 (2025→2030E) | 年销量 | 来源 |
|---|---|---|---|---|
| 乘用车 ADAS | 8000万辆/年 | 8% → 25% | 640万 → 2000万 | IHS / Yole [3] |
| 割草机器人 | 1.3亿户 | 6% → 20% | 780万 → 2600万 | Census + GMInsights [4] |
| 服务机器人 | — | — | 500万 → 2000万 | IFR [5] |

### 3.2 量价推演

| 场景 | 年终端量 | LiDAR搭载率 | 颗数/台 | ASP | 速腾份额 | 收入 |
|---|---|---|---|---|---|---|
| ADAS 2030E base | 2000万 | 100% | 1.2 | $80 | 30% | $576M |
| 割草 2030E base | 2600万 | 40% | 1.5 | $45 | 30% | $211M |
| 服务 2030E base | 2000万 | 60% | 1.0 | $50 | 25% | $150M |
| **合计** | | | | | | **$937M** |

## 4. 类比验证

[扫地机器人渗透曲线图] → 2030E 割草机器人渗透率合理区间 15-25%
[L2+ ADAS 渗透曲线图] → 2030E 车载 LiDAR 渗透率合理区间 20-35%

## 5. 三场景汇总

| 场景 | 2027E 收入 | 2030E 收入 | 隐含 CAGR | 关键假设差异 |
|---|---|---|---|---|
| Bear | $380M | $620M | 30% | 渗透率偏低 + 份额丢失 |
| Base | $500M | $940M | 42% | 中性渗透 + 份额维持 |
| Bull | $650M | $1,300M | 52% | 渗透加速 + 份额提升 |

## 6. 关键不确定性

1. 割草机器人是否需要 LiDAR（vs 视觉方案替代）— 技术路线风险
2. LiDAR ASP 下降速度（如果跌到 $20/颗，市场规模下降但量上升）
3. 竞品（禾赛、Livox）在机器人赛道的市场份额争夺

---

## Facts Index

[1] fact_id=... | source: MarketsAndMarkets LiDAR Market Report 2025
[2] fact_id=... | source: GGII 2025 全球激光雷达市场报告
...
```

### 5.2 结构化产物

`valuation/{seg_id}/market_sizing.json`

```json
{
  "segment_id": "robot_lidar",
  "method": "bottom_up",
  "tam": {"2025A": 3270, "2027E": 6500, "2030E": 12790, "unit": "USD_M", "source": "MarketsAndMarkets"},
  "sam": {"2025A": 2000, "2027E": 4200, "2030E": 8500},
  "som": {"2025A": 800, "2027E": 1800, "2030E": 3500},
  "company_revenue": {
    "2025A": 200,
    "2027E": {"bear": 380, "base": 500, "bull": 650},
    "2030E": {"bear": 620, "base": 940, "bull": 1300}
  },
  "key_drivers": [
    {"name": "penetration_rate", "current": 0.06, "2030E": {"bear": 0.15, "base": 0.20, "bull": 0.25}},
    {"name": "market_share", "current": 0.25, "2030E": {"bear": 0.22, "base": 0.30, "bull": 0.35}},
    {"name": "asp_usd", "current": 50, "2030E": {"bear": 35, "base": 45, "bull": 55}}
  ],
  "analogies_used": ["roomba_penetration_curve", "l2_adas_penetration_curve"],
  "fact_refs": ["fact_mkt_001", "fact_mkt_002", "fact_mkt_003"]
}
```

---

## 6. 与 assumption-builder 的协作

```
evi-market-sizing 输出：
  market_sizing.json 中的 company_revenue 三场景
         ↓
evi-assumption-builder 消费：
  growth_bridge.json 中的 revenue 预测直接用 market_sizing 的数字
  fact_refs 指向 market_sizing 报告中的引用
```

**如果 market_sizing 已经存在**（之前跑过），assumption-builder 直接引用。
**如果不存在**（新分部或首次分析），assumption-builder 应该触发 market-sizing 做推演。

---

## 7. 执行流程

```
Step 1: 确定推演对象
  - 读 business_segments.json → 对每个 segment 检查
  - 如果 segment 已有 market_sizing.json 且更新时间 < 30 天 → 跳过
  - 否则 → 需要推演

Step 2: 收集行业数据（并发子 agent）
  - WebSearch "{行业} market size forecast 2025 2030"
  - WebSearch "{终端品类} penetration rate {region}"
  - WebSearch "{公司} market share {行业} 2025"
  - 查 FMP peers 的收入规模（侧面验证 TAM）
  → 所有数据写入 information/indexed_facts.json（新增 fact）

Step 3: 推演（三种方法至少用两种）
  - Top-Down：行业报告数字 → 份额
  - Bottom-Up：终端量 × 渗透率 × ASP × 份额
  - Analogy：类比品类渗透曲线
  → 交叉验证：三种方法的结果应在 ±30% 范围内

Step 4: 三场景（必须）
  - Bear：渗透率/份额取低端
  - Base：中性假设
  - Bull：渗透率/份额取高端

Step 5: 写入输出
  - 报告：reports/segments/{seg_id}_market_sizing.md
  - 结构化：valuation/{seg_id}/market_sizing.json

Step 6: 更新 changelog
  - 追加到 reports/changelog.md
```

---

## 8. 硬规则

1. **至少用两种方法交叉验证** — 单一方法不接受
2. **每个数字都要有来源** — 第三方报告 / WebSearch / FMP 数据
3. **禁止用 1 个 TAM 数字直接推收入** — 必须有 SAM/SOM 层层收缩
4. **渗透率假设必须有类比依据** — "我觉得 20%"不行，要说"参照扫地机器人从 5% → 12% 的 5 年曲线"
5. **ASP 趋势必须考虑** — LiDAR 这种硬件 ASP 是下降的，不能用当前价直接乘未来量
6. **份额假设不能>50%** — 除非有垄断级证据（如 TSMC 晶圆代工）
7. **三场景收入差距**：Bull / Bear 的比值应在 1.5-3x 之间（太窄说明假设缺乏弹性，太宽说明没约束）

---

## 9. 常用数据源参考

| 类型 | 来源 | 特点 |
|---|---|---|
| 行业报告（付费） | MarketsAndMarkets / Yole / Gartner / IDC | TAM 权威 |
| 行业报告（免费摘要） | Grand View Research / GMInsights / Fortune BI | WebSearch 可获取摘要 |
| 终端数量 | US Census Bureau / Eurostat / 中国统计局 | 基数可靠 |
| 渗透率 | 品牌年报 / 行业协会（IFR / GGII / CPCA） | 当前值 |
| 类比曲线 | Statista / Our World in Data / 公开研报 | 历史渗透轨迹 |
| 竞品份额 | Yole / GGII / 公司公告 | 竞争格局 |
| ASP 趋势 | 公司财报（收入÷出货量）/ 产品定价表 | 价格走势 |

---

## 10. 高级方法论（来自 CFA / McKinsey / Damodaran 框架）

> 以下内容补充了从"市场空间"到"可信估值假设"的逻辑链条。
> 参考：CFA Institute 2026 公司分析、McKinsey Valuation、Damodaran NYU Stern。

### 10.1 收入驱动因子拆解（7 类变量）

在做市场空间推演前，**必须先理解公司的收入公式**：

| 变量类型 | 含义 | 示例 |
|---|---|---|
| **量** | 出货量 / 订单数 / 活跃用户 | LiDAR 年出货 120 万颗 |
| **价** | ASP / ARPU / take rate | LiDAR ASP $45/颗 |
| **渗透率** | 目标客户采用率 / 区域覆盖 | 割草机器人渗透率 6%→20% |
| **留存/复购** | NRR / 复购率 / 流失率 | SaaS NRR 110% |
| **频次** | 交易频次 / 使用频次 | 年均更换 LiDAR 周期 5 年 |
| **供给** | 产能 / 利用率 / 良率 | 工厂年产能 200 万颗 |
| **结构** | 产品线 / 区域 / 客户组合 | 机器人占收入 40%，ADAS 60% |

**不同商业模式的收入公式**：
- 制造业：`出货量 × ASP`（再拆：产能 × 利用率 × 良率 × ASP）
- SaaS：`客户数 × ARPU`（验证：NRR / LTV:CAC / 回收期）
- 平台：`GMV × take rate`（⚠️ GMV ≠ 收入）
- 消费品：`销量 × 单价 × 产品组合`

### 10.2 增长率的三重验证（不是拍脑袋）

增长率预测必须通过三条路径交叉验证：

```
路径 1：历史趋势（剔除一次性因素后的自然增长率）
路径 2：内生增长公式 g = 再投资率 × ROIC（或 g = 留存率 × ROE）
路径 3：市场空间约束（本 skill 的核心输出）
```

**关键判断**：
- 如果路径 3（市场空间）暗示 40% CAGR，但路径 2（内生增长）只支持 20%（因为 ROIC 不够高或再投资不够多）→ 说明增长需要外部融资或并购
- McKinsey：**只有 ROIC > WACC 的增长才创造价值**。收入增长 30% 但每一元新增资本赚不回 WACC → 价值破坏

### 10.3 想象空间 vs 可达路径

> "拿下 1% 的巨大 TAM 就足够大"——这是估值陷阱（1% fallacy）。

正确的思路是从 SOM 出发：

```
SOM（当前可获得）→ 验证执行路径
  ↓ 扩张里程碑达成
SAM（可服务市场）→ 需要什么条件？（新产品/新区域/新渠道）
  ↓ 条件兑现
TAM（理论天花板）→ 只作为"天花板参照"，不进入基准假设
```

**每一层扩张都要回答 6 个约束**：
1. 产品是否满足新客户？
2. 渠道能否触达？
3. 客户迁移成本是否阻碍替代？
4. 竞争者是否会反击？（价格战/渠道封锁）
5. 扩张需要多少资本？ROIC 能否维持？
6. 监管/本地化是否限制？

### 10.4 竞争格局 → 利润率可持续性

市场空间推演必须结合竞争判断：

```
市场大 + 竞争弱 → 高份额 + 高利润率 → 估值倍数高
市场大 + 竞争激烈 → 份额难提 + 利润率受压 → 增长不创造价值
```

**护城河五类来源**（Morningstar）：
1. 转换成本（客户嵌入深 → 流失率低）
2. 网络效应（用户越多价值越大）
3. 无形资产（品牌/专利/数据）
4. 成本优势（规模/技术/位置）
5. 有效规模（市场容不下第二家）

**估值映射**：护城河宽 → 超额收益持续期 20 年+ → DCF 终值增长率可给更高值。

### 10.5 增长阶段匹配模型选择

| 公司阶段 | 增长模型 | 估值方法 |
|---|---|---|
| 早期亏损（速腾 2024 前） | TAM → 渗透率曲线 → 收入拐点 | PS / EV-Sales |
| 高增长转盈利（速腾 2025） | Bottom-Up 量价 + 利润率爬坡 | DCF + PS 交叉 |
| 稳定增长 | 内生增长 g = 再投资率 × ROIC | DCF（主）+ PE |
| 成熟/周期 | 周期归一化 + 份额稳定 | EV/EBITDA + DDM |

### 10.6 Reverse DCF 反推检验

做完市场空间推演后，**必须用 Reverse DCF 反推**：

```
当前股价 → 反推隐含的收入增长率 / 终局利润率 / ROIC
         ↓ 对比
你推演的 base case 增长率
         ↓
如果隐含增长 > 你的 base → 市场比你乐观 → 你可能低估了
如果隐含增长 < 你的 base → 市场比你悲观 → 可能有机会
```

这就把"市场空间推演"直接连到了"投资判断"：**不是算出一个目标价，而是判断当前价格隐含的假设是否合理**。
