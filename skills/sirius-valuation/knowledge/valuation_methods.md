# 估值方法公式手册 (Valuation Methods Formulary)

> 每种方法包含：适用场景、数据需求、逐步公式、敏感性分析框架。
> 所有金额单位：百万元（港股百万港元，美股百万美元）。每股金额：元/股（港元/股，美元/股）。
> 百分比保留 2 位小数，金额使用千位逗号分隔符。

---

## 共用模块：WACC 计算

> DCF 和 DDM 均需要折现率。WACC 在 Phase 2 开始时统一计算一次。

### 数据需求
- §14: Rf（无风险利率）
- §11: 10 年周线（用于 Beta 估算）
- §3: 财务费用（利息支出近似）
- §4: 有息负债（短期借款 + 长期借款 + 应付债券）、总市值
- §1: 总市值
- §12: 所得税率（或 §3 所得税/利润总额）

### 计算步骤

**Step W1: 权益成本 Ke (CAPM)**
```
Ke = Rf + Beta × ERP
```
- Rf: §14 提供。若缺失 → A股默认 2.5%, 港股 4.0%, 美股 4.0%
- Beta: 默认值（无市场指数对比数据时）：
  - 大盘蓝筹（市值 > 1000 亿）→ Beta = 0.8
  - 中盘（100-1000 亿）→ Beta = 1.0
  - 小盘（< 100 亿）→ Beta = 1.2
- ERP（股权风险溢价）：A股 = 6.0%, 港股 = 5.5%, 美股 = 5.0%

**Step W2: 债务成本 Kd**
```
Kd_pre_tax = §3 财务费用（最近年） / §4 有息负债均值（最近 2 年）
```
- 若 §3 财务费用为 0 或负值 → Kd_pre_tax = Rf + 1.0%
- 若有息负债 = 0 → 跳过 Kd，WACC = Ke

**Step W3: 资本结构权重**
```
E = §1 总市值（百万元）
D = §4 有息负债最新年（百万元）
E_weight = E / (E + D)
D_weight = D / (E + D)
```

**Step W4: 有效税率**
```
Tax_rate = §3 所得税 / §3 利润总额（5 年平均，剔除亏损年份）
```
- 若无法计算 → A股默认 25%, 港股 16.5%, 美股 21%

**Step W5: WACC**
```
WACC = Ke × E_weight + Kd_pre_tax × (1 - Tax_rate) × D_weight
```
- 合理范围检查：WACC 应在 6%-15% 区间。超出则检查输入参数是否异常。

---

## 方法 1: DCF — 自由现金流折现（稳定版）

### 适用：蓝筹价值型 + 混合型

### 数据需求
- §5: OCF、Capex（5 年）→ FCF = OCF - |Capex|
- §3: 营收、净利润（5 年）
- §4: 现金、有息负债
- §1: 总市值、总股本
- WACC（共用模块）

### 计算步骤

**Step 1: FCF 基线**
```
FCF_base = 近 3 年 FCF 均值（若某年 FCF 为负且偏离均值 > 50%，视为异常年份剔除）
```
- 若 3 年均为负 → 标注"FCF 为负，DCF 可靠性低"，仍继续计算但降低权重

**Step 2: FCF 增长率假设**
```
g_hist = 5 年 FCF 的 CAGR（若 FCF 有负值年份，改用 3 年 CAGR 或营收 CAGR 替代）
g_conservative = g_hist × 0.8（保守折扣）
g_terminal = min(3%, GDP 长期增长率)
```
- A股 GDP 长期增长率假设 = 4.5%
- 港股/美股 = 2.5%

**Step 3: 5 年显式期 FCF 预测**
```
Year 1: FCF_1 = FCF_base × (1 + g_conservative)
Year 2: FCF_2 = FCF_1 × (1 + g_conservative)
Year 3: FCF_3 = FCF_2 × (1 + g_fade)     // g_fade = (g_conservative + g_terminal) / 2
Year 4: FCF_4 = FCF_3 × (1 + g_fade2)    // g_fade2 = (g_fade + g_terminal) / 2
Year 5: FCF_5 = FCF_4 × (1 + g_terminal)
```

**Step 4: 终值 (Terminal Value)**
```
TV = FCF_5 × (1 + g_terminal) / (WACC - g_terminal)
```
- 检查：g_terminal 必须 < WACC，否则模型无解 → 降低 g_terminal 至 WACC - 2%

**Step 5: 折现求和**
```
PV_FCF = Σ(t=1..5) FCF_t / (1 + WACC)^t
PV_TV = TV / (1 + WACC)^5
Enterprise_Value = PV_FCF + PV_TV
```

**Step 6: 每股内在价值**
```
Equity_Value = Enterprise_Value + Cash - Total_Debt
Intrinsic_per_share = Equity_Value / Total_Shares
```
- Cash = §4 货币资金（最新年）
- Total_Debt = §4 有息负债（最新年）

**Step 7: 敏感性分析**
构建 3×3 矩阵：

| | g_terminal - 0.5% | g_terminal | g_terminal + 0.5% |
|---|---|---|---|
| WACC - 1% | | | |
| WACC | | | |
| WACC + 1% | | | |

每格填入对应的 Intrinsic_per_share。

---

## 方法 2: DDM — 股利折现模型

### 适用：蓝筹价值型（高分红稳定公司）

### 数据需求
- §6: DPS 序列（5 年）
- §3: EPS、归母净利润（用于支付率）
- §14: Rf（用于 Ke）
- WACC 中的 Ke

### 计算步骤

**Step 1: 历史股利分析**
```
DPS_series = §6 每年税前每股股息（5 年）
Payout_ratio_series = DPS / EPS（每年）
DPS_CAGR = 5 年 DPS 的 CAGR
Payout_avg = 3 年支付率均值
Payout_std = 3 年支付率标准差
```

**Step 2: 模型选择**
- 若 DPS_CAGR < 5% 且 Payout_std/Payout_avg < 0.20 → **Gordon 单阶段模型**
- 否则 → **两阶段模型**

**Step 3a: Gordon 模型**
```
g = min(DPS_CAGR, ROE_avg × (1 - Payout_avg))
V = DPS_latest × (1 + g) / (Ke - g)
```
- 检查：g 必须 < Ke，否则 → 切换两阶段模型

**Step 3b: 两阶段模型**
```
Phase 1（5 年高速期）:
  g1 = DPS_CAGR
  DPS_t = DPS_latest × (1 + g1)^t,  t = 1..5

Phase 2（永续低速期）:
  g2 = min(3%, GDP 长期增长率)
  DPS_6 = DPS_5 × (1 + g2)

V = Σ(t=1..5) DPS_t / (1+Ke)^t  +  [DPS_6 / (Ke - g2)] / (1+Ke)^5
```

**Step 4: 每股内在价值**
```
Intrinsic_per_share = V
```
（DDM 直接给出每股价值，无需 EV→Equity 转换）

**Step 5: 敏感性分析**
| | g - 0.5% | g | g + 0.5% |
|---|---|---|---|
| Ke - 1% | | | |
| Ke | | | |
| Ke + 1% | | | |

---

## 方法 3: PE Band — 历史市盈率区间

### 适用：蓝筹价值型 + 混合型（盈利稳定公司）

### 数据需求
- §11: 10 年周线价格
- §3: EPS 序列（5-10 年）
- §1: 当前 PE (TTM)

### 计算步骤

**Step 1: 历史 PE 序列**
```
对每个财年：
  Year_end_price = §11 中该年最后一周收盘价
  PE_year = Year_end_price / §3 当年 EPS
筛选：剔除 PE < 0（亏损年份）和 PE > 100（异常年份）
```

**Step 2: PE 统计**
```
PE_min = 历史最低 PE
PE_25 = 25 分位数
PE_median = 中位数
PE_75 = 75 分位数
PE_max = 历史最高 PE
PE_avg = 均值
```

**Step 3: 正常化 EPS**
```
EPS_normalized = 近 3 年 EPS 均值
```
- 若存在 §12 扣非净利润 → 优先使用 扣非净利润 / 总股本

**Step 4: 估值区间**
```
低估价位 = PE_25 × EPS_normalized
合理价位 = PE_median × EPS_normalized
高估价位 = PE_75 × EPS_normalized
```

**Step 5: 当前位置评估**
```
Current_PE = §1 PE(TTM)
Percentile = Current_PE 在历史 PE 序列中的分位数
```

**Step 6: 输出**
- 取合理价位作为 Intrinsic_per_share
- 低估/高估价位作为估值区间的上下界

---

## 方法 4: PEG — 市盈率相对盈利增长

### 适用：成长型 + 混合型

### 数据需求
- §1: PE (TTM), 当前股价
- §3: 归母净利润（5 年）→ 计算增长率
- §3: EPS (TTM)

### 计算步骤

**Step 1: 计算增长率 G**
```
G = 归母净利润 3 年 CAGR（%）
```
- 若 3 年 CAGR 异常（> 80% 或 < 0%）→ 改用 5 年 CAGR
- 若净利润有亏损年份 → PEG 不适用，跳过本方法

**Step 2: 计算 PEG**
```
PE = §1 PE(TTM)
PEG = PE / G
```

**Step 3: PEG 估值判断**
| PEG 值 | 估值判断 |
|--------|---------|
| < 0.5 | 显著低估 |
| 0.5 - 1.0 | 低估 |
| 1.0 - 1.5 | 合理 |
| 1.5 - 2.0 | 偏高 |
| > 2.0 | 高估 |

**Step 4: 反解合理股价**
```
Fair_PE = G × 1.0（PEG = 1 的均衡点）
Fair_Price = Fair_PE × EPS_TTM
```

**Step 5: 敏感性（增长率变化影响）**
| G 假设 | PEG | Fair_PE | Fair_Price |
|--------|-----|---------|-----------|
| G - 5% | | | |
| G | | | |
| G + 5% | | | |

**Step 6: 输出**
- Intrinsic_per_share = Fair_Price
- 估值区间 = [G-5% Fair Price, G+5% Fair Price]

---

## 方法 5: DCF（情景分析版）— 成长型

### 适用：成长型（高不确定性公司）

### 数据需求
同方法 1（DCF 稳定版）

### 与稳定版的区别
- 不用单一增长率，而是设 3 个情景
- 最终结果为概率加权

### 计算步骤

**Step 1-2: WACC**
同方法 1

**Step 3: 三情景 FCF 预测**

**乐观情景 (25% 概率)**：
```
Revenue growth = 历史营收 CAGR（维持高增速）
Net margin = 逐步从当前向行业成熟公司水平收敛（每年 +0.5%）
Capex/Revenue = 历史比率
FCF_t = Revenue_t × Net_margin_t + D&A_t - Capex_t
```

**基准情景 (50% 概率)**：
```
Revenue growth = 历史营收 CAGR × 0.7（减速）
Net margin = 维持当前水平
Capex/Revenue = 历史比率
```

**悲观情景 (25% 概率)**：
```
Revenue growth = Year 1-2: 历史 CAGR × 0.4; Year 3-5: 0%
Net margin = 当前水平 × 0.8（压缩）
Capex/Revenue = 历史比率 × 1.2（效率下降）
```

**Step 4: 每个情景执行 DCF 方法 1 的 Step 4-6**

**Step 5: 概率加权**
```
Weighted_intrinsic = 0.25 × V_optimistic + 0.50 × V_base + 0.25 × V_pessimistic
```

**Step 6: 输出**
- Intrinsic_per_share = Weighted_intrinsic
- 估值区间 = [V_pessimistic, V_optimistic]

---

## 方法 6: PS — 市销率

### 适用：成长型（尤其亏损或微利公司）

### 数据需求
- §3: 营收序列（5 年）
- §1: 总市值、总股本
- §11: 10 年周线价格（用于历史 PS）

### 计算步骤

**Step 1: 当前 PS**
```
PS_current = §1 总市值 / §3 最新年营收
```

**Step 2: 历史 PS 序列**
```
对每个财年：
  Market_cap_year_end = §11 年末收盘价 × §1 总股本
  PS_year = Market_cap_year_end / §3 当年营收
```

**Step 3: PS 统计**
```
PS_min, PS_25, PS_median, PS_75, PS_max
```

**Step 4: 估值**
```
Revenue_TTM = §3 最新年营收
Fair_Value = PS_median × Revenue_TTM
Fair_Price = Fair_Value / Total_Shares
```

**Step 5: 估值区间**
```
低估价位 = PS_25 × Revenue_TTM / Total_Shares
合理价位 = PS_median × Revenue_TTM / Total_Shares
高估价位 = PS_75 × Revenue_TTM / Total_Shares
```

**Step 6: 输出**
- Intrinsic_per_share = Fair_Price
- 估值区间 = [低估价位, 高估价位]

---

## 交叉验证规则

### 一致性评估

```
CV = 各方法 Intrinsic_per_share 的标准差 / 均值
```

| CV 值 | 一致性 | 处理 |
|-------|--------|------|
| < 15% | 高 | 直接取加权平均 |
| 15%-30% | 中 | 标注偏离最大的方法及原因 |
| > 30% | 低 | 逐一分析偏离原因，考虑剔除异常方法后重新加权 |

### 偏离原因分析指引

| 偏离方法 | 常见原因 |
|---------|---------|
| DCF 偏高 | 终值占比过大（>80%），增长率假设过于乐观 |
| DCF 偏低 | FCF 基期包含异常低年份 |
| DDM 偏低 | 支付率偏低，分红不反映真实盈利能力 |
| PE Band 偏高 | 历史牛市 PE 推高中位数 |
| PEG 偏低 | 短期高增速不可持续 |
| PS 偏高 | 低利润率公司收入倍数虚高 |

### 最终估值区间

```
Conservative = min(各方法中性估值)
Central = 加权平均
Optimistic = max(各方法中性估值)
```

---

## 反向估值（Reverse Valuation）

> 常驻分析模块。用当前市场价格反推市场隐含的增长假设，揭示"价格里包含了多少增长预期"。
> 由 Python 引擎自动计算，LLM 负责结合定性报告解读含义。

### 三种反解方法

**方法 R1: E/P 反解盈利增长（基于 Gordon 模型变形）**
```
E/P = Ke - g_implied
g_implied_earnings = Ke - (1/PE)
```
- 含义：如果把公司视为永续增长体，盈利收益率 = 要求回报率 - 增长率
- 适用：所有盈利为正的公司

**方法 R2: 反向永续 DCF（FCF 收益率法）**
```
Market_Cap = FCF / (WACC - g)
g_implied_fcf = WACC - FCF_yield
FCF_yield = FCF_base / Market_Cap
```
- 含义：市场定价隐含的 FCF 永续增长率
- 若 g_implied < 0：市场定价隐含 FCF 萎缩

**方法 R3: 反向 DDM（股利增长反解）**
```
P = DPS × (1+g) / (Ke - g)
g_implied_div = (P × Ke - DPS) / (P + DPS)
```
- 含义：当前股价隐含的股利永续增长率

### 增长折价分析

```
增长折价 = 实际增长率 - 市场隐含增长率
```

| 折价幅度 | 含义 |
|---------|------|
| > 8 pct | 市场几乎未为增长付费（典型蓝筹困境） |
| 3-8 pct | 市场部分认可增长，但有显著折价 |
| 0-3 pct | 市场基本定价了增长 |
| < 0 pct | 市场给予增长溢价（成长股特征） |

### 隐含要求回报率

```
r_implied = FCF_yield + g_actual
```
- 若 r_implied >> WACC：市场对该公司的风险定价远高于模型假设
- 差额反映市场未被模型捕获的风险溢价（行业天花板、地缘风险、风格偏好等）
