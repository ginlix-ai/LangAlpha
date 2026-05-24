---
name: evi-valuation-ps
description: "EVI PS / EV-Sales：未盈利或高增长业务的关键定价手段。包含 P/S vs EV/Sales 选择、行业基准、资本结构修正。"
---

# EVI Valuation — PS / EV-Sales（市销率）

## 1. 核心思想

> 对于未盈利或利润不稳定的高增长公司，**收入是最可靠的定价锚**

适用场景：
- SaaS / 云计算 / 生物医药 / 高增长平台
- 周期性低谷利润转负
- 一次性减值导致 PE 失真
- 对成熟业务的 DCF 交叉验证

```
P/S = 市值 / 收入
EV/Sales = (市值 + 总债务 - 现金) / 收入
```

---

## 2. P/S vs EV/Sales：必须用 EV/Sales

### 2.1 P/S 的根本缺陷

P/S 存在**分子分母错配**：
- 分子"市值（Market Cap）"仅属于股权持有者
- 分母"收入（Revenue）"在偿还利息前是属于**所有资本提供者**（含债权人）

**后果**：高杠杆公司 P/S 被压低 → 看起来便宜 → 智能体可能做出"该股极便宜"的错误判断（实则只是债务多）。

### 2.2 EV/Sales 的修正

```
EV = Market Cap + Total Debt - Cash & Equivalents
EV/Sales = EV / TTM Revenue
```

**口径一致**：分子是属于全部资本提供者的企业价值，分母是属于全部资本提供者的收入。

→ **本 skill 默认使用 EV/Sales，P/S 仅作参考**。

---

## 3. 行业基准（2026 年）

| 行业类型 | 典型 EV/Sales | 逻辑 |
|---|---|---|
| **高溢价**：生物技术 / REITs / SaaS 软件 | 5-15x | 高毛利（70%+）+ 经常性收入 + 高增长 |
| **科技中位**：消费互联网 / 平台 | 3-8x | 平台效应 + 轻资产 |
| **均衡定价**：硬件科技 / 实体工业服务 | 1-4x | 中等毛利 + 周期性 |
| **传统制造** | 0.5-2x | 低毛利 + 重资产 |
| **深度折价**：分销 / 大宗 / 消费必需 | 0.1-0.5x | 极低毛利 + 高周转 |

### 3.1 行业内细分判断

非周期性科技股的 P/S 经验阈值：
- < 0.75：极具吸引力（"强力买入"）
- 0.75 - 1.5：合理价值
- 1.5 - 3.0：偏贵（需匹配毛利率）
- \> 3.0 + 毛利不匹配：高风险资产

---

## 4. 计算步骤

### 4.1 选 peer 集合（≥ 3 家）

| 维度 | 要求 |
|---|---|
| 行业 | 同细分行业 |
| 增速 | 收入增速差 < 15pp |
| 毛利率 | 差异 < 10pp |
| 规模 | 市值 0.3x ~ 3x |

### 4.2 取倍数

- 中位数（防极端值）
- 25-75% 分位用于 bear/bull

### 4.3 质量调整 (pct_adj_pp)

| 调整项 | 范围 |
|---|---|
| 毛利率高于 peer 均值 | +5% ~ +15% |
| 增速低于 peer | -10% ~ -20% |
| 客户集中度高 | -5% ~ -10% |
| 收入经常性低 | -5% ~ -15% |

### 4.4 三场景

```
bear = forward_rev_bear × peer_p25_multiple
base = forward_rev_base × peer_median × (1 + adj_pp/100)
bull = forward_rev_bull × peer_p75_multiple × (1 + upgrade_pp/100)
```

### 4.5 EV → Equity 转换

```
Equity Value = EV - Net Debt - Minority Interest + Investments at Fair Value
Per Share = Equity Value / Total Shares Outstanding
```

---

## 5. 陷阱与防护

| 陷阱 | 表现 | 防护 |
|---|---|---|
| 收入质量差 | 一次性合同/关联交易 | 检查收入经常性比例（recurring %） |
| 增速脱节 | peer 增速 30%，目标只有 10% | 必须匹配增速区间选 peer |
| 毛利率忽视 | 同 PS 但毛利差 30pp | 加跑 EV/Gross Profit 二次校验 |
| 烧钱公司 | 高收入但巨额亏损 | 检查 cash burn rate，标注风险 |
| 高杠杆 | P/S 看似便宜实则债务多 | 必须用 EV/Sales |

---

## 6. 输出格式

### ps_result.json

```jsonc
{
  "method": "PS",
  "metric_used": "EV/Sales",
  "segment_id": "cloud",
  "values": {"bear": 408000, "base": 571000, "bull": 815000},
  "currency": "RMB million",
  "value_type": "EV",
  "peer_set": [
    {"name": "AWS", "symbol": "AMZN", "ev_sales": 5.5, "as_of": "2026-04-30", "fact_ref": "fact_..._aws"},
    {"name": "Azure", "symbol": "MSFT", "ev_sales": 6.8, "as_of": "2026-04-30", "fact_ref": "fact_..._azure"},
    {"name": "阿里云", "symbol": "BABA", "ev_sales": 3.2, "as_of": "2026-04-30", "fact_ref": "fact_..._aliyun"}
  ],
  "multiple_used": {
    "metric": "EV/Sales",
    "p25": 4.5,
    "median": 5.2,
    "p75": 7.0,
    "pct_adj_pp": -10
  },
  "forward_revenue": {"bear": 95000, "base": 122000, "bull": 145000},
  "quality_adjustments": [
    "毛利率低于 peer 均值 8pp → -10pp",
    "客户集中度（Top 3 占 32%）→ -5pp",
    "增速对标 ✓"
  ],
  "fact_refs": ["fact_..._aws", "..."],
  "confidence": 0.6
}
```

### reports/valuation.md 段落

```markdown
### cloud — EV/Sales

**Peer 集合**（5 家）：AWS、Azure、GCP、阿里云、华为云

| Peer | EV/Sales | 增速 | 毛利率 |
|---|---|---|---|
| AWS | 5.5x | 25% | 35% |
| Azure | 6.8x | 28% | 40% |
| GCP | 4.2x | 30% | 25% |
| 阿里云 | 3.2x | 12% | 28% |
| 华为云 | (未上市) | — | — |

**倍数**：peer 中位数 5.2x EV/Sales

**质量调整**：-10pp（毛利率低 8pp + 客户集中度高）

**计算**：base 122,000M × 5.2 × 0.9 = 571,000M

**三场景 EV**：Bear 408K / Base 571K / Bull 815K

**EV → Equity**：扣净债 +现金 100,000M、少数股东权益 -50,000M → Equity Value 同样三场景。
```

---

## 7. 操作约束

- **默认用 EV/Sales 而非 P/S**（除非业务无债务）
- 至少 3 家 peer，少于 → `{"status":"insufficient_peers"}` 降级为 cross_check
- 倍数必须来自 indexed_facts，**禁止 hardcode**
- 所有 peer fact_id 写入 fact_refs 以便审计
- 报告必须列出 peer 表格（含增速 + 毛利率，不能只有倍数）
- pct_adj_pp 必须有理由（不能直接套同行倍数）
