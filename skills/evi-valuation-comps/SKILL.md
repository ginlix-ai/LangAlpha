---
name: evi-valuation-comps
description: "EVI 可比公司估值：基于市场已对类似公司定价的逻辑，用同行倍数推算目标公司合理价值。包含 peer 选择标准、调整因子回归、足球场图。"
---

# EVI Valuation — Comparable Companies（可比公司）

## 1. 核心思想

> 市场已经给类似公司定价了——我们用它们的倍数来推算目标公司的合理价值

Comps 是**最贴近市场实际定价**的方法。比 DCF 更"市场化"，比 PS 更"全面"（多种倍数交叉验证）。

---

## 2. Peer 选择标准（关键！）

| 维度 | 要求 | 容忍度 |
|---|---|---|
| **行业** | 同细分行业（不能跨行业混比） | 严格 |
| **规模** | 市值在 0.3x ~ 3x 范围 | 严格 |
| **增速** | 收入增速差异 | < 15pp |
| **毛利率** | 差异 | < 10pp |
| **地域** | 同市场优先（港股比港股） | 优先但可放宽 |
| **数量** | 至少 4-5 家，理想 6-10 家 | ≥ 3 家硬下限 |

> ⚠️ 选错 peer 是 Comps 最大的失败原因。宁可选 3 家精准 peer，不选 8 家泛 peer。

---

## 3. 常用倍数

| 倍数 | 适用 | 公式 | 优先级 |
|---|---|---|---|
| **EV/EBITDA** | 通用性最强 | EV / TTM EBITDA | ⭐⭐⭐ |
| **EV/Sales** | 未盈利公司 | EV / TTM Revenue | ⭐⭐ |
| **P/E** | 盈利稳定 | Market Cap / Net Income | ⭐⭐ |
| **P/B** | 金融/重资产 | Market Cap / Book Value | ⭐ |
| **EV/FCF** | 现金流导向 | EV / TTM FCF | ⭐⭐ |

**原则**：根据业务特征选 2-3 种倍数交叉，不要只用一种。

### 3.1 倍数选择决策树

```
是否盈利稳定？
├─ 是 → 用 EV/EBITDA + P/E
└─ 否 →
   ├─ 收入清晰？→ EV/Sales
   ├─ 资产密集？→ P/B
   └─ 现金流为正？→ EV/FCF
```

---

## 4. 调整因子（回归调整）

当目标公司与 peer 存在系统性差异时，需做估值调整：

```
目标合理倍数 = Peer 中位数 × (1 + 增速溢价) × (1 - 风险折价) × (1 + 质量溢价)

增速溢价 = (目标增速 - peer 均值) × 系数 [0.5-1.0]
风险折价 = 流动性折扣 + 集中度折扣 + 治理折扣
质量溢价 = 毛利率优势 + 经常性收入占比 + ROE 优势
```

### 4.1 典型调整范围

| 调整项 | 范围 | 触发条件 |
|---|---|---|
| 增速溢价 | -20% ~ +30% | 增速差 > 5pp |
| 流动性折扣 | -10% ~ 0% | 日均成交 < peer 中位 30% |
| 客户集中度折扣 | -5% ~ -15% | Top 3 客户 > 30% |
| 治理折扣 | -5% ~ -20% | 双重股权 / 关联交易多 |
| 毛利率溢价 | -10% ~ +15% | 毛利差 > 5pp |
| ROE 溢价 | -5% ~ +10% | ROE 差 > 5pp |

---

## 5. 足球场图（Football Field）

最终报告应包含估值范围对比：

```
                        当前价格
                            ↓
方法          Bear ──────────────── Base ──────────────── Bull
DCF           |━━━━━━━━━━━━━━━━━━━━━━━━━━━━|
EV/Sales      |━━━━━━━━━━━━━━━━━━━━━━━━━|
P/E Comps          |━━━━━━━━━━━━━━━━━━━━━━━|
EV/EBITDA          |━━━━━━━━━━━━━━━━━━━━━━━━━|
PEG (P40)               |━━━━━━━━━━━━━━━━━|
DDM         |━━━━━━━━|
Reverse                  ★ (当前价格隐含)
                  ─────────────────────────
最终汇总:           |━━━━━━━━━━━━━━━━━━|  Bear-Bull 区间
```

---

## 6. 计算步骤

```
1. 选 peer set (≥3 家)
2. 拉取每家 peer 的多种倍数
3. 计算每种倍数的 p25 / median / p75
4. 评估目标 vs peer 的 growth/margin/risk profile
5. 计算 pct_adj_pp（必须有理由）
6. 三场景：
   bear = forward_metric_bear × peer_p25 × (1 + adj_bear/100)
   base = forward_metric_base × peer_median × (1 + adj_base/100)
   bull = forward_metric_bull × peer_p75 × (1 + adj_bull/100)
7. 用多种倍数交叉验证一致性
```

---

## 7. 输出格式

### comps_result.json

```jsonc
{
  "method": "Comps",
  "segment_id": "games",
  "values": {"bear": 1850000, "base": 2235000, "bull": 2710000},
  "currency": "RMB million",
  "peer_set": [
    {"name": "网易", "symbol": "NTES", "weight": 1.0, "fact_ref": "fact_..._netease"},
    {"name": "EA", "symbol": "EA", "weight": 1.0, "fact_ref": "fact_..._ea"},
    {"name": "Take-Two", "symbol": "TTWO", "weight": 1.0, "fact_ref": "fact_..._ttwo"}
  ],
  "multiples": [
    {
      "metric": "P/E",
      "stat": "median",
      "value": 18.5,
      "adj_pp": -2.0,
      "adj_reason": "增速差 +3pp 但游戏管线确定性更高",
      "implied_value_base": 2350000,
      "weight_in_final": 0.4
    },
    {
      "metric": "EV/EBITDA",
      "stat": "median",
      "value": 12.0,
      "adj_pp": 0.0,
      "implied_value_base": 2150000,
      "weight_in_final": 0.4
    },
    {
      "metric": "EV/Sales",
      "stat": "median",
      "value": 4.2,
      "adj_pp": -5.0,
      "implied_value_base": 2200000,
      "weight_in_final": 0.2
    }
  ],
  "applicability": "high",      // high / medium / low
  "consistency_cv": 0.05,       // 各倍数估值的变异系数
  "fact_refs": ["fact_..._netease", "..."],
  "confidence": 0.65
}
```

### reports/valuation.md 段落

```markdown
### games — Comps

**Peer 集合**：网易、米哈游（OTC）、动视暴雪、Take-Two、EA

| Peer | P/E | EV/EBITDA | EV/Sales | 增速 | 毛利率 |
|---|---|---|---|---|---|
| 网易 | 17.2x | 11.5x | 4.0x | 14% | 60% |
| 动视暴雪 | 22.5x | 14.8x | 5.2x | 12% | 65% |
| Take-Two | 20.1x | 12.5x | 4.5x | 14% | 55% |
| EA | 18.0x | 11.0x | 3.8x | 13% | 70% |

**倍数中位数**：P/E 18.5x / EV/EBITDA 12.0x / EV/Sales 4.2x

**调整理由**：
- 目标增速 +3pp 高于 peer，但游戏管线（《王者》《和平》）确定性更高 → P/E -2pp
- 毛利率与 peer 一致 → EV/EBITDA 不调整
- 收入经常性偏低（一次性付费比例高）→ EV/Sales -5pp

**三方法交叉**：
- P/E：18.5 × (1-2%) × forward_NI 1300M × 18.13 = 2,350,000M
- EV/EBITDA：12.0 × forward_EBITDA = 2,150,000M
- EV/Sales：4.2 × (1-5%) × forward_Rev = 2,200,000M

**一致性**：CV = 5%（高一致性 ✓）

**汇总（按倍数权重 0.4/0.4/0.2）**：Base = 2,235,000M
```

---

## 8. 操作约束

- ≥ 3 家 peer。少于 → 降级为 cross_check（仅供参考）
- 必须给出 `adj_pp`（不能直接套同行倍数）→ 写明调整理由
- 必须用 ≥ 2 种倍数交叉验证
- CV > 30% 时标 `"applicability":"low"`，建议改用 DCF
- peer 表格必须含增速 + 毛利率（仅倍数没有意义）
- 必须输出足球场图所需数据（每方法的 bear/base/bull）

---

## 9. 脚本计算（必须使用）

⚠️ **严禁手动计算倍数！必须用脚本 `comps_calc.py`。**

原因：手动从不同源拼凑 Market Cap（实时）+ EBITDA/Revenue（历史 FY）会导致**时间口径严重错配**。

### 使用方法

```bash
python3 .agents/skills/evi-valuation-comps/scripts/comps_calc.py \
    --symbol 0981.HK \
    --peers "UMC,GFS,TSM,1347.HK" \
    --segment foundry \
    --data-dir data/{symbol_dir}
```

只需要输入**股票代码**，脚本自动从 FMP 获取：目标公司全部财务数据 + 所有 peer 的多维倍数（TTM 时间对齐）。

### 参数说明

| 参数 | 含义 |
|------|------|
| `--symbol` | 目标公司代码 |
| `--peers` | 逗号分隔的 peer 代码 |
| `--segment` | 分部名称（默认 overall） |
| `--data-dir` | 输出目录（可选） |

### 脚本保证

1. **所有 peer 倍数来自 FMP `keyMetrics-TTM`** — 时间对齐，不手动拼凑
2. **多倍数交叉**：EV/EBITDA + EV/Sales + P/E + P/B 全部自动计算
3. **自动输出三场景** — 用 P25/Median/P75 分位
4. **EV → Equity 转换** — 自动扣除 net debt 得出每股价值
5. **输出路径**：`data/{symbol_dir}/valuation/{segment}/comps_result.json`
