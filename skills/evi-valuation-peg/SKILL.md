---
name: evi-valuation-peg
description: "EVI PEG：PE 与增长率比值估值。包含传统阈值法 + 行业百分位动态对齐法（解决信号稀缺问题）。"
---

# EVI Valuation — PEG（市盈率增长比率）

## 1. 核心思想

> PE 高不一定贵——如果增长够快

PE = 30x 看似昂贵，但若 EPS 以 30% 速度增长，PEG = 1.0，估值与增长完美匹配。
PEG 的核心：**衡量"每单位增长付出的溢价"**。

```
PEG = PE / (EPS 增长率 × 100)
```

---

## 2. 判断框架

### 2.1 传统绝对阈值

| PEG | 解读 |
|---|---|
| < 0.5 | 显著低估（需确认增长可持续） |
| 0.5 - 1.0 | 低估 / 合理（Peter Lynch 买入区间） |
| = 1.0 | 完美定价（估值 = 增长） |
| 1.0 - 2.0 | 偏贵（需要更高确定性） |
| > 2.0 | 高估（除非有极强护城河） |

### 2.2 ⚠️ 绝对阈值的问题

历史数据显示绝对 PEG < 1.0 在实际市场中**极度稀缺**：
- 标普 500 自 1985 年至 2020 年，整体 PEG 跌破 1.0 的月份极少（2000s 仅 3 次，2010s 仅 5 次）
- 机械应用绝对阈值 → 智能体会错失大量行业投资机会

### 2.3 ✅ 行业百分位动态对齐法（推荐）

**不用绝对阈值，改用同行业 PEG 百分位排名**：

1. 构建 peer group（10-15 家同行业公司）
2. 拉取每家的 forward PE / consensus growth → 计算 PEG
3. 看目标公司在 peer 中的百分位

| 百分位 | 解读 |
|---|---|
| < 25% | 行业内低估（强买入候选） |
| 25-50% | 估值有吸引力 |
| 50-75% | 估值合理 |
| > 75% | 行业内高估 |

**优势**：
- 信号呈连续分布，不会出现"全行业都不低估"的死局
- 自动消除板块间风险溢价差异
- 不会被周期性收益暴增误导

---

## 3. EPS 增长率的选择

| 来源 | 优先级 | 注意事项 |
|---|---|---|
| 本模型自建预测 | 最高 | 来自 assumption_ledger 的 EPS 增速 |
| 卖方一致预期 (consensus) | 次之 | 3-5 年 CAGR |
| 历史 EPS CAGR | 最低 | 仅用于交叉验证 |

---

## 4. 使用限制

| 场景 | 处理 |
|---|---|
| EPS 为负 | PEG 无意义 → 跳过，改用 PS |
| 一次性收益主导 | 必须用 normalized EPS（剔除非经常性损益） |
| 周期股顶部 | 利润膨胀 → PEG 看似便宜但实则见顶 |
| 极高增速（>40%） | PEG 失真 → 改用 DCF |

---

## 5. 计算流程

```
1. 取 normalized_earnings（剔除一次性后的 EPS）
2. 取 sustainable_growth_rate（3-5 年可持续增速）
3. 取 peg_range（行业百分位 25/50/75 或行业合理区间）

forward_pe_target = sustainable_g × peg_range

implied_value = normalized_earnings × forward_pe_target

三场景：
  bear  = normalized_eps_bear × g_bear × peg_25%
  base  = normalized_eps_base × g_base × peg_50%
  bull  = normalized_eps_bull × g_bull × peg_75%
```

---

## 6. 输出格式

### peg_result.json

```jsonc
{
  "method": "PEG",
  "segment_id": "games",
  "values": {"bear": 12500, "base": 18300, "bull": 26800},
  "currency": "RMB million",
  "method_variant": "industry_percentile",
  "normalized_earnings": {"bear": 800, "base": 1100, "bull": 1450},
  "earnings_adjustments": [
    "剔除 2024 投资收益 -150",
    "汇兑损益标准化 +30",
    "税率回归 25% +20"
  ],
  "sustainable_growth_pct": 12.0,
  "growth_source": "consensus + own model average",
  "peer_group": [
    {"name": "网易", "peg": 0.95},
    {"name": "EA", "peg": 1.35},
    {"name": "Take-Two", "peg": 1.65}
  ],
  "peg_range": {"p25": 0.95, "p50": 1.35, "p75": 1.85},
  "target_percentile": "P40",
  "fact_refs": ["fact_..._consensus_growth", "fact_..._industry_peg"],
  "confidence": 0.55
}
```

### reports/valuation.md 段落

```markdown
### games — PEG

**正常化 EPS**：剔除一次性投资收益、汇兑损益、税率异常 → base 1,100M

**可持续增长率**：12%（自建预测 11% + consensus 13% 的均值）[^22][^23]

**行业百分位对齐**：
| Peer | Forward PE | Consensus Growth | PEG |
|---|---|---|---|
| 网易 | 13x | 14% | 0.93 |
| 米哈游 (隐含) | 18x | 25% | 0.72 |
| EA | 17x | 13% | 1.31 |
| Take-Two | 22x | 14% | 1.57 |
| 动视暴雪 | 25x | 12% | 2.08 |

行业 PEG 分布：P25=0.95 / P50=1.35 / P75=1.85

**目标公司位于 P40**（低于中位数，估值具吸引力）

**三场景隐含 forward PE**：12x / 16x / 22x → 估值区间 12,500 / 18,300 / 26,800 (M)
```

---

## 7. 操作约束

- normalized_earnings 必须**显式列出剔除项**
- 增长率 > 25% 时谨慎：高速增长期 PEG 失真，建议改用 DCF
- 至少 1 条 medium+ 可靠性事实支撑增长率
- 必须用行业百分位对齐法（除非 peer 不足 5 家）
- peer < 5 家 → 降级为 cross_check
