---
name: evi-reverse-valuation
description: "EVI 反向估值：从当前股价反推市场对增长率/利润率/WACC 的隐含预期。回答'市场已 price-in 多少？'"
---

# EVI Reverse Valuation（反向估值）

## 1. 核心思想

> 不是"公司值多少"，而是"**市场认为公司值多少**"

正向 DCF：假设 → 估值 → 与股价比较
**反向 DCF**：股价 → 反推假设 → 与基本面比较

回答的问题：
- 市场是不是把"AI 收入"已经 price-in？
- 维持当前价，市场需要相信营收 CAGR 多少？利润率多少？
- 关键参数处于历史 / 行业 / 同业的哪个分位？

---

## 2. 三种反推维度

| 反推 | 锁定 | 求解 |
|---|---|---|
| **隐含增速** | WACC + 终端 g + 利润率 | Revenue CAGR |
| **隐含利润率** | WACC + 增速 + 终端 g | EBIT Margin |
| **隐含折现率** | 增速 + 终端 g + 利润率 | WACC |

通常**先反推增速**（市场最关心增长），其它两个作为补充。

---

## 3. 计算流程

### Step 1：建立 SOTP 估值函数

基于 group + 各 segment 的现有假设，构建：

```
F(g_revenue, m_ebit, w_wacc) = SOTP_EV(各分部用 g/m/w 重新计算 DCF 后求和)
```

### Step 2：求解 F = current_EV

```
current_EV = current_market_cap + total_debt - cash
```

在 (g, m, w) 三维空间做网格搜索，找到使 F = current_EV 的曲面。

### Step 3：选 1 个代表点

通常**锁定 wacc**（用 group ledger 的值，最稳定），仅在 g、m 上搜索。

或：**锁定 m**（用历史均值），求解 g。

---

## 4. 与基准对比表（核心输出）

| 参数 | 市场隐含 | 历史均值 | 同业中位 | 管理层指引 | 判定 |
|---|---|---|---|---|---|
| Revenue CAGR (5Y) | 11.5% | 9.0% | 10.0% | 10-12% | slightly aggressive |
| Long-term EBIT Margin | 33.0% | 30.5% | 31.5% | 33-35% | near peak |
| WACC | 9.4% | — | — | — | aligned |

### 4.1 判定标准

| 判定 | 含义 |
|---|---|
| **deeply pessimistic** | 隐含值 < 历史最差 5% 分位 |
| **pessimistic** | 隐含值 < 历史 25% 分位 |
| **aligned** | 隐含值在历史 25-75% 分位 |
| **slightly aggressive** | 隐含值在历史 75-95% 分位 |
| **deeply aggressive** | 隐含值 > 历史 95% 分位 / 接近峰值 |

---

## 5. 投资解读（必须有）

报告必须有 1-3 句**结论性**解读，写给投资人看：

> 例：市场已隐含云 + AI 利润率维持在历史峰值附近且营收 CAGR 略高于行业。下行风险偏向利润率回归——若 AI 商业化不及预期，市场需重新定价。

---

## 6. 触发重估的指标（核心价值）

反向估值的最大价值是**告诉你哪些数据需要持续盯**：

```markdown
## 触发市场重估的关键指标

| 指标 | 当前隐含 | 重估阈值（向下） | 重估阈值（向上） | 数据来源 |
|---|---|---|---|---|
| 云收入增速 | 22% | < 18% (低于隐含) | > 26% | 季报披露 |
| 长期 EBIT Margin | 33% | < 30% | > 35% | 季报 / 业绩说明会 |
| 国央企续约率 | 95%（隐含） | < 90% | > 98% | 公司官方公告 |
| AI 商业化收入 | 占比 8%（隐含） | < 5% | > 12% | 季报分部数据 |

如任何一项偏离重估阈值 → 触发 evi-revaluation-updater
```

---

## 7. 输出格式

### reverse_valuation.json

```jsonc
{
  "schema_version": 1,
  "valuation_date": "2026-05-22",
  "current_price": 380.0,
  "current_market_cap": 3489000,
  "current_ev": 3520000,
  "implied": {
    "revenue_cagr_5y_pct": 11.5,
    "long_term_ebit_margin_pct": 33.0,
    "wacc_pct": 9.4
  },
  "vs_benchmarks": [
    {
      "variable": "revenue_cagr_5y_pct",
      "implied": 11.5,
      "history_avg": 9.0,
      "history_p75": 11.0,
      "peer_median": 10.0,
      "guidance_range": [10.0, 12.0],
      "verdict": "slightly aggressive",
      "interpretation": "略高于历史均值，与管理层指引上限吻合"
    },
    {
      "variable": "long_term_ebit_margin_pct",
      "implied": 33.0,
      "history_high": 34.0,
      "history_avg": 30.5,
      "verdict": "near peak"
    }
  ],
  "rerate_triggers": [
    {
      "metric": "cloud_revenue_growth_yoy",
      "current_implied": 22.0,
      "threshold_down": 18.0,
      "threshold_up": 26.0,
      "data_source": "季报"
    }
  ],
  "interpretation": "市场已隐含云+AI 利润率维持峰值附近且营收 CAGR 略高于行业。下行风险偏向利润率回归。",
  "fact_refs": ["fact_..._historical_margin", "fact_..._peer_growth"],
  "confidence": 0.55
}
```

### reports/reverse_valuation.md

```markdown
# 反向估值 — {display_name}

## 1. 当前市场假设

当前价 380 HKD，对应 EV 3.52T RMB。要让 SOTP 估值等于当前 EV，市场需相信：

| 参数 | 隐含值 | 我们 Base | 历史均值 | 同业中位 | 判定 |
|---|---|---|---|---|---|
| Revenue CAGR (5Y) | **11.5%** | 12.5% | 9.0% | 10.0% | slightly aggressive |
| Long-term EBIT Margin | **33.0%** | 32.0% | 30.5% | 31.5% | near peak |
| WACC | **9.4%** | 9.0% | — | — | aligned |

## 2. 投研解读

市场已隐含云 + AI 利润率维持在历史峰值附近且营收 CAGR 略高于行业。
**下行风险偏向利润率回归**——若 AI 商业化不及预期或竞争加剧，市场需重新定价。

但若 AI 商业化确认 + 云利润率突破历史峰值 → 仍有 +20% 上行空间（对应我们的 Bull 场景）。

## 3. 与我们 Base Case 的差异

我们的 Base 比市场更乐观：CAGR 12.5% vs 11.5%（+1pp）
→ Base 上行 56% 中，约 10pp 来自更乐观的增速假设。

## 4. 触发市场重估的关键指标

| 指标 | 当前隐含 | 向下阈值 | 向上阈值 | 数据频次 |
|---|---|---|---|---|
| 云收入增速 (YoY) | 22% | < 18% | > 26% | 季报 |
| EBIT Margin | 33% | < 30% | > 35% | 季报 |
| 国央企续约率 | 95% | < 90% | > 98% | 半年报 |
| AI 商业化占比 | 8% | < 5% | > 12% | 季报 |

任一偏离 → evi-monitor 标 high severity → evi-revaluation-updater 触发重估

---

## Facts Index

[1] fact_id=fact_reverse_001 | segment=group | reliability=high
    text: 公司过去 5 年营收 CAGR 9.0%。
    source: 自计算（base/fmp/incomeStatement.json）

[2] fact_id=fact_reverse_002 | segment=group | reliability=high
    text: 历史 EBIT margin 峰值 34%（2022），均值 30.5%。
    source: base/fmp/incomeStatement.json
```

---

## 8. 操作约束

- 至少基于 1 个 segment 的 DCF 已完成（否则没法构 F 函数）
- 反推时建议**锁定 wacc**（最稳定），仅在 g、m 上搜索
- 必须有 `interpretation` 段：1-3 句话，结论性，写给投资人看
- 必须列出 `rerate_triggers`（这是 monitor / revaluation-updater 的输入）
- 与基准对比表必须包含历史 + 同业 + 管理层指引三个维度
