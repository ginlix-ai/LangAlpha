---
name: evi-valuation-dcf
description: "EVI DCF / SOTP-DCF：完整的折现现金流估值。包含 WACC 推导、FCF 预测、终值计算、三场景 + 敏感性分析、4 项质量检查。结果存 dcf_result.json + reports/valuation.md。"
---

# EVI Valuation — DCF（折现现金流）

## 1. 核心思想

> 一家公司值多少钱 = 它未来产生的所有自由现金流折现到今天的总和

DCF 是**最根本的估值方法**——不依赖市场情绪、不依赖可比公司、只依赖公司自己的赚钱能力。但代价是：对假设极度敏感。

---

## 2. 计算框架

```
企业价值 (EV) = Σ FCF_t / (1+WACC)^t  +  TV / (1+WACC)^n

  FCF_t = EBIT × (1-税率) + 折旧摊销 - 资本支出 - 营运资金变动
  TV (终端价值) = FCF_n × (1+g) / (WACC - g)

每股价值 = (EV + 现金 - 有息负债 - 少数股东权益) / 总股数
```

---

## 3. WACC 推导（Ke 用 CAPM）

```
Ke = Rf + β × ERP + 公司特有风险溢价
Kd = 债务利率 × (1 - 税率)
WACC = Ke × E/(E+D) + Kd × D/(E+D)
```

| 参数 | 取值方法 | 数据来源 |
|---|---|---|
| Rf | 10年期国债收益率 | 公开市场数据 |
| β | 公司 2-5 年回归 beta | FMP / Bloomberg |
| ERP | 市场特定（中国 6%、港股 5.5%、美股 5%） | 损害门 / Damodaran |
| Kd | 公司发债利率 / 隐含借贷成本 | 财报附注 / FMP |
| 税率 | 5 年加权有效税率 | incomeStatement |

**风险溢价加项**（写到 `risk_adjustment.json`）：
- 执行风险（新业务/新市场）：+50~150bps
- 监管风险：+50~100bps
- 客户集中度：+25~75bps
- 治理风险：+25~100bps

---

## 4. FCF 预测（5-10 年）

### 4.1 三场景设计

| 场景 | Revenue Growth | EBIT Margin | WACC | Terminal g |
|---|---|---|---|---|
| Bear | 管理层指引下限 / 行业底部 | 压缩 100-200bps | +100bps | -50bps |
| Base | 管理层中位 + 行业均值 | 当前趋势延续 | 计算值 | 基准 |
| Bull | 管理层上限 / 积极催化剂 | 扩张 100-200bps | -50bps | +50bps |

### 4.2 增长曲线

预测期通常 5-10 年，分两段：
- **高增长期**：3-5 年，按 growth_bridge 中的逐年率
- **过渡期**：2-5 年，从高增长率线性收敛到终端增长率
- **终端**：永续，通常 2-3%（不超过名义 GDP）

### 4.3 必须从 assumption_ledger 读取

- 不允许在脚本中 hardcode 增长率
- growth_bridge.json 必须列出每年的 Revenue（三场景）
- margin_bridge.json 必须列出每年的 EBIT margin + CapEx/Revenue
- risk_adjustment.json 包含 wacc_premium_bps + terminal_growth_pct + execution_risk_factor

---

## 5. 敏感性分析（必须包含）

构建 5×5 矩阵：

```
        Terminal g →
WACC ↓   1.5%   2.0%   2.5%   3.0%   3.5%
8.0%    XXX    XXX    XXX    XXX    XXX
8.5%    XXX    XXX    XXX    XXX    XXX
9.0%    XXX    XXX    XXX    XXX    XXX  ← Base
9.5%    XXX    XXX    XXX    XXX    XXX
10.0%   XXX    XXX    XXX    XXX    XXX
```

另外报告 EBIT Margin ±100bps 对 EV 的影响。

---

## 6. 4 项质量检查（强制）

| 检查 | 阈值 | 含义 | 处理 |
|---|---|---|---|
| 终端价值占比 | TV/EV > 75% | 模型过度依赖永续假设 | 降低 g 或延长预测期 |
| WACC vs 隐含折现率 | 偏离 > 200bps | β 或债务假设可能不准 | 重新审计 |
| 增长率断崖 | 末年率 - g > 200bps | 缺少过渡期衰减 | 加入过渡期 |
| CapEx 标准化 | 偏离历史均值 > 20% | 一次性大额支出失真 | 用 normalized CapEx |

---

## 7. 调用脚本

```bash
python3 .agents/skills/evi-valuation-dcf/scripts/dcf_calc.py \
    --data-dir data/{symbol_dir} \
    --segment cloud \
    --years 10 \
    --terminal-growth 2.5
```

脚本流程：
1. 读 growth_bridge → 推导 10 年 FCF（三场景）
2. 读 group WACC + risk_adjustment → 分部 WACC
3. 计算终端价值 + 折现现金流
4. 三场景独立计算
5. 敏感性矩阵
6. 写 `valuation/{segment_id}/dcf_result.json`

---

## 8. 输出格式

### dcf_result.json

```jsonc
{
  "method": "DCF",
  "segment_id": "cloud",
  "values": { "bear": 1920000, "base": 2232000, "bull": 2663000 },
  "currency": "RMB million",
  "wacc_used": 9.5,
  "terminal_growth_used": 2.5,
  "tv_pct_of_ev": 62.3,            // 质量检查
  "key_assumptions": ["assump_cloud_growth_2026_base", "..."],
  "sensitivity": {
    "wacc_5x5": [...],             // WACC × g 矩阵
    "margin_pm100bps": -7.2        // EBIT margin -100bps 对 EV 影响 %
  },
  "quality_checks": {
    "tv_pct_pass": true,
    "wacc_consistency_pass": true,
    "growth_smoothness_pass": true,
    "capex_normalized": false
  },
  "confidence": 0.7
}
```

### reports/valuation.md 段落

```markdown
### cloud — DCF

**WACC 推导**：β=1.05, Rf=2.7%, ERP=6%, 加 50bps 执行风险溢价 → 9.5% [^1]

**FCF 预测路径**（base 场景）：
| 年份 | Revenue | EBIT Margin | EBIT | FCF |
|---|---|---|---|---|
| 2026E | 122,000 | 15.0% | 18,300 | 12,000 |
| 2027E | ... | ... | ... | ... |

**关键假设**：
- 收入增速：2026 +22%（[4] 国央企续约 + AI 算力需求）逐步收敛至 2030 +12%
- EBIT margin：从 11% (2025) 升至 16% (2030)，驱动：高毛利 AI 收入占比提升 [7]、低毛利项目收缩 [8]

**三场景估值**：Bear 1,920K / Base 2,232K / Bull 2,663K（百万元）

**敏感性**：WACC 每 +100bps → EV -13%；EBIT margin 每 -100bps → EV -7%

**质量检查**：终端价值占 EV 62%（< 75% ✓）、WACC 与隐含折现率一致 ✓
```

---

## 9. 操作约束

- 只用 assumption_ledger 显式声明的变量，**禁止脚本中 hardcode**
- 三场景 (bear/base/bull) 都必须计算
- 缺关键字段 → 写 `{"status":"missing_inputs","missing":[...]}` 并 exit 2
- SOTP-DCF：每个 segment 独立跑，最后由 evi-valuation-orchestrator 汇总
- 必须输出 5×5 敏感性矩阵 + 4 项质量检查结果
