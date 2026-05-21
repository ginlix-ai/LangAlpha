# Sirius Valuation Skill

> Sirius 七维度（D1~D7）深度基本面分析框架，自包含数据获取 + 估值引擎。

## 目录结构

```
skills/sirius_valuation/
├── skill.json                      # Skill 元数据 + 工作流定义
├── .env.example                    # FMP API Key 配置模板
├── requirements.txt                # Python 依赖（仅 requests）
├── knowledge/                      # 知识文档（Agent 读取）
│   ├── d1_business_model.md       # D1 商业模式与资本特征
│   ├── d2_moat.md                 # D2 竞争优势与护城河
│   ├── d3_environment.md          # D3 外部环境
│   ├── d4_management.md           # D4 管理层与公司治理
│   ├── d5_forward_guidance.md     # D5 MD&A 解读与前瞻
│   ├── d6_comprehensive.md        # D6 综合评估与投资论点
│   ├── d7_qualitative_adjustment.md # D7 定性调整与估值修正
│   ├── system.md                  # 角色设定与写作风格
│   ├── framework_guide.md         # Greenwald 框架与评级标准
│   ├── judgment_examples.md       # 判断锚点与 Logic Chain 示例
│   ├── framework_scope.md         # 框架适用范围与局限性
│   ├── classification_rules.md    # 公司分类规则
│   ├── valuation_methods.md       # 6 种估值方法完整公式
│   ├── valuation_examples.md      # 4 个估值计算案例
│   ├── report_template.md         # 估值报告模板
│   ├── output_schema.md           # D1-D6 结构化参数 Schema
│   └── writing_style.md           # 写作规范
├── scripts/
│   └── fetch_data.py              # 一键数据获取 + 估值引擎（自包含）
├── data/                          # 运行产物（gitignore）
│   └── {symbol}/
│       ├── raw/*.json             # FMP 原始数据
│       ├── financial_context.md   # 格式化财务数据
│       └── engine_result.json     # 估值引擎计算结果
└── examples/                      # 分析示例（可选）
```

## 快速开始

### 1. 配置 FMP API Key

```bash
cd skills/sirius_valuation
cp .env.example .env
# 编辑 .env，填入 FMP_API_KEY
```

或直接设置环境变量：

```bash
export FMP_API_KEY=your_key_here
```

### 2. 获取数据

```bash
# 港股
python scripts/fetch_data.py --symbol 1357.HK --market hk

# 美股
python scripts/fetch_data.py --symbol AAPL --market us

# A股
python scripts/fetch_data.py --symbol 600519.SS --market cn
```

脚本自动完成：
- 并发调用 FMP API（profile + 利润表 + 资产负债表 + 现金流 + Key Metrics + Ratios）
- 格式化为 `financial_context.md`（Markdown 表格，Agent 可直接读取）
- 运行 Python 估值引擎（WACC + 分类 + DCF/DDM/PE Band/PEG/PS + 交叉验证 + 5x5 敏感性矩阵）
- 输出 `engine_result.json`

### 3. Agent 执行分析

```
执行 DAG：

D1 ─┐
D2 ─┤
D3 ─┼─→ D6（综合评估）─→ D7（定性调整）
D4 ─┤
D5 ─┘
```

Agent 按以下步骤工作：

1. **读取 `data/{symbol}/financial_context.md`** — 获取格式化的财务数据
2. **读取 `knowledge/d1_business_model.md`** — 了解 D1 的分析框架和输出格式
3. **执行 D1 分析** — 基于财务数据，按知识指南输出 JSON
4. **D1-D5 并行完成后**，读取 D6 知识指南 + D1-D5 结果 + `engine_result.json`，执行 D6
5. **D6 完成后**，读取 D7 知识指南 + D1-D6 结果 + `engine_result.json`，执行 D7（选择敏感性矩阵坐标，不做算术）

## LangAlpha 集成

### 在 LangAlpha 中使用

1. 将 `skills/sirius_valuation/` 复制到 LangAlpha 的 `skills/` 目录
2. 在 LangAlpha 的 `SKILL_REGISTRY` 中注册（详见 LangAlpha 文档）
3. 在前端输入 `/sirius-valuation` 激活 Skill

### LangAlpha Docker 启动

```bash
cd /path/to/LangAlpha

# 配置 .env（确保 DB_HOST=postgres, REDIS_URL 使用 Docker 服务名）
# 启动全部服务
FRONTEND_PORT=3100 docker compose up -d

# 前端: http://localhost:3100
# 后端: http://localhost:8000
```

> 如果 5173 端口被其他应用占用，用 `FRONTEND_PORT=3100` 指定。

## 维护与更新

### 源 Prompt 变更时同步

源 prompt 位于 `apps/backend/prompts/valuation/`。当源 prompt 更新时：

1. **D1-D7 知识文档**：对比 `apps/backend/prompts/valuation/d{N}_*.md` 与 `knowledge/d{N}_*.md`
   - 注意：knowledge 版本已去掉 `{{`/`}}` 模板标记和 `{financial_context}` 占位符
   - knowledge 版本增加了"所需数据"、"下游消费"等 Skill 特有章节
2. **通用参考文档**：直接复制 `apps/backend/prompts/valuation/references/*.md` → `knowledge/`

### 估值引擎变更时同步

估值引擎代码位于 `apps/backend/service/valuation_engine.py`。当引擎逻辑更新时：

- 同步更新 `scripts/fetch_data.py` 中的 `compute_valuation()` 函数
- 注意：Skill 版本是自包含的精简版，不依赖宿主项目模块

### 检查清单

- [ ] knowledge/d1-d7 与源 prompt 内容对齐
- [ ] knowledge/ 通用参考文档与源 references/ 一致
- [ ] scripts/fetch_data.py 不 import 任何宿主项目模块
- [ ] .env.example 包含所有必需的环境变量
- [ ] skill.json 的 workflow 步骤与 knowledge/ 文件对应
