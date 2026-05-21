# 知识库数据目录 Schema

## catalog.json 结构

```json
{
  "version": "1.0.0",
  "last_updated": "2024-12-15T10:30:00",
  "symbol": "0700.HK",
  "market": "hk",
  "categories": {
    "financials": {
      "description": "财报 PDF",
      "items": [
        {
          "filename": "2024-annual-report.pdf",
          "path": "knowledge/financials/2024-annual-report.pdf",
          "type": "annual-report | interim-report | quarterly-report | 10-K | 20-F",
          "title": "腾讯控股有限公司 2024年年度报告",
          "year": "2024",
          "date": "2024-03-20",
          "market": "hk",
          "source": "HKEx | 巨潮资讯 | FMP/SEC",
          "url": "https://...",
          "size_bytes": 5242880,
          "downloaded_at": "2024-12-15T10:30:00"
        }
      ]
    },
    "announcements": {
      "description": "公司公告",
      "items": [
        {
          "filename": "2024-12-01_profit-warning.pdf",
          "path": "knowledge/announcements/2024-12-01_profit-warning.pdf",
          "type": "announcement",
          "title": "盈利预告",
          "date": "2024-12-01",
          "market": "hk",
          "source": "HKEx | 巨潮资讯",
          "url": "https://...",
          "size_bytes": 102400,
          "downloaded_at": "2024-12-15T10:30:00"
        }
      ]
    },
    "research": {
      "description": "研究报告",
      "items": [
        {
          "filename": "2024-12-15_Goldman-Sachs.pdf",
          "path": "knowledge/research/2024-12-15_Goldman-Sachs.pdf",
          "type": "analyst-report | research-report",
          "firm": "Goldman Sachs",
          "analyst": "John Doe",
          "title": "Tencent: Strong Gaming Recovery",
          "date": "2024-12-15",
          "price_target": 450.0,
          "rating": "Buy",
          "market": "hk",
          "source": "FMP | SerpAPI | 东方财富",
          "has_pdf": true,
          "size_bytes": 1048576,
          "downloaded_at": "2024-12-15T10:30:00"
        }
      ]
    },
    "transcripts": {
      "description": "电话会纪要",
      "items": [
        {
          "filename": "2024-Q3-earnings-call.md",
          "path": "knowledge/transcripts/2024-Q3-earnings-call.md",
          "type": "earnings-call",
          "date": "2024-11-13",
          "quarter": "Q3",
          "year": "2024",
          "market": "hk",
          "source": "FMP",
          "size_bytes": 51200,
          "downloaded_at": "2024-12-15T10:30:00"
        }
      ]
    }
  },
  "stats": {
    "total_files": 42,
    "total_size_mb": 128.5
  }
}
```

## 文件命名规范

### 研报命名：`{日期}_{机构}.pdf`
- 例: `2024-12-15_Goldman-Sachs.pdf`
- 例: `2024-11-20_中金公司.pdf`
- 例: `2024-10-08_Morgan-Stanley_Buy-Rating.pdf`

### 财报命名：`{年份}-{类型}.pdf`
- 例: `2024-annual-report.pdf`
- 例: `2024-H1-interim-report.pdf`

### 公告命名：`{日期}_{标题摘要}.pdf`
- 例: `2024-12-01_profit-warning.pdf`
- 例: `2024-11-15_share-repurchase.pdf`

### 电话会命名：`{年份}-Q{季度}-earnings-call.md`
- 例: `2024-Q3-earnings-call.md`

## 数据来源覆盖

| 市场 | 财报 | 公告 | 研报 | 电话会 |
|------|------|------|------|--------|
| 港股 | HKEx 披露易 | HKEx | FMP + SerpAPI | FMP |
| A股 | 巨潮资讯 | 巨潮资讯 | 东方财富 + FMP | FMP |
| 美股 | FMP/SEC (10-K) | SEC | FMP + SerpAPI | FMP |

## 知名投研机构列表

### 国际
Goldman Sachs, Morgan Stanley, JP Morgan, UBS, Citigroup, HSBC, Deutsche Bank, Barclays, Credit Suisse, BofA Securities, Jefferies, Bernstein, CLSA, Macquarie, Daiwa, Nomura

### 中国
中金公司, 中信证券, 华泰证券, 国泰君安, 招商证券, 海通证券, 申万宏源, 广发证券, 兴业证券, 东方证券, 光大证券, 天风证券, 浙商证券, 国盛证券
