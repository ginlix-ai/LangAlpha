# User Preferences API - Valid Values Reference

This document lists all valid enum values for the User Preferences API endpoint: `PUT /api/v1/users/me/preferences`

## Request Body Structure

```json
{
  "risk_preference": {
    "risk_tolerance": "<value>"
  },
  "investment_preference": {
    "company_interest": "<value>",
    "holding_period": "<value>",
    "analysis_focus": "<value>"
  },
  "agent_preference": {
    "output_style": "<value>"
  }
}
```

---

## 1. Risk Preference

### `risk_preference.risk_tolerance`

**Valid Values:**
- `"low"` - Low risk tolerance
- `"medium"` - Medium risk tolerance
- `"high"` - High risk tolerance
- `"long_term_focus"` - Long-term focus (lower risk, patient investing)

**Example:**
```json
{
  "risk_preference": {
    "risk_tolerance": "medium"
  }
}
```

**Source:** `src/server/models/user.py` - `RiskTolerance` enum (lines 44-50)

---

## 2. Investment Preference

### `investment_preference.company_interest`

**Valid Values:**
- `"growth"` - Growth companies
- `"stable"` - Stable companies
- `"value"` - Value companies
- `"esg"` - ESG-focused companies

**Example:**
```json
{
  "investment_preference": {
    "company_interest": "growth"
  }
}
```

**Source:** `src/server/models/user.py` - `CompanyInterest` enum (lines 53-59)

---

### `investment_preference.holding_period`

**Valid Values:**
- `"short_term"` - Short-term holding period
- `"mid_term"` - Mid-term holding period
- `"long_term"` - Long-term holding period
- `"flexible"` - Flexible holding period

**Example:**
```json
{
  "investment_preference": {
    "holding_period": "long_term"
  }
}
```

**Source:** `src/server/models/user.py` - `HoldingPeriod` enum (lines 62-68)

---

### `investment_preference.analysis_focus`

**Valid Values:**
- `"growth"` - Focus on growth analysis
- `"valuation"` - Focus on valuation analysis
- `"moat"` - Focus on competitive moat analysis
- `"risk"` - Focus on risk analysis

**Example:**
```json
{
  "investment_preference": {
    "analysis_focus": "moat"
  }
}
```

**Source:** `src/server/models/user.py` - `AnalysisFocus` enum (lines 71-77)

---

## 3. Agent Preference

### `agent_preference.output_style`

**Valid Values:**
- `"summary"` - Summary output style
- `"data"` - Data-focused output style
- `"deep_dive"` - Deep dive analysis output style
- `"quick"` - Quick output style

**Example:**
```json
{
  "agent_preference": {
    "output_style": "deep_dive"
  }
}
```

**Source:** `src/server/models/user.py` - `OutputStyle` enum (lines 80-86)

---

## Complete Example Request

```json
{
  "risk_preference": {
    "risk_tolerance": "medium"
  },
  "investment_preference": {
    "company_interest": "growth",
    "holding_period": "long_term",
    "analysis_focus": "moat"
  },
  "agent_preference": {
    "output_style": "deep_dive"
  }
}
```

---

## Additional Notes

1. **Partial Updates:** You can update only specific preference sections. You don't need to include all fields.

2. **Extra Fields:** The models allow additional fields beyond the enum values (via `extra = "allow"`), so you can add custom fields like `notes`, `avoid_sectors`, `focus_sectors`, etc.

3. **Optional Fields:** All preference fields are optional. You can update just one section at a time.

4. **Validation:** Invalid enum values will result in a validation error from the backend.

---

## API Endpoint

**Method:** `PUT`  
**URL:** `http://localhost:8000/api/v1/users/me/preferences`  
**Headers:**
- `Content-Type: application/json`
- `X-User-Id: <user_id>`

---

## Source Files

- **Enum Definitions:** `src/server/models/user.py` (lines 44-86)
- **Validation Logic:** `src/tools/user_profile/tools.py` (lines 224-333)
- **API Endpoint:** `src/server/app/users.py` (lines 191-231)
