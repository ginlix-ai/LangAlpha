# Timezone and Locale Support Reference

This document explains the timezone and locale support for the User API endpoints.

---

## Timezone Support

### Field: `timezone`

**Type:** `Optional[str]`  
**Max Length:** 100 characters  
**Format:** IANA timezone identifier  
**Validation:** Validated using Python's `zoneinfo.ZoneInfo`

### Supported Timezones

The API accepts **any valid IANA timezone identifier** from the IANA Time Zone Database. There are **hundreds of timezones** supported (typically 400+).

#### Common Examples:

**Americas:**
- `America/New_York` - Eastern Time (EST/EDT)
- `America/Chicago` - Central Time (CST/CDT)
- `America/Denver` - Mountain Time (MST/MDT)
- `America/Los_Angeles` - Pacific Time (PST/PDT)
- `America/Toronto` - Eastern Time (Canada)
- `America/Vancouver` - Pacific Time (Canada)
- `America/Mexico_City` - Central Time (Mexico)
- `America/Sao_Paulo` - Brasília Time (Brazil)
- `America/Buenos_Aires` - Argentina Time

**Europe:**
- `Europe/London` - Greenwich Mean Time / British Summer Time
- `Europe/Paris` - Central European Time
- `Europe/Berlin` - Central European Time
- `Europe/Moscow` - Moscow Time
- `Europe/Istanbul` - Turkey Time

**Asia:**
- `Asia/Shanghai` - China Standard Time
- `Asia/Tokyo` - Japan Standard Time
- `Asia/Hong_Kong` - Hong Kong Time
- `Asia/Singapore` - Singapore Time
- `Asia/Dubai` - Gulf Standard Time
- `Asia/Kolkata` - India Standard Time
- `Asia/Seoul` - Korea Standard Time

**Oceania:**
- `Australia/Sydney` - Australian Eastern Time
- `Australia/Melbourne` - Australian Eastern Time
- `Australia/Perth` - Australian Western Time
- `Pacific/Auckland` - New Zealand Time

**Other:**
- `UTC` - Coordinated Universal Time
- `GMT` - Greenwich Mean Time

### Validation

The timezone is validated using Python's `zoneinfo.ZoneInfo` library, which:
- Uses the IANA Time Zone Database
- Supports all standard timezone identifiers
- Automatically handles Daylight Saving Time (DST)
- Throws `ZoneInfoNotFoundError` for invalid timezones

**Example:**
```python
from zoneinfo import ZoneInfo

# Valid
ZoneInfo("America/New_York")  # ✅ Works

# Invalid
ZoneInfo("Invalid/Timezone")  # ❌ Raises ZoneInfoNotFoundError
```

### How to Find Valid Timezones

You can get a list of all valid IANA timezones using:

**Python:**
```python
import zoneinfo
all_timezones = zoneinfo.available_timezones()
print(len(all_timezones))  # Typically 400+
```

**Online Resources:**
- [IANA Time Zone Database](https://www.iana.org/time-zones)
- [List of tz database time zones (Wikipedia)](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)

---

## Locale Support

### Field: `locale`

**Type:** `Optional[str]`  
**Max Length:** 20 characters  
**Format:** BCP 47 locale identifier (language-country)  
**Validation:** No explicit validation - accepts any string

### Supported Locales

The API accepts **any locale string** up to 20 characters. There is **no explicit validation or restricted list**. However, the system is designed to work with standard BCP 47 locale codes.

#### Common Examples:

**English:**
- `en-US` - English (United States)
- `en-GB` - English (United Kingdom)
- `en-CA` - English (Canada)
- `en-AU` - English (Australia)

**Chinese:**
- `zh-CN` - Chinese (Simplified, China)
- `zh-TW` - Chinese (Traditional, Taiwan)
- `zh-HK` - Chinese (Traditional, Hong Kong)

**Other Languages:**
- `fr-FR` - French (France)
- `de-DE` - German (Germany)
- `ja-JP` - Japanese (Japan)
- `ko-KR` - Korean (Korea)
- `es-ES` - Spanish (Spain)
- `es-MX` - Spanish (Mexico)
- `pt-BR` - Portuguese (Brazil)
- `ru-RU` - Russian (Russia)
- `ar-SA` - Arabic (Saudi Arabia)
- `hi-IN` - Hindi (India)

### Locale Format

Locales typically follow the BCP 47 format:
- **Language code** (ISO 639-1): 2-letter language code (e.g., `en`, `zh`, `fr`)
- **Country code** (ISO 3166-1): 2-letter country code (e.g., `US`, `CN`, `FR`)
- **Format:** `{language}-{country}` (e.g., `en-US`, `zh-CN`)

### System Behavior

The system uses locale for:
1. **Timezone mapping** (in chat requests):
   - `en-US` → `America/New_York`
   - `zh-CN` → `Asia/Shanghai`
   - Others → `UTC`

2. **Language preferences** (for future features)

### Notes

- **No validation**: The API does not validate locale strings against a specific list
- **Flexible**: You can use any locale string that makes sense for your use case
- **Recommended**: Use standard BCP 47 format for best compatibility
- **Future-proof**: The system may add locale validation in the future

---

## API Request Examples

### Create User with Timezone and Locale

```json
{
  "email": "user@example.com",
  "name": "John Doe",
  "timezone": "America/New_York",
  "locale": "en-US"
}
```

### Update User Timezone and Locale

```json
{
  "timezone": "Asia/Shanghai",
  "locale": "zh-CN"
}
```

---

## Summary

| Field | Type | Max Length | Validation | Supported Values |
|-------|------|------------|------------|------------------|
| `timezone` | `Optional[str]` | 100 | IANA timezone validation | **400+** IANA timezone identifiers |
| `locale` | `Optional[str]` | 20 | None (flexible) | **Unlimited** (BCP 47 format recommended) |

---

## Source Files

- **Model Definition:** `src/server/models/user.py` (lines 180-184)
- **Timezone Validation:** `src/server/app/chat.py` (lines 252-256)
- **Locale Mapping:** `src/config/settings.py` (lines 340-385)
- **API Endpoint:** `src/server/app/users.py` (lines 41-72)

---

## Additional Resources

- [IANA Time Zone Database](https://www.iana.org/time-zones)
- [BCP 47 Language Tags](https://tools.ietf.org/html/bcp47)
- [ISO 639-1 Language Codes](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes)
- [ISO 3166-1 Country Codes](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)
