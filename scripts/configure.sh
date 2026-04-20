#!/usr/bin/env bash
# Interactive setup wizard for LangAlpha.
# Configures .env and agent_config.yaml based on your available services.
set -euo pipefail

BOLD='\033[1m'
DIM='\033[2m'
GREEN='\033[32m'
CYAN='\033[36m'
NC='\033[0m'

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="$REPO_ROOT/agent_config.yaml"
ENV_FILE="$REPO_ROOT/.env"
ENV_EXAMPLE="$REPO_ROOT/.env.example"

header()  { printf "\n${BOLD}${CYAN}── %s ──${NC}\n\n" "$1"; }
success() { printf "  ${GREEN}✓${NC} %s\n" "$1"; }
info()    { printf "  ${DIM}%s${NC}\n" "$1"; }

# --- Helpers ----------------------------------------------------------------

set_env() {
    local key="$1" val="$2"
    if grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
        # Portable sed -i (works on macOS + Linux)
        local tmp="${ENV_FILE}.tmp.$$"
        sed "s|^${key}=.*|${key}=${val}|" "$ENV_FILE" > "$tmp" && mv "$tmp" "$ENV_FILE"
    else
        echo "${key}=${val}" >> "$ENV_FILE"
    fi
}

# Toggle MCP server enabled flag by name
toggle_mcp() {
    local name="$1" enabled="$2"
    awk -v n="$name" -v e="$enabled" '
        /^[[:space:]]*- name:/ && index($0, "\"" n "\"") { hit=1 }
        hit && /^[[:space:]]*enabled:/ { sub(/enabled: *(true|false)/, "enabled: " e); hit=0 }
        { print }
    ' "$CONFIG" > "$CONFIG.tmp" && mv "$CONFIG.tmp" "$CONFIG"
}

# Set a field under the top-level llm: block
set_llm_field() {
    local field="$1" value="$2"
    awk -v f="$field" -v val="$value" '
        /^llm:/ { in_llm=1 }
        in_llm && /^  [a-z]/ && $0 ~ "^  "f":" { sub(f": *\"[^\"]*\"", f": \"" val "\""); in_llm=0 }
        /^[a-z]/ && !/^llm:/ { in_llm=0 }
        { print }
    ' "$CONFIG" > "$CONFIG.tmp" && mv "$CONFIG.tmp" "$CONFIG"
}

set_llm_null() {
    # Replace the entire llm: block (up to the next top-level key) with "llm: null"
    awk '
        /^llm:/ { print "llm: null"; skip=1; next }
        skip && /^[a-z]/ { skip=0 }
        skip { next }
        { print }
    ' "$CONFIG" > "${CONFIG}.tmp.$$" && mv "${CONFIG}.tmp.$$" "$CONFIG"
}

ensure_llm_block() {
    # If llm is currently "llm: null", expand to a full block so set_llm_field works.
    if grep -q '^llm: null' "$CONFIG" 2>/dev/null; then
        local tmp="${CONFIG}.tmp.$$"
        sed 's/^llm: null$/llm:\
  name: ""\
  flash: ""\
  compaction: ""\
  fetch: ""\
  fallback: []/' "$CONFIG" > "$tmp" && mv "$tmp" "$CONFIG"
    fi
}

set_search_api() {
    local tmp="${CONFIG}.tmp.$$"
    sed "s|^search_api:.*|search_api: ${1}|" "$CONFIG" > "$tmp" && mv "$tmp" "$CONFIG"
}

set_storage_provider() {
    local value="$1"
    awk -v val="$value" '
        /^storage:/ { in_storage=1 }
        in_storage && /^  provider:/ { sub(/provider: *"[^"]*"/, "provider: \"" val "\""); in_storage=0 }
        /^[a-z]/ && !/^storage:/ { in_storage=0 }
        { print }
    ' "$CONFIG" > "$CONFIG.tmp" && mv "$CONFIG.tmp" "$CONFIG"
}

prompt_choice() {
    local prompt="$1" default="$2" result
    read -rp "$(printf "${BOLD}%s${NC} [${default}]: " "$prompt")" result
    echo "${result:-$default}"
}

prompt_secret() {
    local prompt="$1" result
    read -rp "$(printf "  %s: " "$prompt")" result
    echo "$result"
}

MANIFEST_DIR="$REPO_ROOT/src/llms/manifest"

# Query model manifest files (providers.json + models.json)
_manifest() {
    python3 - "$MANIFEST_DIR" "$@" <<'PYEOF'
import json, sys
d, mode = sys.argv[1], sys.argv[2]
with open(f'{d}/providers.json') as f: pd = json.load(f)
with open(f'{d}/models.json') as f: md = json.load(f)
# Flatten grouped v2 provider_config (same logic as ModelConfig._flatten_providers)
pc = {}
for gk, cfg in pd['provider_config'].items():
    variants = cfg.get('variants')
    shared = {k: v for k, v in cfg.items() if k != 'variants'}
    if not variants:
        pc[gk] = shared; continue
    has_self = gk in variants
    for vk, ovr in variants.items():
        merged = {**shared, **ovr}
        if vk != gk: merged['parent_provider'] = gk
        pc[vk] = merged
    if not has_self:
        pc[gk] = shared
def _provider_family(p):
    return {p} | {k for k, c in pc.items() if c.get('parent_provider') == p}
def _has_models(p):
    fam = _provider_family(p)
    return any(v.get('provider') in fam and 'embedding' not in n for n, v in md.items())
if mode == 'providers':
    items = []
    for k, c in pc.items():
        if c.get('parent_provider') or c.get('access_type') in ('oauth', 'coding_plan'): continue
        dn, ek = c.get('display_name'), c.get('env_key')
        if not dn or not ek or ek == 'lm-studio': continue
        if not _has_models(k): continue
        items.append((k, dn, ek))
    for k, dn, ek in sorted(items, key=lambda x: x[1].lower()):
        print(f'{k}\t{dn}\t{ek}')
elif mode == 'models':
    p = sys.argv[3]
    fam = {p} | {k for k, c in pc.items() if c.get('parent_provider') == p}
    for m in sorted(n for n, v in md.items() if v.get('provider') in fam and 'embedding' not in n):
        print(m)
PYEOF
}

# =============================================================================

printf "\n${BOLD}${CYAN}LangAlpha Configuration Wizard${NC}\n"
printf "${DIM}Configures .env and agent_config.yaml for your setup.${NC}\n"
printf "${DIM}Re-run anytime — previous values will be overwritten.${NC}\n"

# Create .env if needed
if [ ! -f "$ENV_FILE" ]; then
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    success "Created .env from .env.example"
fi

# ---------------------------------------------------------------------------
# 1. LLM Provider
# ---------------------------------------------------------------------------
header "LLM Provider"
printf "  How will you access LLM models?\n\n"
printf "  ${BOLD}1${NC}) OAuth — connect your Claude or ChatGPT subscription in the UI\n"
printf "  ${BOLD}2${NC}) API key — choose from available providers\n"
printf "  ${BOLD}3${NC}) Skip — configure models later in the web UI\n\n"
info "You can change models later in agent_config.yaml (see models.json for all options)."
printf "\n"
llm=$(prompt_choice "Choice" "1")

case $llm in
    1)
        ensure_llm_block
        printf "\n"
        printf "  Which subscription will you connect?\n"
        printf "    ${BOLD}a${NC}) Claude (Anthropic)\n"
        printf "    ${BOLD}b${NC}) ChatGPT (OpenAI Codex)\n"
        printf "\n"
        sub=$(prompt_choice "Sub-choice" "a")
        case $sub in
            b)
                set_llm_field "name" "gpt-5.4-oauth"
                set_llm_field "flash" "gpt-5.4-mini-oauth"
                set_llm_field "compaction" "gpt-5.4-mini-oauth"
                set_llm_field "fetch" "gpt-5.4-mini-oauth"
                success "ChatGPT OAuth — connect your subscription in the UI after starting"
                ;;
            *)
                set_llm_field "name" "claude-sonnet-4-6-oauth"
                set_llm_field "flash" "claude-haiku-4-5-oauth"
                set_llm_field "compaction" "claude-haiku-4-5-oauth"
                set_llm_field "fetch" "claude-haiku-4-5-oauth"
                success "Claude OAuth — connect your subscription in the UI after starting"
                ;;
        esac
        ;;
    2)
        ensure_llm_block
        # Read providers dynamically from manifest
        _providers=(); _p_names=(); _p_envs=()
        while IFS=$'\t' read -r key name env; do
            _providers+=("$key"); _p_names+=("$name"); _p_envs+=("$env")
        done < <(_manifest providers)

        printf "\n  Available providers:\n\n"
        for i in "${!_providers[@]}"; do
            printf "    ${BOLD}%2d${NC}) %s\n" "$((i+1))" "${_p_names[$i]}"
        done
        printf "\n"

        p_idx=$(prompt_choice "Provider" "1")
        p_idx=$((p_idx - 1))
        _sel_provider="${_providers[$p_idx]}"
        _sel_env="${_p_envs[$p_idx]}"
        _sel_display="${_p_names[$p_idx]}"

        # Prompt for API key
        key=$(prompt_secret "$_sel_env")
        [ -n "$key" ] && set_env "$_sel_env" "$key"

        # Read models for selected provider
        _models=()
        while IFS= read -r m; do
            _models+=("$m")
        done < <(_manifest models "$_sel_provider")

        if [ ${#_models[@]} -eq 0 ]; then
            info "No models found for $_sel_display — edit agent_config.yaml manually."
        elif [ ${#_models[@]} -eq 1 ]; then
            set_llm_field "name" "${_models[0]}"
            set_llm_field "flash" "${_models[0]}"
            set_llm_field "compaction" "${_models[0]}"
            set_llm_field "fetch" "${_models[0]}"
            success "$_sel_display — ${_models[0]}"
        else
            printf "\n  Available models for %s:\n\n" "$_sel_display"
            for i in "${!_models[@]}"; do
                printf "    ${BOLD}%2d${NC}) %s\n" "$((i+1))" "${_models[$i]}"
            done
            printf "\n"

            main_idx=$(prompt_choice "Main model" "1")
            flash_idx=$(prompt_choice "Flash/light model" "${#_models[@]}")
            main_model="${_models[$((main_idx-1))]}"
            flash_model="${_models[$((flash_idx-1))]}"

            set_llm_field "name" "$main_model"
            set_llm_field "flash" "$flash_model"
            set_llm_field "compaction" "$flash_model"
            set_llm_field "fetch" "$flash_model"
            success "$_sel_display — $main_model (main), $flash_model (flash)"
        fi
        printf "\n"
        info "Tip: edit agent_config.yaml to add fallback models, or switch models in the web UI."
        ;;
    3)
        set_llm_null
        success "LLM set to null — the setup wizard will guide you through model selection on first launch."
        ;;
    *)
        info "Skipping LLM config — edit agent_config.yaml manually."
        ;;
esac

# ---------------------------------------------------------------------------
# 2. Financial Data
# ---------------------------------------------------------------------------
header "Financial Data"
printf "  ${BOLD}1${NC}) Yahoo Finance — free, no API key needed\n"
printf "  ${BOLD}2${NC}) FMP (Financial Modeling Prep) — high-quality data (free tier available)\n"
printf "  ${BOLD}3${NC}) Both — FMP + Yahoo Finance MCP servers\n"
printf "\n"
info "See README > Data Provider Fallback Chain for details."
printf "\n"
data=$(prompt_choice "Choice" "1")

case $data in
    1)
        for s in price_data fundamentals macro options; do toggle_mcp "$s" "false"; done
        for s in yf_price yf_fundamentals yf_analysis yf_market; do toggle_mcp "$s" "true"; done
        success "Yahoo Finance MCP servers enabled (free data, no API key)"
        ;;
    2)
        key=$(prompt_secret "FMP_API_KEY")
        [ -n "$key" ] && set_env "FMP_API_KEY" "$key"
        for s in price_data fundamentals macro options; do toggle_mcp "$s" "true"; done
        for s in yf_price yf_fundamentals yf_analysis yf_market; do toggle_mcp "$s" "false"; done
        success "FMP configured — full financial data MCP servers enabled"
        ;;
    3)
        key=$(prompt_secret "FMP_API_KEY")
        [ -n "$key" ] && set_env "FMP_API_KEY" "$key"
        for s in price_data fundamentals macro options; do toggle_mcp "$s" "true"; done
        for s in yf_price yf_fundamentals yf_analysis yf_market; do toggle_mcp "$s" "true"; done
        success "FMP + Yahoo Finance — all financial data MCP servers enabled"
        ;;
    *)
        info "Skipping data config."
        ;;
esac

# ---------------------------------------------------------------------------
# 3. Sandbox
# ---------------------------------------------------------------------------
header "Sandbox (Code Execution)"
printf "  ${BOLD}1${NC}) Docker — local containers, no signup needed\n"
printf "  ${BOLD}2${NC}) Daytona — cloud sandboxes with persistent workspaces\n"
printf "\n"
sandbox=$(prompt_choice "Choice" "1")

case $sandbox in
    1)
        set_env "SANDBOX_PROVIDER" "docker"
        success "Docker sandbox — image will be built on first 'make up'"
        ;;
    2)
        key=$(prompt_secret "DAYTONA_API_KEY")
        [ -n "$key" ] && set_env "DAYTONA_API_KEY" "$key"
        set_env "SANDBOX_PROVIDER" "daytona"
        success "Daytona configured — cloud sandboxes with workspace persistence"
        ;;
    *)
        info "Skipping sandbox config."
        ;;
esac

# ---------------------------------------------------------------------------
# 4. Web Search
# ---------------------------------------------------------------------------
header "Web Search (optional)"
printf "  ${BOLD}1${NC}) None — skip for now\n"
printf "  ${BOLD}2${NC}) Serper (serper.dev)\n"
printf "  ${BOLD}3${NC}) Tavily (tavily.com)\n"
printf "\n"
search=$(prompt_choice "Choice" "1")

case $search in
    1)
        info "No web search configured — agent will rely on financial data tools only."
        ;;
    2)
        key=$(prompt_secret "SERPER_API_KEY")
        [ -n "$key" ] && set_env "SERPER_API_KEY" "$key"
        set_search_api "serper"
        success "Serper configured"
        ;;
    3)
        key=$(prompt_secret "TAVILY_API_KEY")
        [ -n "$key" ] && set_env "TAVILY_API_KEY" "$key"
        set_search_api "tavily"
        success "Tavily configured"
        ;;
    *)
        info "Skipping search config."
        ;;
esac

# ---------------------------------------------------------------------------
# 5. Cloud Storage (optional — for chart image uploads)
# ---------------------------------------------------------------------------
header "Cloud Storage (optional)"
printf "  Used to upload chart images so the agent can share them in responses.\n\n"
printf "  ${BOLD}1${NC}) None — skip (charts stay local)\n"
printf "  ${BOLD}2${NC}) AWS S3\n"
printf "  ${BOLD}3${NC}) Cloudflare R2\n"
printf "  ${BOLD}4${NC}) Alibaba Cloud OSS\n"
printf "\n"
storage=$(prompt_choice "Choice" "1")

case $storage in
    1)
        set_storage_provider "none"
        info "Cloud storage disabled — charts will not be uploaded."
        ;;
    2)
        set_storage_provider "s3"
        success "AWS S3 selected — set S3_* credentials in .env"
        ;;
    3)
        set_storage_provider "r2"
        success "Cloudflare R2 selected — set R2_* credentials in .env"
        ;;
    4)
        set_storage_provider "oss"
        success "Alibaba Cloud OSS selected — set OSS_* credentials in .env"
        ;;
    *)
        info "Skipping storage config."
        ;;
esac

# ---------------------------------------------------------------------------
# 6. Infrastructure (PostgreSQL + Redis)
# ---------------------------------------------------------------------------
header "Infrastructure (PostgreSQL + Redis)"
printf "  ${BOLD}1${NC}) Docker Compose — start PostgreSQL and Redis in containers (default)\n"
printf "  ${BOLD}2${NC}) External — use your own PostgreSQL and Redis instances\n"
printf "\n"
infra=$(prompt_choice "Choice" "1")

case $infra in
    1)
        set_env "COMPOSE_PROFILES" "infra"
        success "Docker Compose will manage PostgreSQL and Redis"
        ;;
    2)
        set_env "COMPOSE_PROFILES" ""
        printf "\n"
        info "Configure the following in .env to point to your instances:"
        printf "\n"
        printf "    ${DIM}# Database${NC}\n"
        printf "    ${DIM}DB_HOST=<host>${NC}\n"
        printf "    ${DIM}DB_PORT=5432${NC}\n"
        printf "    ${DIM}DB_USER=<user>${NC}\n"
        printf "    ${DIM}DB_PASSWORD=<password>${NC}\n"
        printf "    ${DIM}DB_NAME=<database>${NC}\n"
        printf "\n"
        printf "    ${DIM}# Redis${NC}\n"
        printf "    ${DIM}REDIS_URL=redis://:<password>@<host>:6379/0${NC}\n"
        printf "\n"
        success "External infrastructure — edit .env with your connection details"
        ;;
    *)
        info "Skipping infrastructure config."
        ;;
esac

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
header "Pre-flight Checks"

WARN='\033[33m'
_warn() { printf "  ${WARN}!${NC} %s\n" "$1"; }

# Docker
if command -v docker &>/dev/null; then
    if docker info &>/dev/null; then
        success "Docker is installed and running"
    else
        _warn "Docker is installed but not running — start Docker Desktop or the daemon"
    fi
else
    _warn "Docker not found — install Docker to use 'make up'"
fi

# Ports
_check_port() {
    local port="$1" label="$2"
    if lsof -iTCP:"$port" -sTCP:LISTEN -P -n &>/dev/null 2>&1; then
        _warn "Port $port ($label) is already in use"
    else
        success "Port $port ($label) is free"
    fi
}

_check_port 8000 "backend"
_check_port 5173 "frontend"
_check_port 5432 "PostgreSQL"
_check_port 6379 "Redis"

# --- Security: BYOK encryption key ----------------------------------------
_BYOK=$(grep '^BYOK_ENCRYPTION_KEY=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- || true)
if [ -z "$_BYOK" ] || [ "$_BYOK" = "langalpha-local-dev-encryption-key" ]; then
    set_env BYOK_ENCRYPTION_KEY "$(openssl rand -hex 32)"
    success "Generated BYOK_ENCRYPTION_KEY"
else
    info "BYOK_ENCRYPTION_KEY already set"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
header "Done"
printf "  Files updated:\n"
printf "    ${DIM}.env${NC}               API keys and service settings\n"
printf "    ${DIM}agent_config.yaml${NC}  LLM models, MCP servers, search provider\n"
printf "\n"
printf "  Next steps:\n"
printf "    ${BOLD}make up${NC}            Start the full stack\n"
printf "    ${BOLD}make help${NC}          See all available commands\n"
printf "\n"
printf "  Want more models or features? Edit these files:\n"
printf "    ${DIM}.env${NC}               Add API keys for additional providers\n"
printf "    ${DIM}agent_config.yaml${NC}  Add fallback models, enable MCP servers, tune settings\n"
printf "    ${DIM}src/llms/manifest/${NC}  See all configured models or add custom ones\n"
printf "\n"
