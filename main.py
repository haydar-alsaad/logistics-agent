"""
Barq Express Delivery Agent API - v1.0 (multi-tenant, dual auth)
Architecture: Supabase-backed via httpx REST (PostgREST + Storage), one
self-owned Supabase project for the Barq vertical. Backs the Barq node of the
T2 multi-agent WhatsApp workflow (router → Barq / Watheeq).

v1.0 — DESIGN NOTES (why this file looks the way it does)
  This service is a sibling of the Al-Noor healthcare API (v3.5) and copies
  its machinery on purpose. Every pattern below exists because its absence
  cost us a demo or a day on an earlier vertical:

  AUTH — TWO MODES, ONE CODE PATH
    Mode A (preferred): SUPABASE_SERVICE_ROLE_KEY. Barq is a self-owned
      Supabase project, so the service key is available. It bypasses RLS and
      never expires — no token dance.
    Mode B (fallback): service-account password grant, copied verbatim from
      healthcare v3.x (Lovable Cloud projects never expose the service key).
      Selected automatically when SUPABASE_SERVICE_ROLE_KEY is absent.
      Proactive refresh 5 min before expiry, refresh-token grant first,
      password grant as fallback, asyncio.Lock so N concurrent requests do
      ONE token fetch, and any 401/403 forces re-auth + one retry.
    In both modes a 401/403 that survives the retry raises 502. An empty
    list masquerading as "no rows" is what made a broken key look like
    "patient not found" for hours on healthcare. Never again.

  TENANCY (playbook §3)
    Per-tenant tables carry owner_id; every sb_* helper takes an explicit
    `owner` and `_scope_params` RAISES if a per-tenant query has none — a
    500 in the logs beats serving another sales person's demo data.
    caller_phone → owner_id via demo_users (5-min TTL cache, phone
    normalisation incl. Saudi local "05…" form); unregistered callers land in
    DEFAULT_OWNER_ID (a demo that lands in the shared tenant is recoverable;
    a hard 400 in front of a prospect is not). /whoami explains the routing.

  TIME IS SERVER-SIDE
    "Now" = datetime.now(ZoneInfo(TZ_NAME)). Every date the agent will quote
    comes with *_label_en / *_label_ar ("Sunday, 4 October" / "الأحد 4
    أكتوبر") so the model never computes a weekday — LLMs get weekdays wrong
    often enough to break a demo. Availability (delivery windows) is never
    stored as a grid: it is computed from today + rules, so nothing goes
    stale after the nightly date-shift clone.

  PRAYER TIMES
    Inline solar-position algorithm (Umm al-Qura: Fajr 18.5°, Isha =
    Maghrib + 90 min, Asr standard shadow 1). No network, no dependency —
    a prayer-time API outage must not break slot selection.

  LOCATION SNAPPING
    Presenters demo from Dubai, Amsterdam, Madrid. A WhatsApp pin > 60 km
    from every city in the reference data snaps (DEMO_LOCATION_SNAP=true) to
    the shipment's delivery district / customer's home district and the
    response says location_snapped:true. The SI never mentions it.

  WORKHORSE /customer
    One lookup, then ALL related fetches in ONE asyncio.gather; enrichment,
    segmentation and eligibility in Python. Eligibility flags are the
    contract with the SI: the agent never decides a rule itself, it reads
    can_open_damage_claim / damage_claim_deadline_label_en etc.

  WRITES
    validate → duplicate check → entity exists → write → log_agent_action →
    {ok:true,…}. Errors are 400/404/409 with detail {code, message, …ctx};
    `code` values are stable enums the SI keys its failure templates on.

  MEDIA + DOCUMENTS
    attachment_urls are downloaded server-side and copied to Storage (never
    failing the write on a fetch error). PDFs are rendered with reportlab
    (Arabic via bundled Amiri TTF + arabic-reshaper + python-bidi), uploaded
    with x-upsert, and returned as a 1-hour signed URL.

  RESPONSE PRUNING (from healthcare v3.5)
    owner_id / created_at / updated_at never leave the API; secrets
    (delivery_code, tracking_token, full national IDs) are never returned
    by the workhorse.

TENANCY MODEL:
  Per-tenant (owner_id): customers, shipments, shipment_events,
    authorized_receivers, cases, returns, payment_requests, tax_invoices,
    store_alerts, agent_actions
  Shared (read-only, cached 60 s): districts, centres, pickup_points, stores,
    drivers, business_rules, delivery_windows, demo_meta, demo_assets

ENV VARS:
  SUPABASE_URL               (required)  e.g. https://xxxx.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  (mode A)    service key — SECRET
  SUPABASE_ANON_KEY          (mode B)    anon/publishable key
  SUPABASE_SERVICE_EMAIL     (mode B)    service-account email
  SUPABASE_SERVICE_PASSWORD  (mode B)    service-account password — SECRET
  DEFAULT_OWNER_ID           (required)  UUID of the fallback demo tenant
  PORTAL_BASE_URL            (required)  e.g. https://barq-portal.lovable.app
  SUPABASE_REST_URL          (optional)  default {SUPABASE_URL}/rest/v1
  SUPABASE_STORAGE_URL       (optional)  default {SUPABASE_URL}/storage/v1
  SUPABASE_AUTH_URL          (optional)  default {SUPABASE_URL}/auth/v1
  DOCS_BUCKET                (optional)  default "documents"
  DEMO_LOCATION_SNAP         (optional)  default "true"
  TZ_NAME                    (optional)  default "Asia/Riyadh"
"""

import asyncio
import base64
import difflib
import hashlib
import io
import math
import mimetypes
import os
import re
import secrets
import unicodedata
import uuid
from datetime import date, datetime, time as dtime, timedelta, timezone
from time import monotonic as _monotonic
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

API_VERSION = "1.0"
SERVICE_NAME = "Barq Express Delivery Agent API"


# ============================================================
# Config
# ============================================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
SUPABASE_SERVICE_EMAIL = os.environ.get("SUPABASE_SERVICE_EMAIL", "")
SUPABASE_SERVICE_PASSWORD = os.environ.get("SUPABASE_SERVICE_PASSWORD", "")
DEFAULT_OWNER_ID = os.environ.get("DEFAULT_OWNER_ID", "")
PORTAL_BASE_URL = os.environ.get("PORTAL_BASE_URL", "").rstrip("/")
SUPABASE_REST_URL = (os.environ.get("SUPABASE_REST_URL") or f"{SUPABASE_URL}/rest/v1").rstrip("/")
SUPABASE_STORAGE_URL = (os.environ.get("SUPABASE_STORAGE_URL") or f"{SUPABASE_URL}/storage/v1").rstrip("/")
SUPABASE_AUTH_URL = (os.environ.get("SUPABASE_AUTH_URL") or f"{SUPABASE_URL}/auth/v1").rstrip("/")
DOCS_BUCKET = os.environ.get("DOCS_BUCKET", "documents")
DEMO_LOCATION_SNAP = os.environ.get("DEMO_LOCATION_SNAP", "true").lower() in ("1", "true", "yes")
TZ_NAME = os.environ.get("TZ_NAME", "Asia/Riyadh")
TZ = ZoneInfo(TZ_NAME)

# Mode A when the service key is present; Mode B otherwise. Decided once at
# boot — flipping modes means a redeploy, which is what you want.
AUTH_MODE = "service_role" if SUPABASE_SERVICE_ROLE_KEY else "service_account"

_required = [("SUPABASE_URL", SUPABASE_URL), ("DEFAULT_OWNER_ID", DEFAULT_OWNER_ID),
             ("PORTAL_BASE_URL", PORTAL_BASE_URL)]
if AUTH_MODE == "service_account":
    _required += [
        ("SUPABASE_ANON_KEY", SUPABASE_ANON_KEY),
        ("SUPABASE_SERVICE_EMAIL", SUPABASE_SERVICE_EMAIL),
        ("SUPABASE_SERVICE_PASSWORD", SUPABASE_SERVICE_PASSWORD),
    ]
_missing = [name for name, val in _required if not val]
if _missing:
    print(f"WARNING: missing required env vars: {', '.join(_missing)}. API will fail.")

FONTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts")


# ============================================================
# Error helpers — every 4xx carries {code, message, ...context}
# ============================================================
# The SI keys its failure templates on `code`. Free-text details are what
# made the healthcare agent paraphrase errors inconsistently; a stable enum
# lets the SI say exactly the right thing ("claims must be opened within 7
# days — this was delivered on …") without guessing.

def api_error(http_status: int, code: str, message: str, **context) -> HTTPException:
    return HTTPException(status_code=http_status, detail={"code": code, "message": message, **context})


def not_found(code: str, message: str, **ctx) -> HTTPException:
    return api_error(404, code, message, **ctx)


def bad_request(code: str, message: str, **ctx) -> HTTPException:
    return api_error(400, code, message, **ctx)


def conflict(code: str, message: str, **ctx) -> HTTPException:
    return api_error(409, code, message, **ctx)


# ============================================================
# Supabase auth — Mode A (service key) / Mode B (service account)
# ============================================================
# Mode B token lifecycle (copied from healthcare v3.x):
#   startup            -> password grant, cache access + refresh token
#   < 5 min to expiry  -> refresh_token grant (cheap)
#   refresh rejected   -> fall back to a fresh password grant
#   PostgREST 401/403  -> force re-auth, retry the request once
# A single asyncio.Lock serialises all of the above so N concurrent requests
# trigger one token fetch rather than N.

_TOKEN_REFRESH_MARGIN = 300.0  # refresh when < 5 min of life remains

_auth_state: dict = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": 0.0,       # monotonic deadline
    "last_error": None,
    "signed_in_at": None,    # wall-clock ISO, for diagnostics
}
_auth_lock: Optional[asyncio.Lock] = None  # created lazily (needs a loop)


async def _auth_request(payload: dict, grant_type: str) -> dict:
    """POST to Supabase's token endpoint. Raises on failure."""
    url = f"{SUPABASE_AUTH_URL}/token?grant_type={grant_type}"
    r = await http_client.post(
        url,
        headers={"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"},
        json=payload,
    )
    r.raise_for_status()
    return r.json()


async def _sign_in_password() -> None:
    """Full sign-in with email + password. Replaces any cached token."""
    data = await _auth_request(
        {"email": SUPABASE_SERVICE_EMAIL, "password": SUPABASE_SERVICE_PASSWORD},
        "password",
    )
    _store_token(data)
    print(f"[auth] signed in as {SUPABASE_SERVICE_EMAIL}")


async def _sign_in_refresh() -> None:
    """Renew using the refresh token. Cheaper than a password grant."""
    rt = _auth_state.get("refresh_token")
    if not rt:
        raise RuntimeError("no refresh token cached")
    data = await _auth_request({"refresh_token": rt}, "refresh_token")
    _store_token(data)
    print("[auth] token refreshed")


def _store_token(data: dict) -> None:
    expires_in = float(data.get("expires_in") or 3600)
    _auth_state["access_token"] = data.get("access_token")
    _auth_state["refresh_token"] = data.get("refresh_token") or _auth_state.get("refresh_token")
    _auth_state["expires_at"] = _monotonic() + expires_in
    _auth_state["last_error"] = None
    _auth_state["signed_in_at"] = datetime.now().astimezone().isoformat()


async def ensure_token(force: bool = False) -> str:
    """Return a valid bearer token.

    Mode A: the service key itself (never expires, nothing to refresh).
    Mode B: cached user token, refreshed or re-signed-in as needed.
    `force=True` discards the cached token — used after a 401/403.
    """
    if AUTH_MODE == "service_role":
        return SUPABASE_SERVICE_ROLE_KEY

    global _auth_lock
    if _auth_lock is None:
        _auth_lock = asyncio.Lock()

    tok = _auth_state.get("access_token")
    fresh_enough = tok and (_auth_state["expires_at"] - _monotonic()) > _TOKEN_REFRESH_MARGIN
    if fresh_enough and not force:
        return tok

    async with _auth_lock:
        # Re-check inside the lock: another coroutine may have just refreshed.
        tok = _auth_state.get("access_token")
        fresh_enough = tok and (_auth_state["expires_at"] - _monotonic()) > _TOKEN_REFRESH_MARGIN
        if fresh_enough and not force:
            return tok
        try:
            if force:
                await _sign_in_password()
            else:
                try:
                    await _sign_in_refresh()
                except Exception:
                    await _sign_in_password()
        except Exception as e:
            _auth_state["last_error"] = str(e)[:300]
            print(f"[auth] sign-in FAILED: {e}")
            raise HTTPException(
                status_code=502,
                detail={"code": "upstream_auth_failed",
                        "message": "Database authentication failed — check service account credentials"},
            )
        return _auth_state["access_token"]


def _apikey() -> str:
    return SUPABASE_SERVICE_ROLE_KEY if AUTH_MODE == "service_role" else SUPABASE_ANON_KEY


async def sb_headers(extra: Optional[dict] = None) -> dict:
    """Build request headers with a currently-valid token."""
    token = await ensure_token()
    h = {
        "apikey": _apikey(),
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    if extra:
        h.update(extra)
    return h


def _auth_stats() -> dict:
    """Diagnostic: token state, exposed via /health. Never leaks the token."""
    if AUTH_MODE == "service_role":
        return {"mode": "service_role", "authenticated": bool(SUPABASE_SERVICE_ROLE_KEY),
                "expires_in_seconds": None, "signed_in_at": None, "last_error": None}
    exp = _auth_state.get("expires_at") or 0
    return {
        "mode": "service_account",
        "authenticated": bool(_auth_state.get("access_token")),
        "expires_in_seconds": round(exp - _monotonic(), 1) if exp else None,
        "signed_in_at": _auth_state.get("signed_in_at"),
        "last_error": _auth_state.get("last_error"),
    }


# ============================================================
# Tenancy: which tables carry owner_id
# ============================================================

TENANT_TABLES = {
    "customers",
    "shipments",
    "shipment_events",
    "authorized_receivers",
    "cases",
    "returns",
    "payment_requests",
    "tax_invoices",
    "store_alerts",
    "agent_actions",
}


# ============================================================
# App
# ============================================================

app = FastAPI(title=SERVICE_NAME, version=API_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

http_client: Optional[httpx.AsyncClient] = None


@app.exception_handler(RequestValidationError)
async def _validation_handler(request: Request, exc: RequestValidationError):
    """FastAPI's default 422 has no `code`. The SI keys on codes, so body
    validation failures come back in the same {code, message} shape as every
    other 400."""
    errs = exc.errors()
    fields = [".".join(str(p) for p in e.get("loc", []) if p != "body") for e in errs]
    return JSONResponse(status_code=400, content={"detail": {
        "code": "validation_error",
        "message": "Invalid or missing fields: " + ", ".join(f for f in fields if f),
        "fields": fields,
    }})


@app.on_event("startup")
async def startup():
    global http_client
    # HTTP/2 multiplexes the workhorse's ~10 parallel queries over ONE TCP
    # connection. On HTTP/1.1 the ~6-connection limit queues the rest for
    # ~300 ms. Requires h2 (httpx[http2]).
    http_client = httpx.AsyncClient(
        http2=True,
        timeout=30.0,
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
    )
    if AUTH_MODE == "service_account":
        # Sign in up front so the first real request doesn't pay for it. Don't
        # crash on failure — /health reports it and every request retries.
        try:
            await ensure_token(force=True)
        except Exception as e:
            print(f"[auth] startup sign-in failed (will retry on first request): {e}")
    print(f"[boot] {SERVICE_NAME} v{API_VERSION} auth_mode={AUTH_MODE} tz={TZ_NAME} "
          f"snap={DEMO_LOCATION_SNAP} rest={SUPABASE_REST_URL}")


@app.on_event("shutdown")
async def shutdown():
    global http_client
    if http_client:
        await http_client.aclose()


# ============================================================
# Phone normalization + tenant resolution
# ============================================================

def normalize_phone(raw: Optional[str]) -> Optional[str]:
    """Canonicalize a phone number to E.164 with a leading '+'.

    Tolerates: missing '+', spaces, dashes, parentheses, leading '00', and
    the Saudi local mobile form '05XXXXXXXX' (→ +9665XXXXXXXX), which is how
    customers type their own number.
    """
    if not raw:
        return None
    s = re.sub(r"[\s\-().]", "", str(raw).strip())
    if not s:
        return None
    if re.fullmatch(r"05\d{8}", s):
        s = "+966" + s[1:]
    elif s.startswith("00"):
        s = "+" + s[2:]
    elif not s.startswith("+"):
        s = "+" + s
    if not re.fullmatch(r"\+\d{6,20}", s):
        return None
    return s


_TENANT_CACHE_TTL = 300.0  # 5 minutes
_tenant_cache: dict = {}   # normalized_phone -> {"owner_id": str, "ts": float}


async def resolve_owner(caller_phone: Optional[str]) -> str:
    """Resolve a WhatsApp phone number to the owning demo tenant's owner_id.

    Falls back to DEFAULT_OWNER_ID when caller_phone is missing or not
    registered. The fallback is deliberate: a demo that silently lands in the
    shared default tenant is recoverable; a hard 400 mid-demo is not.
    """
    normalized = normalize_phone(caller_phone)
    if not normalized:
        return DEFAULT_OWNER_ID
    cached = _tenant_cache.get(normalized)
    if cached and (_monotonic() - cached["ts"]) <= _TENANT_CACHE_TTL:
        return cached["owner_id"]
    # demo_users is NOT a per-tenant table — it's the tenant registry itself.
    rows = await _sb_raw_get("demo_users", {
        "whatsapp_number": f"eq.{normalized}", "select": "owner_id", "limit": "1",
    })
    owner = rows[0]["owner_id"] if rows else DEFAULT_OWNER_ID
    _tenant_cache[normalized] = {"owner_id": owner, "ts": _monotonic()}
    return owner


def _tenant_cache_stats() -> dict:
    now = _monotonic()
    return {
        "entries": len(_tenant_cache),
        "oldest_age_seconds": (
            round(now - min(v["ts"] for v in _tenant_cache.values()), 1) if _tenant_cache else None
        ),
    }


# ============================================================
# Supabase REST helpers (tenant-aware)
# ============================================================

AUTH_FAIL_CODES = (401, 403)


async def _sb_request(method: str, table: str, *, params=None, json_body=None, extra_headers=None):
    """Execute one PostgREST call with a valid token, retrying once on 401/403.

    In Mode B a 401 usually means an expired/evicted token — re-auth and
    retry. In Mode A there is nothing to refresh, so the retry is a no-op and
    the second failure surfaces as the real configuration error it is.
    """
    url = f"{SUPABASE_REST_URL}/{table}"
    for attempt in (1, 2):
        headers = await sb_headers(extra_headers)
        r = await http_client.request(method, url, headers=headers, params=params or {}, json=json_body)
        if r.status_code in AUTH_FAIL_CODES and attempt == 1 and AUTH_MODE == "service_account":
            print(f"[auth] {r.status_code} on {method} {table} — re-authenticating and retrying")
            await ensure_token(force=True)
            continue
        r.raise_for_status()
        if not r.content:
            return None
        return r.json()


def _upstream_auth_error(table: str) -> HTTPException:
    return HTTPException(status_code=502, detail={
        "code": "upstream_auth_failed",
        "message": f"Database authentication failed — the API cannot read '{table}'",
    })


async def _sb_raw_get(table: str, params: Optional[dict] = None) -> list:
    """Unscoped GET. ONLY for non-tenant tables (demo_users, catalogs) and
    for the token-scoped public routes, which resolve the owner from the row.

    Auth failures are NOT swallowed — an empty list is a legitimate answer, a
    401 never is. Other upstream errors raise 502 rather than pretending the
    table is empty (a 400 from a bad filter must not read as "not found").
    """
    try:
        return await _sb_request("GET", table, params=params or {}) or []
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        print(f"sb_get error {table}: {code} {e.response.text[:300]}")
        if code in AUTH_FAIL_CODES:
            raise _upstream_auth_error(table)
        raise HTTPException(status_code=502, detail={
            "code": "upstream_error", "message": f"Read from {table} failed ({code})"})
    except HTTPException:
        raise
    except Exception as e:
        print(f"sb_get exception {table}: {e}")
        raise HTTPException(status_code=502, detail={
            "code": "upstream_unreachable", "message": f"Database unreachable reading {table}"})


def _scope_params(table: str, params: Optional[dict], owner: Optional[str]) -> dict:
    """Inject owner_id filter for per-tenant tables. Raises loudly if a
    per-tenant table is queried without an owner."""
    p = dict(params or {})
    if table in TENANT_TABLES:
        if not owner:
            raise HTTPException(status_code=500, detail={
                "code": "internal_error",
                "message": f"Internal error: query on per-tenant table '{table}' missing owner scope"})
        p["owner_id"] = f"eq.{owner}"
    return p


async def sb_get(table: str, params: Optional[dict] = None, owner: Optional[str] = None) -> list:
    return await _sb_raw_get(table, _scope_params(table, params, owner))


async def sb_get_one(table: str, params: Optional[dict] = None, owner: Optional[str] = None) -> Optional[dict]:
    p = dict(params or {})
    p.setdefault("limit", "1")
    rows = await sb_get(table, p, owner=owner)
    return rows[0] if rows else None


def _with_owner(table: str, payload, owner: Optional[str]):
    if table not in TENANT_TABLES:
        return payload
    if not owner:
        raise HTTPException(status_code=500, detail={
            "code": "internal_error",
            "message": f"Internal error: insert into per-tenant table '{table}' missing owner scope"})
    if isinstance(payload, list):
        return [{**row, "owner_id": owner} for row in payload]
    return {**payload, "owner_id": owner}


async def sb_insert(table: str, payload, owner: Optional[str] = None) -> Any:
    """INSERT, injecting owner_id for per-tenant tables. Dict or list."""
    body = _with_owner(table, payload, owner)
    try:
        return await _sb_request("POST", table, json_body=body)
    except httpx.HTTPStatusError as e:
        print(f"sb_insert error {table}: {e.response.status_code} {e.response.text[:300]}")
        if e.response.status_code in AUTH_FAIL_CODES:
            raise _upstream_auth_error(table)
        raise HTTPException(status_code=502, detail={
            "code": "upstream_error", "message": f"Insert to {table} failed: {e.response.text[:200]}"})


async def sb_update(table: str, params: dict, payload: dict, owner: Optional[str] = None) -> Any:
    """UPDATE rows matching params, scoped to the tenant."""
    scoped = _scope_params(table, params, owner)
    try:
        return await _sb_request("PATCH", table, params=scoped, json_body=payload)
    except httpx.HTTPStatusError as e:
        print(f"sb_update error {table}: {e.response.status_code} {e.response.text[:300]}")
        if e.response.status_code in AUTH_FAIL_CODES:
            raise _upstream_auth_error(table)
        raise HTTPException(status_code=502, detail={
            "code": "upstream_error", "message": f"Update {table} failed: {e.response.text[:200]}"})


async def sb_delete(table: str, params: dict, owner: Optional[str] = None) -> Any:
    """DELETE rows matching params, scoped to the tenant."""
    scoped = _scope_params(table, params, owner)
    try:
        return await _sb_request("DELETE", table, params=scoped)
    except httpx.HTTPStatusError as e:
        print(f"sb_delete error {table}: {e.response.status_code} {e.response.text[:300]}")
        raise HTTPException(status_code=502, detail={
            "code": "upstream_error", "message": f"Delete from {table} failed"})


# ---- Tenant-scoped ID generation (playbook §4) ---------------------------
# New business IDs are max(existing for THIS owner)+1 within the prefix. Each
# tenant counts independently — CLM-88201 in Alice's demo and CLM-88201 in
# Bob's are different claims, by design. Two concurrent writes can compute the
# same next id; the composite PK rejects the second with 409 and we retry.

_ID_BASES = {  # first id = base + 1 when the tenant has none yet
    "CLM-": 88200, "INV-": 44700, "CMP-": 20900, "RET-": 40000, "PAY-": 50000,
    "TAX-": 70000, "ALR-": 30000, "RCV-": 20000, "EVT-": 100000,
}


def _id_number(value: str, prefix: str) -> Optional[int]:
    if not value or not value.startswith(prefix):
        return None
    try:
        return int(value[len(prefix):])
    except ValueError:
        return None


async def _next_id(table: str, id_col: str, prefix: str, width: int, owner: str) -> str:
    rows = await sb_get(table, {"select": id_col, id_col: f"like.{prefix}*"}, owner=owner)
    nums = [n for n in (_id_number(r.get(id_col), prefix) for r in rows) if n is not None]
    n = max(nums) if nums else _ID_BASES.get(prefix, 0)
    return f"{prefix}{n + 1:0{width}d}"


async def insert_with_next_id(table: str, id_col: str, prefix: str, width: int,
                              row: dict, owner: str) -> dict:
    """Insert `row` under the next tenant-scoped id, retrying on a PK race."""
    last_err = None
    for _ in range(4):
        new_id = await _next_id(table, id_col, prefix, width, owner)
        body = _with_owner(table, {**row, id_col: new_id}, owner)
        try:
            res = await _sb_request("POST", table, json_body=body)
            return (res[0] if isinstance(res, list) and res else {**row, id_col: new_id})
        except httpx.HTTPStatusError as e:
            last_err = e
            if e.response.status_code == 409:
                continue  # someone took this id between read and write
            print(f"insert {table} failed: {e.response.status_code} {e.response.text[:300]}")
            raise HTTPException(status_code=502, detail={
                "code": "upstream_error", "message": f"Insert to {table} failed: {e.response.text[:200]}"})
    raise HTTPException(status_code=502, detail={
        "code": "upstream_error", "message": f"Could not allocate an id in {table}: {last_err}"})


async def add_events(events: list, owner: str) -> list:
    """Append shipment_events rows with consecutive tenant-scoped EVT ids.
    One id read + one batch insert, so a 3-shipment consolidation doesn't
    race itself."""
    if not events:
        return []
    for _ in range(4):
        base_id = await _next_id("shipment_events", "event_id", "EVT-", 6, owner)
        n0 = _id_number(base_id, "EVT-")
        now_iso = _now().isoformat()
        rows = []
        for i, ev in enumerate(events):
            rows.append({
                "event_id": f"EVT-{n0 + i:06d}",
                "event_at": ev.get("event_at") or now_iso,
                "source": ev.get("source") or "Agent",
                **{k: v for k, v in ev.items() if k not in ("event_at", "source")},
            })
        try:
            await _sb_request("POST", "shipment_events", json_body=_with_owner("shipment_events", rows, owner))
            return rows
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                continue
            print(f"add_events failed: {e.response.status_code} {e.response.text[:300]}")
            return []  # the timeline is secondary; never fail the parent write
    return []


async def log_agent_action(
    customer_id: Optional[str],
    tracking_number: Optional[str],
    action_type: str,
    description: str,
    metadata: Optional[dict] = None,
    owner: Optional[str] = None,
    status: str = "Success",
    source: str = "Agent",
):
    """Insert into agent_actions for the portal's Live Activity Drawer.
    `description` is the human line staff read. Audit failures never break
    the parent operation."""
    try:
        await sb_insert("agent_actions", {
            "customer_id": customer_id,
            "tracking_number": tracking_number,
            "action_type": action_type,
            "description": description,
            "metadata": metadata or {},
            "status": status,
            "source": source,
        }, owner=owner)
    except Exception as e:
        print(f"agent_actions log failed: {e}")


# ============================================================
# In-process cache for SHARED reference tables (60 s)
# ============================================================
# Shared catalogs are identical for every tenant and change only on a
# migration, so one global cache serves everyone. Tenant resets don't touch
# them — nothing to invalidate.

_REF_CACHE_TTL = 60.0
_REF_TABLES = {
    "districts": "district_id.asc",
    "centres": "centre_id.asc",
    "pickup_points": "pickup_point_id.asc",
    "stores": "store_id.asc",
    "drivers": "driver_id.asc",
    "business_rules": "rule_key.asc",
    "delivery_windows": "window_code.asc",
}
_reference_cache: dict = {t: {"data": None, "ts": 0.0} for t in _REF_TABLES}
_ref_locks: dict = {}


async def get_ref(table: str) -> list:
    """Return a shared catalog from cache or fetch+cache it. A per-table lock
    stops a cold-cache burst from firing N identical queries."""
    c = _reference_cache[table]
    if c["data"] is not None and (_monotonic() - c["ts"]) <= _REF_CACHE_TTL:
        return c["data"]
    lock = _ref_locks.setdefault(table, asyncio.Lock())
    async with lock:
        c = _reference_cache[table]
        if c["data"] is not None and (_monotonic() - c["ts"]) <= _REF_CACHE_TTL:
            return c["data"]
        rows = await sb_get(table, {"select": "*", "order": _REF_TABLES[table]})
        c["data"], c["ts"] = rows, _monotonic()
        return rows


async def get_refs() -> dict:
    """All shared catalogs, indexed. One gather; free on a warm cache."""
    names = list(_REF_TABLES)
    results = await asyncio.gather(*(get_ref(t) for t in names))
    data = dict(zip(names, results))
    return {
        "districts": {d["district_id"]: d for d in data["districts"]},
        "centres": {c["centre_id"]: c for c in data["centres"]},
        "pickup_points": {p["pickup_point_id"]: p for p in data["pickup_points"]},
        "stores": {s["store_id"]: s for s in data["stores"]},
        "drivers": {d["driver_id"]: d for d in data["drivers"]},
        "rules": rules_dict(data["business_rules"]),
        "windows": {w["window_code"]: w for w in data["delivery_windows"]},
    }


def _cache_stats() -> dict:
    now = _monotonic()
    return {
        t: {
            "warm": e["data"] is not None,
            "row_count": len(e["data"]) if e["data"] is not None else 0,
            "age_seconds": round(now - e["ts"], 1) if e["ts"] else None,
        }
        for t, e in _reference_cache.items()
    }


def _num(v) -> Any:
    """PostgREST numerics → int when integral, else float. None passes."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return v
    return int(f) if f.is_integer() else f


def money(v) -> float:
    try:
        return round(float(v or 0) + 1e-9, 2)
    except (TypeError, ValueError):
        return 0.0


def rules_dict(rows: list) -> dict:
    """business_rules rows → flat {rule_key: value}. The SI quotes numbers
    ONLY from this dict."""
    out = {}
    for r in rows or []:
        v = r.get("value_num")
        out[r["rule_key"]] = _num(v) if v is not None else r.get("value_text")
    return out


# Fallbacks if a rule row is missing — identical to SPEC §2.2, so a partial
# seed degrades to spec behaviour rather than to a crash.
_RULE_DEFAULTS = {
    "damage_claim_window_days": 7, "default_return_window_days": 14, "hold_max_days": 30,
    "hold_storage_fee_after_days": 30, "locker_hold_hours": 48,
    "delivery_code_value_threshold_sar": 2000, "customs_duty_free_threshold_sar": 1000,
    "customs_vat_percent": 15, "customs_clearance_fee_sar": 15, "complaint_sla_working_days": 5,
    "investigation_callback_hours": 48, "store_legal_delivery_days": 15,
    "redirect_same_city_fee_sar": 0, "redirect_other_city_fee_sar": 25,
    "max_attempts_before_branch": 2, "return_to_sender_after_days": 14,
    "tga_phone": "19929", "moc_phone": "1900", "barq_care_phone": "920012345",
    "working_days": "Sun-Thu", "delivery_days": "Sat-Thu",
}


def rule(rules: dict, key: str):
    v = rules.get(key)
    return v if v is not None else _RULE_DEFAULTS.get(key)


# ============================================================
# Time, calendar and bilingual labels
# ============================================================
# Every human-facing date is produced HERE, in both languages. The model
# never computes a weekday.

def _now() -> datetime:
    return datetime.now(TZ)


def _today() -> date:
    return _now().date()


_WEEKDAYS_EN = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_WEEKDAYS_AR = ["الإثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت", "الأحد"]
_MONTHS_EN = ["January", "February", "March", "April", "May", "June", "July", "August",
              "September", "October", "November", "December"]
_MONTHS_AR = ["يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو", "يوليو", "أغسطس",
              "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر"]


def parse_date(v) -> Optional[date]:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.astimezone(TZ).date() if v.tzinfo else v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    try:
        if len(s) > 10:
            return parse_dt(s).date()
        return date.fromisoformat(s[:10])
    except (ValueError, AttributeError):
        return None


def parse_dt(v) -> Optional[datetime]:
    """Parse a timestamptz from PostgREST into an aware datetime in TZ."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip().replace(" ", "T", 1)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # PostgREST can emit "+00" (no minutes); fromisoformat wants "+00:00".
        s = re.sub(r"([+-]\d{2})$", r"\1:00", s)
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)


def date_label_en(d: Optional[date], with_year: Optional[bool] = None) -> Optional[str]:
    """'Sunday, 4 October' (year appended when not the current year)."""
    if not d:
        return None
    if with_year is None:
        with_year = d.year != _today().year
    s = f"{_WEEKDAYS_EN[d.weekday()]}, {d.day} {_MONTHS_EN[d.month - 1]}"
    return f"{s} {d.year}" if with_year else s


def date_label_ar(d: Optional[date], with_year: Optional[bool] = None) -> Optional[str]:
    """'الأحد 4 أكتوبر' (year appended when not the current year)."""
    if not d:
        return None
    if with_year is None:
        with_year = d.year != _today().year
    s = f"{_WEEKDAYS_AR[d.weekday()]} {d.day} {_MONTHS_AR[d.month - 1]}"
    return f"{s} {d.year}" if with_year else s


def hhmm_to_min(v) -> Optional[int]:
    """'16:00' / '16:00:00' / time → minutes since midnight."""
    if v is None or v == "":
        return None
    if isinstance(v, dtime):
        return v.hour * 60 + v.minute
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})(?::\d{2})?\s*", str(v))
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if h > 24 or mi > 59:
        return None
    return h * 60 + mi


def min_to_hhmm(m: int) -> str:
    m = int(round(m)) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


def time_label_en(m: int) -> str:
    """'2:40pm', '4pm', '12pm'."""
    m = int(round(m)) % (24 * 60)
    h, mi = divmod(m, 60)
    suffix = "am" if h < 12 else "pm"
    h12 = h % 12 or 12
    return f"{h12}:{mi:02d}{suffix}" if mi else f"{h12}{suffix}"


def time_label_ar(m: int) -> str:
    """'2:40 م', '4 م' (ص = morning, م = afternoon/evening)."""
    m = int(round(m)) % (24 * 60)
    h, mi = divmod(m, 60)
    suffix = "ص" if h < 12 else "م"
    h12 = h % 12 or 12
    return f"{h12}:{mi:02d} {suffix}" if mi else f"{h12} {suffix}"


def range_label_en(a: int, b: int) -> str:
    """'4–7pm' when both sides share am/pm, else '11am–1pm'."""
    same = (a // 60 < 12) == (b // 60 < 12)
    left = time_label_en(a)
    if same:
        left = left[:-2]
    return f"{left}–{time_label_en(b)}"


def range_label_ar(a: int, b: int) -> str:
    return f"{time_label_ar(a)} – {time_label_ar(b)}"


def dt_label_en(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    dt = dt.astimezone(TZ)
    return f"{date_label_en(dt.date())}, {time_label_en(dt.hour * 60 + dt.minute)}"


def dt_label_ar(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    dt = dt.astimezone(TZ)
    return f"{date_label_ar(dt.date())}، {time_label_ar(dt.hour * 60 + dt.minute)}"


def labels(prefix: str, d: Optional[date]) -> dict:
    """{prefix: iso, prefix_label_en, prefix_label_ar} for a date."""
    return {prefix: d.isoformat() if d else None,
            f"{prefix}_label_en": date_label_en(d), f"{prefix}_label_ar": date_label_ar(d)}


# ---- Delivery / working day calendars -------------------------------------
# Python weekday(): Mon=0 … Thu=3, Fri=4, Sat=5, Sun=6.
# Delivery days Sat–Thu (skip Friday). Working days Sun–Thu (skip Fri+Sat).

def is_delivery_day(d: date) -> bool:
    return d.weekday() != 4


def is_working_day(d: date) -> bool:
    return d.weekday() not in (4, 5)


def next_delivery_day(d: date) -> date:
    """First delivery day strictly after d."""
    n = d + timedelta(days=1)
    while not is_delivery_day(n):
        n += timedelta(days=1)
    return n


def add_delivery_days(d: date, n: int) -> date:
    for _ in range(n):
        d = next_delivery_day(d)
    return d


def add_working_days(d: date, n: int) -> date:
    """d + n working days (Sun–Thu). Day 0 is not counted, so a complaint
    opened on Thursday with a 5-day SLA is due the following Thursday."""
    count = 0
    cur = d
    while count < n:
        cur += timedelta(days=1)
        if is_working_day(cur):
            count += 1
    return cur


# ============================================================
# Geometry: haversine, districts, short addresses, snapping
# ============================================================

def haversine_m(lat1, lng1, lat2, lng2) -> float:
    """Great-circle distance in metres."""
    r = 6371000.0
    p1, p2 = math.radians(float(lat1)), math.radians(float(lat2))
    dp = p2 - p1
    dl = math.radians(float(lng2) - float(lng1))
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def distance_labels(m: float) -> dict:
    m = float(m)
    if m < 1000:
        v = int(round(m / 10.0) * 10)
        return {"distance_m": int(round(m)), "distance_label_en": f"{v}m", "distance_label_ar": f"{v} م"}
    km = round(m / 1000.0, 1)
    km_s = f"{km:g}"
    return {"distance_m": int(round(m)), "distance_label_en": f"{km_s}km", "distance_label_ar": f"{km_s} كم"}


def short_address_for(prefix: str, lat: float, lng: float) -> str:
    """Deterministic Saudi-style short address: 4-letter district prefix + 4
    digits from a stable hash of the pin rounded to 4 dp (~11 m). Same pin →
    same code on every call, so the agent can quote it back and the confirm
    step matches."""
    key = f"{round(float(lat), 4):.4f},{round(float(lng), 4):.4f}"
    digits = int(hashlib.sha256(key.encode()).hexdigest(), 16) % 10000
    return f"{(prefix or 'BRQX')[:4].upper()}{digits:04d}"


def city_centroids(districts: dict) -> dict:
    """city_en → (lat, lng), the mean of that city's district centroids."""
    acc: dict = {}
    for d in districts.values():
        if d.get("centroid_lat") is None or d.get("centroid_lng") is None:
            continue
        acc.setdefault(d["city_en"], []).append((float(d["centroid_lat"]), float(d["centroid_lng"])))
    return {c: (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)) for c, pts in acc.items()}


SNAP_THRESHOLD_M = 60000.0


def needs_snap(lat: float, lng: float, districts: dict) -> bool:
    """True when the pin is > 60 km from every city in the reference data."""
    cents = city_centroids(districts)
    if not cents:
        return False
    return min(haversine_m(lat, lng, c[0], c[1]) for c in cents.values()) > SNAP_THRESHOLD_M


def nearest_district(lat: float, lng: float, districts: dict) -> tuple:
    best, best_d = None, None
    for d in districts.values():
        if d.get("centroid_lat") is None:
            continue
        dist = haversine_m(lat, lng, d["centroid_lat"], d["centroid_lng"])
        if best_d is None or dist < best_d:
            best, best_d = d, dist
    return best, best_d


_AR_DIACRITICS = re.compile(r"[ً-ٰٟـ]")


def _norm_text(s: str) -> str:
    """Normalise EN/AR place & store names for fuzzy matching: lowercase,
    strip diacritics/tatweel, unify alef/ya/ta-marbuta, drop 'al-'/'ال' and
    'حي'/'district'."""
    s = unicodedata.normalize("NFKC", str(s or "")).lower()
    s = _AR_DIACRITICS.sub("", s)
    s = re.sub("[إأآٱ]", "ا", s).replace("ى", "ي").replace("ة", "ه").replace("ؤ", "و").replace("ئ", "ي")
    s = re.sub(r"[\-_'’`.,،/()]", " ", s)
    s = re.sub(r"\b(district|neighbourhood|neighborhood|dist|the)\b", " ", s)
    s = re.sub(r"(^|\s)حي(\s|$)", " ", s)
    s = re.sub(r"\bal\s+", "", s)
    s = re.sub(r"\bel\s+", "", s)
    s = re.sub(r"(^|\s)ال", r"\1", s)
    return re.sub(r"\s+", " ", s).strip()


def _fuzzy_score(needle: str, hay: str) -> float:
    """0..1: containment wins outright, else SequenceMatcher ratio on the best
    same-length window of the haystack (handles 'deliver to al malqa please')."""
    n, h = _norm_text(needle), _norm_text(hay)
    if not n or not h:
        return 0.0
    if n == h:
        return 1.0
    if len(n) >= 3 and (f" {n} " in f" {h} " or f" {h} " in f" {n} "):
        return 0.95
    best = difflib.SequenceMatcher(None, n, h).ratio()
    words = h.split()
    k = len(n.split())
    for i in range(len(words)):
        chunk = " ".join(words[i:i + k])
        best = max(best, difflib.SequenceMatcher(None, n, chunk).ratio())
    return best


def match_district_text(text: str, districts: dict, prefer_city: Optional[str] = None) -> Optional[dict]:
    """Fuzzy-match free text to a district (EN/AR names). Al Rawdah exists in
    Riyadh AND Jeddah: a city named in the text wins, else the shipment's
    city, else the best score."""
    if not text:
        return None
    t_norm = _norm_text(text)
    scored = []
    for d in districts.values():
        s = max(_fuzzy_score(d.get("name_en") or "", text), _fuzzy_score(d.get("name_ar") or "", text),
                _fuzzy_score(text, d.get("name_en") or ""), _fuzzy_score(text, d.get("name_ar") or ""))
        if s < 0.8:
            continue
        city_hit = any(c and _norm_text(c) and _norm_text(c) in t_norm for c in (d.get("city_en"), d.get("city_ar")))
        pref = prefer_city and d.get("city_en") == prefer_city
        scored.append((s, city_hit, bool(pref), d))
    if not scored:
        return None
    scored.sort(key=lambda x: (round(x[0], 2), x[1], x[2]), reverse=True)
    top = round(scored[0][0], 2)
    tied = [x for x in scored if round(x[0], 2) == top]
    tied.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return tied[0][3]


# ============================================================
# Prayer times — inline solar algorithm (Umm al-Qura parameters)
# ============================================================
# Port of the well-known praytimes.org computation. Fajr at 18.5° below the
# horizon, Sunrise/Maghrib at 0.833° (refraction + solar radius), Dhuhr at
# solar noon, Asr at shadow factor 1 (standard / Shafi'i), Isha = Maghrib +
# 90 min (Umm al-Qura, outside Ramadan). Accurate to ~1 minute vs published
# Riyadh timetables — ample for "keep 5:30–6:30 free".

def _dsin(d): return math.sin(math.radians(d))
def _dcos(d): return math.cos(math.radians(d))
def _dtan(d): return math.tan(math.radians(d))
def _darcsin(x): return math.degrees(math.asin(max(-1.0, min(1.0, x))))
def _darccos(x): return math.degrees(math.acos(max(-1.0, min(1.0, x))))
def _darctan2(y, x): return math.degrees(math.atan2(y, x))
def _darccot(x): return math.degrees(math.atan(1.0 / x))
def _fix(a, b): a = a - b * math.floor(a / b); return a + b if a < 0 else a


def _julian(y, m, d) -> float:
    if m <= 2:
        y -= 1
        m += 12
    a = math.floor(y / 100)
    b = 2 - a + math.floor(a / 4)
    return math.floor(365.25 * (y + 4716)) + math.floor(30.6001 * (m + 1)) + d + b - 1524.5


def _sun_position(jd: float) -> tuple:
    dd = jd - 2451545.0
    g = _fix(357.529 + 0.98560028 * dd, 360)
    q = _fix(280.459 + 0.98564736 * dd, 360)
    L = _fix(q + 1.915 * _dsin(g) + 0.020 * _dsin(2 * g), 360)
    e = 23.439 - 0.00000036 * dd
    ra = _darctan2(_dcos(e) * _dsin(L), _dcos(L)) / 15.0
    eqt = q / 15.0 - _fix(ra, 24)
    decl = _darcsin(_dsin(e) * _dsin(L))
    return decl, eqt


def prayer_times_for(d: date, lat: float, lng: float, tz_name: str = None) -> dict:
    """Return {'fajr','sunrise','dhuhr','asr','maghrib','isha'} as minutes
    since local midnight for date d at (lat, lng)."""
    tzinfo = ZoneInfo(tz_name) if tz_name else TZ
    offset_h = datetime(d.year, d.month, d.day, 12, tzinfo=tzinfo).utcoffset().total_seconds() / 3600.0
    lat, lng = float(lat), float(lng)
    jd = _julian(d.year, d.month, d.day) - lng / (15 * 24.0)

    def mid_day(t):
        _, eqt = _sun_position(jd + t)
        return _fix(12 - eqt, 24)

    def sun_angle_time(angle, t, ccw=False):
        decl, _ = _sun_position(jd + t)
        noon = mid_day(t)
        x = (-_dsin(angle) - _dsin(decl) * _dsin(lat)) / (_dcos(decl) * _dcos(lat))
        tt = _darccos(x) / 15.0
        return noon - tt if ccw else noon + tt

    def asr_time(factor, t):
        decl, _ = _sun_position(jd + t)
        angle = -_darccot(factor + _dtan(abs(lat - decl)))
        return sun_angle_time(angle, t)

    # Initial guesses (hours) as day fractions, one refinement pass.
    t = {"fajr": 5, "sunrise": 6, "dhuhr": 12, "asr": 13, "maghrib": 18}
    t = {k: v / 24.0 for k, v in t.items()}
    raw = {
        "fajr": sun_angle_time(18.5, t["fajr"], ccw=True),
        "sunrise": sun_angle_time(0.833, t["sunrise"], ccw=True),
        "dhuhr": mid_day(t["dhuhr"]),
        "asr": asr_time(1, t["asr"]),
        "maghrib": sun_angle_time(0.833, t["maghrib"]),
    }
    adj = offset_h - lng / 15.0
    out = {k: (v + adj) * 60.0 for k, v in raw.items()}
    out["isha"] = out["maghrib"] + 90.0
    return {k: int(round(v)) for k, v in out.items()}


# Buffers around each prayer (minutes before, after). Maghrib's longer tail
# covers the short gap to the congregation + dispersal.
PRAYER_BUFFERS = {"dhuhr": (20, 20), "asr": (20, 20), "maghrib": (20, 40), "isha": (20, 20)}
PRAYER_NAMES_EN = {"fajr": "Fajr", "dhuhr": "Dhuhr", "asr": "Asr", "maghrib": "Maghrib", "isha": "Isha"}
PRAYER_NAMES_AR = {"fajr": "الفجر", "dhuhr": "الظهر", "asr": "العصر", "maghrib": "المغرب", "isha": "العشاء"}


def window_prayer_conflicts(start_min: int, end_min: int, prayers: dict) -> list:
    """Prayers whose buffered interval overlaps [start, end)."""
    hits = []
    for name, (before, after) in PRAYER_BUFFERS.items():
        p = prayers.get(name)
        if p is None:
            continue
        if (p - before) < end_min and (p + after) > start_min:
            hits.append(name)
    return hits


def prayer_row(d: date, lat: float, lng: float) -> dict:
    p = prayer_times_for(d, lat, lng)
    row = {"date": d.isoformat(), "date_label_en": date_label_en(d), "date_label_ar": date_label_ar(d)}
    for k in ("fajr", "sunrise", "dhuhr", "asr", "maghrib", "isha"):
        row[k] = min_to_hhmm(p[k])
        row[f"{k}_label_en"] = time_label_en(p[k])
        row[f"{k}_label_ar"] = time_label_ar(p[k])
    return row


# ============================================================
# Delivery windows + slot computation
# ============================================================

def window_bounds(w: dict) -> tuple:
    return hhmm_to_min(w.get("start_time")), hhmm_to_min(w.get("end_time"))


def window_labels(code: Optional[str], windows: dict) -> dict:
    """{window_label_en, window_label_ar} for a window code — from the
    delivery_windows table, computed if the label columns are empty."""
    w = windows.get(code) if code else None
    if not w:
        return {"window_label_en": None, "window_label_ar": None}
    a, b = window_bounds(w)
    return {
        "window_label_en": w.get("label_en") or (range_label_en(a, b) if a is not None else code),
        "window_label_ar": w.get("label_ar") or (range_label_ar(a, b) if a is not None else code),
    }


VALID_PRAYERS = {"dhuhr", "asr", "maghrib", "isha"}


def parse_avoid_prayers(v) -> list:
    """Accept a list or comma string of prayer names. The boolean form
    ('true'/'yes'/'1') means the evening prayer — Maghrib — deliberately: with
    3-hour windows every window touches SOME prayer, so 'avoid all prayers'
    would return nothing. Daytime prayers are handled by drivers pausing; the
    one customers ask about (L-07) is Maghrib."""
    if v is None or v == "" or v is False:
        return []
    if v is True:
        return ["maghrib"]
    items = v if isinstance(v, (list, tuple)) else re.split(r"[,\s]+", str(v))
    out = []
    for it in items:
        s = str(it).strip().lower()
        if not s:
            continue
        if s in ("true", "yes", "1", "on"):
            out.append("maghrib")
        elif s in ("false", "no", "0", "off"):
            continue
        elif s in ("all", "any"):
            out.extend(sorted(VALID_PRAYERS))
        elif s in VALID_PRAYERS:
            out.append(s)
        elif s == "zuhr" or s == "duhr":
            out.append("dhuhr")
        elif s == "ishaa":
            out.append("isha")
        else:
            raise bad_request("invalid_prayer", f"Unknown prayer '{it}'. Use dhuhr, asr, maghrib or isha.")
    return sorted(set(out), key=["dhuhr", "asr", "maghrib", "isha"].index)


def slot_filter_reason(w: dict, day_prayers: dict, avoid: list, not_before: Optional[int],
                       not_after: Optional[int], earliest: Optional[int]) -> tuple:
    """Return (conflicting_prayers, excluded_reason or None) for a window."""
    a, b = window_bounds(w)
    conflicts = window_prayer_conflicts(a, b, day_prayers)
    if avoid and any(c in avoid for c in conflicts):
        return conflicts, "prayer"
    if not_before is not None and a < not_before:
        return conflicts, "not_before"
    if not_after is not None and b > not_after:
        return conflicts, "not_after"
    if earliest is not None and a < earliest:
        return conflicts, "customer_earliest_time"
    return conflicts, None


def compute_slots(from_date: date, n_days: int, windows: dict, lat: float, lng: float,
                  avoid: list, not_before: Optional[int], not_after: Optional[int],
                  earliest: Optional[int]) -> tuple:
    """Next n delivery days × windows. Returns (slots, excluded, prayer_rows)."""
    slots, excluded, prayer_rows = [], [], []
    d = from_date
    while not is_delivery_day(d):
        d += timedelta(days=1)
    ordered = sorted(windows.values(), key=lambda w: window_bounds(w)[0] or 0)
    for _ in range(n_days):
        p = prayer_times_for(d, lat, lng)
        prayer_rows.append(prayer_row(d, lat, lng))
        for w in ordered:
            conflicts, reason = slot_filter_reason(w, p, avoid, not_before, not_after, earliest)
            item = {
                "date": d.isoformat(),
                "date_label_en": date_label_en(d),
                "date_label_ar": date_label_ar(d),
                "window_code": w["window_code"],
                **window_labels(w["window_code"], windows),
                "conflicts_prayer": bool(conflicts),
                "prayer_name": conflicts[0] if conflicts else None,
                "conflicting_prayers": conflicts,
            }
            if conflicts:
                item["prayer_time_label_en"] = time_label_en(p[conflicts[0]])
                item["prayer_time_label_ar"] = time_label_ar(p[conflicts[0]])
            if reason:
                excluded.append({**item, "excluded_reason": reason})
            else:
                slots.append(item)
        d = next_delivery_day(d)
    return slots, excluded, prayer_rows


def pick_next_slot(from_date: date, windows: dict, lat: float, lng: float, preferred: Optional[str],
                   avoid: list, not_before, not_after, earliest, max_days: int = 14) -> Optional[tuple]:
    """First acceptable (date, window_code) on/after from_date. On each day the
    preferred window (customer preference, else W3 'after work') is tried
    first, then later windows, then earlier ones."""
    ordered = sorted(windows.values(), key=lambda w: window_bounds(w)[0] or 0)
    codes = [w["window_code"] for w in ordered]
    pref = preferred if preferred in windows else ("W3" if "W3" in windows else (codes[0] if codes else None))
    if pref is None:
        return None
    i = codes.index(pref)
    order = [pref] + codes[i + 1:] + list(reversed(codes[:i]))
    d = from_date
    for _ in range(max_days):
        if is_delivery_day(d):
            p = prayer_times_for(d, lat, lng)
            for code in order:
                _, reason = slot_filter_reason(windows[code], p, avoid, not_before, not_after, earliest)
                if not reason:
                    return d, code
        d += timedelta(days=1)
    return None


# ============================================================
# Live ETA (Out for Delivery)
# ============================================================
# Stored eta_start/eta_end would go stale the moment the date-shift clone
# runs, so the ETA is computed from route_stop: the route starts at 12:00,
# 25 min per stop, ±20 min. If the clock has already passed that estimate
# (a 5pm demo), the driver is assumed to be working through the stops before
# yours and the estimate slides forward — it never shows a time in the past.

ROUTE_START_MIN = 12 * 60
MIN_PER_STOP = 25
ETA_SPREAD = 20


def _round5(m: float) -> int:
    return int(5 * round(m / 5.0))


def compute_live_eta(route_stop: Optional[int], route_total: Optional[int], now: datetime) -> Optional[dict]:
    if not route_stop:
        return None
    route_stop = int(route_stop)
    now_min = now.hour * 60 + now.minute
    completed = 0
    if now_min > ROUTE_START_MIN:
        completed = min(route_stop - 1, int((now_min - ROUTE_START_MIN) // MIN_PER_STOP))
    stops_before = max(0, route_stop - 1 - completed)
    base_centre = ROUTE_START_MIN + MIN_PER_STOP * route_stop
    live_centre = now_min + MIN_PER_STOP * (stops_before + 1)
    centre = max(base_centre, live_centre) if now_min > base_centre - ETA_SPREAD - 10 else base_centre
    start, end = _round5(centre - ETA_SPREAD), _round5(centre + ETA_SPREAD)
    return {
        "eta_start": min_to_hhmm(start),
        "eta_end": min_to_hhmm(end),
        "eta_label_en": f"{time_label_en(start)}–{time_label_en(end)}",
        "eta_label_ar": f"{time_label_ar(start)} – {time_label_ar(end)}",
        "route_stop": route_stop,
        "route_total": int(route_total) if route_total else None,
        "stops_before_you": stops_before,
        "is_live": True,
    }


def driver_progress_fraction(route_stop: Optional[int], now: datetime) -> float:
    """0 (at the centre) … 0.95 (almost at your door), by time of day.
    Floors at 0.1: an Out-for-Delivery van is visibly out."""
    eta = compute_live_eta(route_stop or 1, None, now)
    centre = (hhmm_to_min(eta["eta_start"]) + hhmm_to_min(eta["eta_end"])) / 2.0
    now_min = now.hour * 60 + now.minute
    span = max(1.0, centre - ROUTE_START_MIN)
    f = (now_min - ROUTE_START_MIN) / span
    return round(max(0.1, min(0.95, f)), 3)


# ============================================================
# Validation helpers
# ============================================================

def validate_national_id(v: Optional[str]) -> str:
    """10 digits starting 1 (Saudi) or 2 (Iqama)."""
    s = re.sub(r"\s", "", str(v or ""))
    if not re.fullmatch(r"[12]\d{9}", s):
        raise bad_request("invalid_national_id",
                          "The ID number must be 10 digits starting with 1 (Saudi ID) or 2 (Iqama).")
    return s


def is_valid_vat(v: Optional[str]) -> bool:
    """ZATCA VAT registration number: 15 digits, starts and ends with 3."""
    s = re.sub(r"\s", "", str(v or ""))
    return bool(re.fullmatch(r"3\d{13}3", s))


def vat_split(total_incl: float, vat_percent: float = 15.0) -> tuple:
    """VAT-inclusive total → (subtotal, vat) using the 15/115 split, with the
    VAT taking the rounding remainder so subtotal + vat == total exactly."""
    total = money(total_incl)
    sub = round(total * 100.0 / (100.0 + vat_percent) + 1e-9, 2)
    return sub, round(total - sub, 2)


# ============================================================
# Response pruning
# ============================================================
# Same principle as healthcare v3.5: the workhorse opens every conversation
# and is re-sent to the model on every tool-loop iteration. Tenancy plumbing
# and audit timestamps never leave; secrets (delivery codes, tracking tokens)
# never reach the model at all.

_NOISE_KEYS = ("owner_id", "created_at", "updated_at")
_SHIPMENT_SECRET_KEYS = ("tracking_token", "delivery_code", "pod", "customs", "eta_start", "eta_end")


def _strip(rows, *extra_keys):
    drop = set(_NOISE_KEYS) | set(extra_keys)
    return [{k: v for k, v in r.items() if k not in drop} for r in (rows or [])]


def _strip_one(row, *extra_keys):
    if not row:
        return row
    drop = set(_NOISE_KEYS) | set(extra_keys)
    return {k: v for k, v in row.items() if k not in drop}


# ============================================================
# Storage helpers (documents bucket)
# ============================================================

_signed_cache: dict = {}  # path -> (url, monotonic_expiry)


def _storage_path(path: str) -> str:
    """Accept 'demo-assets/…' or 'documents/demo-assets/…'."""
    p = (path or "").lstrip("/")
    if p.startswith(f"{DOCS_BUCKET}/"):
        p = p[len(DOCS_BUCKET) + 1:]
    return p


async def _storage_request(method: str, url: str, *, content=None, json_body=None, headers=None):
    for attempt in (1, 2):
        token = await ensure_token()
        h = {"apikey": _apikey(), "Authorization": f"Bearer {token}"}
        if headers:
            h.update(headers)
        r = await http_client.request(method, url, headers=h, content=content, json=json_body)
        if r.status_code in AUTH_FAIL_CODES and attempt == 1 and AUTH_MODE == "service_account":
            await ensure_token(force=True)
            continue
        return r


async def storage_upload(path: str, data: bytes, content_type: str, upsert: bool = True) -> bool:
    url = f"{SUPABASE_STORAGE_URL}/object/{DOCS_BUCKET}/{_storage_path(path)}"
    r = await _storage_request("POST", url, content=data, headers={
        "Content-Type": content_type or "application/octet-stream",
        "x-upsert": "true" if upsert else "false",
        "cache-control": "no-cache",
    })
    if r.status_code >= 300:
        print(f"[storage] upload {path} failed: {r.status_code} {r.text[:200]}")
        return False
    return True


def _absolute_signed(signed: str) -> str:
    if signed.startswith("http"):
        return signed
    if signed.startswith("/storage/v1/"):
        base = SUPABASE_STORAGE_URL[: -len("/storage/v1")] if SUPABASE_STORAGE_URL.endswith("/storage/v1") else SUPABASE_URL
        return f"{base}{signed}"
    return f"{SUPABASE_STORAGE_URL}{signed if signed.startswith('/') else '/' + signed}"


async def storage_sign(path: str, expires_in: int = 3600, use_cache: bool = True) -> Optional[str]:
    """Signed URL for a private object, or None if it doesn't exist. Cached
    for most of its life so the workhorse doesn't re-sign POD photos on every
    call."""
    p = _storage_path(path)
    if not p:
        return None
    if use_cache:
        hit = _signed_cache.get((p, expires_in))
        if hit and hit[1] > _monotonic():
            return hit[0]
    url = f"{SUPABASE_STORAGE_URL}/object/sign/{DOCS_BUCKET}/{p}"
    try:
        r = await _storage_request("POST", url, json_body={"expiresIn": expires_in},
                                   headers={"Content-Type": "application/json"})
    except Exception as e:
        print(f"[storage] sign {p} error: {e}")
        return None
    if r.status_code >= 300:
        print(f"[storage] sign {p} failed: {r.status_code} {r.text[:150]}")
        return None
    body = r.json() or {}
    signed = body.get("signedURL") or body.get("signedUrl")
    if not signed:
        return None
    full = _absolute_signed(signed)
    if use_cache:
        _signed_cache[(p, expires_in)] = (full, _monotonic() + expires_in * 0.8)
    return full


async def storage_download(path: str) -> Optional[bytes]:
    url = f"{SUPABASE_STORAGE_URL}/object/{DOCS_BUCKET}/{_storage_path(path)}"
    try:
        r = await _storage_request("GET", url)
    except Exception as e:
        print(f"[storage] download {path} error: {e}")
        return None
    if r.status_code >= 300:
        return None
    return r.content


# ---- Customer media (attachment_urls) --------------------------------------
# Photos/PDFs the customer sends arrive as URLs from the Nebelus WhatsApp
# parser. Those URLs expire, so we copy them into our own bucket at write
# time. A failed fetch NEVER fails the write: the claim is what matters; the
# original URL is kept with stored=false so staff can still try it.

MAX_MEDIA_BYTES = 10 * 1024 * 1024
_EXT_BY_TYPE = {"image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png", "image/webp": "webp",
                "image/heic": "heic", "application/pdf": "pdf", "image/gif": "gif"}


async def _fetch_one_attachment(url: str, owner: str, entity: str) -> dict:
    out = {"source_url": url, "path": url, "stored": False, "content_type": None}
    try:
        async with http_client.stream("GET", url, timeout=20.0, follow_redirects=True) as r:
            if r.status_code >= 300:
                raise RuntimeError(f"HTTP {r.status_code}")
            ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
            buf = bytearray()
            async for chunk in r.aiter_bytes():
                buf.extend(chunk)
                if len(buf) > MAX_MEDIA_BYTES:
                    raise RuntimeError("file larger than 10 MB")
        if not ctype or ctype == "application/octet-stream":
            ctype = mimetypes.guess_type(url.split("?")[0])[0] or "application/octet-stream"
        ext = _EXT_BY_TYPE.get(ctype) or (url.split("?")[0].rsplit(".", 1)[-1][:5] if "." in url.split("?")[0][-6:] else "bin")
        path = f"attachments/{owner}/{entity}/{uuid.uuid4().hex}.{ext}"
        ok = await storage_upload(path, bytes(buf), ctype)
        out["content_type"] = ctype
        if ok:
            out.update({"path": path, "stored": True})
    except Exception as e:
        print(f"[media] fetch {url[:80]} failed: {e}")
        out["error"] = str(e)[:120]
    return out


async def store_attachments(urls, owner: str, entity: str) -> list:
    urls = [u for u in (urls or []) if isinstance(u, str) and u.strip()]
    if not urls:
        return []
    return list(await asyncio.gather(*(_fetch_one_attachment(u.strip(), owner, entity) for u in urls)))


def _as_list(v) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        return [s for s in re.split(r"[\s,]+", v) if s]
    return [v]


# ============================================================
# Shipment enrichment + eligibility
# ============================================================

MOVABLE_STATUSES = {"Created", "Picked Up", "In Transit", "At Sorting Centre", "On Hold",
                    "Delivery Attempted", "At Branch", "Held — Customer Request"}
OPEN_CASE_STATUSES = {"Open", "Under Review", "Awaiting Customer", "Escalated"}


def is_delivered(s: dict) -> bool:
    return bool(s.get("delivered_at")) or s.get("status") == "Delivered"


def is_active(s: dict) -> bool:
    return not is_delivered(s) and s.get("status") != "Returned to Sender"


def shipment_coords(s: dict, districts: dict) -> tuple:
    """Delivery point: stored pin, else the delivery district centroid, else
    Riyadh centre (last resort so prayer maths never gets None)."""
    if s.get("delivery_lat") is not None and s.get("delivery_lng") is not None:
        return float(s["delivery_lat"]), float(s["delivery_lng"])
    d = districts.get(s.get("delivery_district_id"))
    if d and d.get("centroid_lat") is not None:
        return float(d["centroid_lat"]), float(d["centroid_lng"])
    return 24.7136, 46.6753


def shipment_city(s: dict, districts: dict) -> Optional[str]:
    d = districts.get(s.get("delivery_district_id"))
    return d.get("city_en") if d else None


def delivered_date(s: dict) -> Optional[date]:
    dt = parse_dt(s.get("delivered_at"))
    return dt.date() if dt else None


def compute_eligibility(s: dict, store: Optional[dict], rules: dict, today: date,
                        open_return: bool = False) -> dict:
    """The rule engine the SI reads. Pure function — see tests/test_units.py."""
    st = s.get("status")
    delivered = is_delivered(s)
    movable = st in MOVABLE_STATUSES and not delivered
    active = is_active(s)
    dd = delivered_date(s)
    days_since = (today - dd).days if dd else None
    claim_window = int(rule(rules, "damage_claim_window_days"))
    ret_window = int((store or {}).get("return_window_days") or rule(rules, "default_return_window_days"))
    last_scan = parse_dt(s.get("last_scan_at"))
    days_since_scan = (today - last_scan.date()).days if last_scan else None
    out = {
        "can_reschedule": movable,
        "can_change_address": movable,
        "can_redirect_pickup": movable,
        "can_hold": movable,
        "can_consolidate": movable,
        "can_back_to_home_delivery": st == "At Branch",
        "can_add_driver_note": active,
        "can_set_time_rule": active,
        "can_add_receiver": active,
        "can_resend_delivery_code": bool(s.get("requires_delivery_code")) and active,
        "can_pay_online": s.get("cod_status") == "Pending" and money(s.get("cod_amount_sar")) > 0,
        "can_open_damage_claim": bool(delivered and days_since is not None and days_since <= claim_window),
        "damage_claim_window_days": claim_window,
        "days_since_delivery": days_since,
        "can_return": bool(delivered and days_since is not None and days_since <= ret_window and not open_return),
        "return_window_days": ret_window,
        "can_open_investigation": bool(delivered and not s.get("pod_locked")),
        "can_complain": True,
        "complaint_recommended": bool(active and days_since_scan is not None and days_since_scan >= 3),
        "days_since_last_scan": days_since_scan,
    }
    if dd:
        out.update(labels("damage_claim_deadline", dd + timedelta(days=claim_window)))
        out.update(labels("return_deadline", dd + timedelta(days=ret_window)))
    else:
        out.update({"damage_claim_deadline": None, "return_deadline": None})
    return out


def mask_customs(customs: Optional[dict]) -> Optional[dict]:
    """Customs blob for the model: amounts + requirements, the ID shown only
    as a mask of its last 4 (which is all we ever stored)."""
    if not customs:
        return None
    c = {k: v for k, v in customs.items() if k not in ("national_id_last4", "documents")}
    last4 = customs.get("national_id_last4")
    c["national_id_masked"] = f"******{last4}" if last4 else None
    for k in ("goods_value_sar", "shipping_sar", "duty_sar", "vat_sar", "clearance_fee_sar", "total_due_sar"):
        if k in c:
            c[k] = money(c[k])
    c["documents_received"] = len(customs.get("documents") or [])
    return c


def tracking_url_for(s: dict) -> Optional[str]:
    tok = s.get("tracking_token")
    return f"{PORTAL_BASE_URL}/track/{tok}" if tok and PORTAL_BASE_URL else None


def enrich_shipment(s: dict, refs: dict, now: datetime, *, receivers=None, cases=None,
                    events=None, open_return: bool = False, pod_photo_url: Optional[str] = None) -> dict:
    today = now.date()
    stores, centres, windows = refs["stores"], refs["centres"], refs["windows"]
    store = stores.get(s.get("store_id")) or {}
    centre = centres.get(s.get("current_centre_id")) or {}
    district = refs["districts"].get(s.get("delivery_district_id")) or {}
    sched = parse_date(s.get("scheduled_date"))
    eta_d = parse_date(s.get("eta_date"))
    last_scan = parse_dt(s.get("last_scan_at"))
    out = _strip_one(s, *_SHIPMENT_SECRET_KEYS)
    out.update({
        "store_name_en": store.get("name_en"),
        "store_name_ar": store.get("name_ar"),
        "store_return_window_days": store.get("return_window_days"),
        "centre_name_en": centre.get("name_en"),
        "centre_name_ar": centre.get("name_ar"),
        "delivery_district_name_en": district.get("name_en"),
        "delivery_district_name_ar": district.get("name_ar"),
        "delivery_city_en": district.get("city_en"),
        "delivery_city_ar": district.get("city_ar"),
        **labels("scheduled_date", sched),
        "scheduled_in_days": (sched - today).days if sched else None,
        **window_labels(s.get("scheduled_window"), windows),
        **labels("eta_date", eta_d),
        "last_scan_at_label_en": dt_label_en(last_scan),
        "last_scan_at_label_ar": dt_label_ar(last_scan),
        "days_since_last_scan": (today - last_scan.date()).days if last_scan else None,
        "cod_amount_sar": money(s.get("cod_amount_sar")),
        "declared_value_sar": money(s.get("declared_value_sar")),
        "shipping_fee_sar": money(s.get("shipping_fee_sar")),
        "customs": mask_customs(s.get("customs")),
        "tracking_url": tracking_url_for(s),
        "delivery_code_issued_at_label_en": dt_label_en(parse_dt(s.get("delivery_code_issued_at"))),
    })
    if s.get("return_to_sender_date"):
        rts = parse_date(s["return_to_sender_date"])
        out.update(labels("return_to_sender_date", rts))
        out["days_until_return_to_sender"] = (rts - today).days
    if s.get("hold_until"):
        out.update(labels("hold_until", parse_date(s["hold_until"])))
    # Live ETA only while the van is out; otherwise the scheduled date/window is the ETA.
    if s.get("status") == "Out for Delivery":
        out["eta"] = compute_live_eta(s.get("route_stop"), s.get("route_total"), now)
        drv = refs["drivers"].get(s.get("driver_id")) or {}
        out["driver"] = {"name_en": drv.get("name_en"), "name_ar": drv.get("name_ar"),
                         "vehicle_plate": drv.get("vehicle_plate")} if drv else None
    else:
        out["eta"] = None
    pp = refs["pickup_points"].get(s.get("pickup_point_id"))
    if pp:
        out["pickup_point"] = {
            "pickup_point_id": pp["pickup_point_id"], "type": pp.get("type"),
            "name_en": pp.get("name_en"), "name_ar": pp.get("name_ar"),
            "address_en": pp.get("address_en"), "address_ar": pp.get("address_ar"),
            "hours_en": pp.get("hours_en"), "hours_ar": pp.get("hours_ar"),
            "hold_hours": pp.get("hold_hours"),
        }
        if s.get("status") == "In Locker" or pp.get("type") == "Locker":
            out["pickup_point"]["locker_code"] = s.get("locker_code")
    else:
        out["pickup_point"] = None
    if s.get("status") not in ("In Locker",) and not pp:
        out.pop("locker_code", None)
    if is_delivered(s):
        pod = s.get("pod") or {}
        dt = parse_dt(s.get("delivered_at"))
        out.update({
            "delivered_at_label_en": dt_label_en(dt),
            "delivered_at_label_ar": dt_label_ar(dt),
            "pod": {
                "signed_by": pod.get("signed_by"),
                "method": pod.get("method"),
                "distance_from_address_m": pod.get("distance_from_address_m"),
                "gps_lat": pod.get("gps_lat"),
                "gps_lng": pod.get("gps_lng"),
                "has_photo": bool(pod.get("photo_path")),
                "photo_url": pod_photo_url,
            } if pod else None,
        })
    out["eligibility"] = compute_eligibility(s, store, refs["rules"], today, open_return=open_return)
    out["authorized_receivers"] = _strip(receivers or [], "owner_id")
    out["open_cases"] = [
        {k: c.get(k) for k in ("case_id", "case_type", "status", "due_at")}
        | labels("due", parse_date(c.get("due_at")))
        for c in (cases or []) if c.get("status") in OPEN_CASE_STATUSES
    ]
    # Timeline: the model needs "what happened last", not the full story —
    # 4 events for live parcels, 2 for delivered ones (the POD block covers the rest).
    out["recent_events"] = [
        {k: e.get(k) for k in ("status", "location_en", "location_ar", "note_en", "note_ar")}
        | {"event_at_label_en": dt_label_en(parse_dt(e.get("event_at"))),
           "event_at_label_ar": dt_label_ar(parse_dt(e.get("event_at")))}
        for e in (events or [])[:(2 if is_delivered(s) else 4)]
    ]
    return _prune_shipment(out)


# Keys the SI reads as "explicitly none" — kept even when null/empty.
_SHIPMENT_KEEP_EMPTY = {"eta", "pickup_point", "authorized_receivers", "open_cases", "scheduled_date",
                        "window_code", "scheduled_window", "hold_reason_en", "hold_reason_ar", "customs"}
_SHIPMENT_DROP = {"weight_kg", "eta_start", "eta_end", "delivery_code_issued_at", "driver_id",
                  "last_scan_at", "delivered_at", "created_at", "updated_at"}


def _prune_shipment(out: dict) -> dict:
    """v1.0 pruning (healthcare v3.5 discipline): the workhorse is re-sent on
    every tool-loop iteration. Null / empty keys carry no information for the
    model, raw ISO timestamps are duplicated by their labels, and a P3 lookup
    (8 parcels) went from ~48 KB to roughly half after this."""
    return {k: v for k, v in out.items()
            if k not in _SHIPMENT_DROP
            and (k in _SHIPMENT_KEEP_EMPTY or not (v is None or v == [] or v == {} or v == ""))}


async def _require_shipment(tracking_number: Optional[str], owner: str) -> dict:
    tn = (tracking_number or "").strip().upper().replace(" ", "")
    if not tn:
        raise bad_request("validation_error", "tracking_number is required")
    s = await sb_get_one("shipments", {"tracking_number": f"eq.{tn}"}, owner=owner)
    if not s:
        raise not_found("shipment_not_found", f"No shipment {tn} found")
    return s


async def _require_customer(customer_id: Optional[str], owner: str) -> dict:
    cid = (customer_id or "").strip().upper()
    if not cid:
        raise bad_request("validation_error", "customer_id is required")
    c = await sb_get_one("customers", {"customer_id": f"eq.{cid}"}, owner=owner)
    if not c:
        raise not_found("customer_not_found", f"No customer {cid} found")
    return c


def _pay_url(token: Optional[str]) -> Optional[str]:
    return f"{PORTAL_BASE_URL}/pay/{token}" if token else None


def _payment_out(p: dict) -> dict:
    out = _strip_one(p, "pay_token")
    out["amount_sar"] = money(p.get("amount_sar"))
    out["pay_url"] = _pay_url(p.get("pay_token")) if p.get("status") == "Pending" else None
    if p.get("paid_at"):
        dt = parse_dt(p["paid_at"])
        out["paid_at_label_en"], out["paid_at_label_ar"] = dt_label_en(dt), dt_label_ar(dt)
    return out


# ============================================================
# Ops: /, /health, /whoami
# ============================================================

@app.get("/")
async def root():
    return {
        "service": SERVICE_NAME,
        "version": API_VERSION,
        "multi_tenant": True,
        "auth_mode": AUTH_MODE,
        "supabase_configured": bool(SUPABASE_URL),
        "default_owner_configured": bool(DEFAULT_OWNER_ID),
        "portal_configured": bool(PORTAL_BASE_URL),
        "timezone": TZ_NAME,
        "now": _now().isoformat(timespec="seconds"),
    }


@app.get("/health")
async def health():
    """Derived-status health check (cron-job.org warms this every 5 min —
    which also keeps the shared-catalog cache and the Mode B token hot).
    'ok' only when env, auth, a per-tenant read AND the shared catalogs all
    work — a green /health that can't read demo data is how the healthcare
    tenant-routing bug hid for days."""
    if _missing:
        return {"status": "degraded", "reason": f"missing env vars: {', '.join(_missing)}",
                "auth": _auth_stats(), "version": API_VERSION}
    checks = {"api": "ok"}
    try:
        rows = await sb_get("customers", {"select": "customer_id", "limit": "1"}, owner=DEFAULT_OWNER_ID)
        checks["supabase"] = "ok" if rows else "no_data"
    except HTTPException as e:
        checks["supabase"] = f"error: {e.detail}"
    except Exception as e:
        checks["supabase"] = f"error: {str(e)[:200]}"
    try:
        refs = await get_refs()
        checks["reference_data"] = "ok" if refs["districts"] and refs["windows"] and refs["rules"] else "empty"
    except Exception as e:
        checks["reference_data"] = f"error: {str(e)[:200]}"
    auth = _auth_stats()
    healthy = (checks["supabase"] in ("ok", "no_data") and checks["reference_data"] == "ok"
               and auth["authenticated"])
    return {
        "status": "ok" if healthy else "degraded",
        "version": API_VERSION,
        "multi_tenant": True,
        "auth_mode": AUTH_MODE,
        "default_owner_configured": bool(DEFAULT_OWNER_ID),
        "env": {"portal_base_url": PORTAL_BASE_URL or None, "docs_bucket": DOCS_BUCKET,
                "tz": TZ_NAME, "location_snap": DEMO_LOCATION_SNAP},
        "now": _now().isoformat(timespec="seconds"),
        "checks": checks,
        "auth": auth,
        "reference_cache": _cache_stats(),
        "tenant_cache": _tenant_cache_stats(),
    }


@app.get("/whoami")
async def whoami(caller_phone: Optional[str] = Query(None, description="Phone to resolve, with or without '+'")):
    """Resolve a caller_phone to its demo tenant and explain the result.
    Bypasses (but reports) the tenant cache so a stale entry is visible."""
    normalized = normalize_phone(caller_phone)
    result: dict = {
        "caller_phone_received": caller_phone,
        "normalized_phone": normalized,
        "default_owner_id": DEFAULT_OWNER_ID or None,
        "auth_mode": AUTH_MODE,
    }
    cached = _tenant_cache.get(normalized) if normalized else None
    result["cache"] = {
        "present": bool(cached),
        "owner_id": cached["owner_id"] if cached else None,
        "age_seconds": round(_monotonic() - cached["ts"], 1) if cached else None,
    }
    if not normalized:
        result.update({"matched": False, "fell_back": True, "resolved_owner_id": DEFAULT_OWNER_ID or None,
                       "reason": "caller_phone missing or not a valid phone number"})
        return result
    rows = await _sb_raw_get("demo_users", {"whatsapp_number": f"eq.{normalized}",
                                            "select": "owner_id,email", "limit": "1"})
    if rows:
        result.update({"matched": True, "fell_back": False, "resolved_owner_id": rows[0]["owner_id"],
                       "tenant_email": rows[0].get("email")})
    else:
        all_rows = await _sb_raw_get("demo_users", {"select": "owner_id"})
        result.update({
            "matched": False, "fell_back": True, "resolved_owner_id": DEFAULT_OWNER_ID or None,
            "visible_tenant_count": len(all_rows),
            "reason": (f"no demo_users row with whatsapp_number = '{normalized}' "
                       f"({len(all_rows)} tenants visible to the API) — "
                       "writes for this caller will land in the fallback tenant"),
        })
    return result


# ============================================================
# READ: /customer (the workhorse — one gather)
# ============================================================

@app.get("/customer")
async def get_customer(
    customer_id: Optional[str] = Query(None),
    tracking_number: Optional[str] = Query(None),
    phone: Optional[str] = Query(None),
    caller_phone: Optional[str] = Query(None, description="Demo tenant routing — WhatsApp sender number"),
):
    """Full customer package: profile, rules, active + delivered shipments
    (enriched, with eligibility), cases, returns, payments, tax invoices,
    store alerts, summary."""
    owner = await resolve_owner(caller_phone)
    now = _now()
    today = now.date()

    # Step 1: find the customer (within this tenant)
    customer = None
    if customer_id:
        customer = await sb_get_one("customers", {"customer_id": f"eq.{customer_id.strip().upper()}"}, owner=owner)
    elif tracking_number:
        tn = tracking_number.strip().upper().replace(" ", "")
        s = await sb_get_one("shipments", {"tracking_number": f"eq.{tn}", "select": "customer_id"}, owner=owner)
        if not s:
            raise not_found("shipment_not_found", f"No shipment {tn} found")
        customer = await sb_get_one("customers", {"customer_id": f"eq.{s['customer_id']}"}, owner=owner)
    elif phone:
        normalized = normalize_phone(phone)
        if normalized:
            customer = await sb_get_one("customers", {"phone": f"eq.{normalized}"}, owner=owner)
        if not customer:
            customer = await sb_get_one("customers", {"phone": f"eq.{phone.strip()}"}, owner=owner)
    else:
        raise bad_request("validation_error", "Provide customer_id, tracking_number or phone")
    if not customer:
        raise not_found("customer_not_found", "No customer matched")
    cid = customer["customer_id"]

    # Step 2: ALL related data in ONE gather. shipment_events and
    # authorized_receivers are keyed by tracking_number (no customer_id), so
    # they're read tenant-wide and filtered in Python: at demo scale (~100
    # events per tenant) that is one cheap query instead of a second round
    # trip that would have to wait for the shipment list.
    (shipments, events, receivers, cases, returns_, payments, tax_invoices,
     alerts, refs) = await asyncio.gather(
        sb_get("shipments", {"customer_id": f"eq.{cid}", "order": "tracking_number.asc"}, owner=owner),
        sb_get("shipment_events", {"order": "event_at.desc"}, owner=owner),
        sb_get("authorized_receivers", {"status": "eq.Active", "order": "receiver_id.asc"}, owner=owner),
        sb_get("cases", {"customer_id": f"eq.{cid}", "order": "opened_at.desc"}, owner=owner),
        sb_get("returns", {"customer_id": f"eq.{cid}", "order": "created_at.desc"}, owner=owner),
        sb_get("payment_requests", {"customer_id": f"eq.{cid}", "order": "created_at.desc"}, owner=owner),
        sb_get("tax_invoices", {"customer_id": f"eq.{cid}", "order": "issued_at.desc"}, owner=owner),
        sb_get("store_alerts", {"customer_id": f"eq.{cid}", "order": "created_at.desc"}, owner=owner),
        get_refs(),
    )
    tns = {s["tracking_number"] for s in shipments}
    by_tn = lambda rows: {tn: [r for r in rows if r.get("tracking_number") == tn] for tn in tns}
    ev_by, rc_by, cs_by = by_tn(events), by_tn(receivers), by_tn(cases)
    open_ret = {r["tracking_number"] for r in returns_ if r.get("status") != "Returned"}

    # POD photos: signed in parallel (cached ~48 min per path).
    photo_paths = {s["tracking_number"]: (s.get("pod") or {}).get("photo_path")
                   for s in shipments if is_delivered(s) and (s.get("pod") or {}).get("photo_path")}
    signed = dict(zip(photo_paths, await asyncio.gather(*(storage_sign(p) for p in photo_paths.values()))))

    # Step 3: enrich + segment
    active, delivered = [], []
    cutoff = today - timedelta(days=120)
    for s in shipments:
        e = enrich_shipment(s, refs, now, receivers=rc_by.get(s["tracking_number"]),
                            cases=cs_by.get(s["tracking_number"]), events=ev_by.get(s["tracking_number"]),
                            open_return=s["tracking_number"] in open_ret,
                            pod_photo_url=signed.get(s["tracking_number"]))
        if is_delivered(s):
            dd = delivered_date(s)
            if dd is None or dd >= cutoff:
                delivered.append((s.get("delivered_at") or "", e))
        elif s.get("status") != "Returned to Sender":
            active.append(e)
    delivered = [e for _, e in sorted(delivered, key=lambda x: x[0], reverse=True)]

    stores = refs["stores"]
    pending = [_payment_out(p) for p in payments if p.get("status") == "Pending"]
    paid = [_payment_out(p) for p in payments if p.get("status") == "Paid"]
    prefs = {"preferred_window": None, "earliest_time": None, "call_before_arrival": False,
             "avoid_prayer_times": False, "leave_with_security": False, **(customer.get("preferences") or {})}
    home = refs["districts"].get(customer.get("district_id")) or {}
    cust_out = _strip_one(customer, "demo_notes")
    cust_out.update({
        "preferences": prefs,
        "preferred_window_label_en": window_labels(prefs.get("preferred_window"), refs["windows"])["window_label_en"],
        "has_company_details": bool(customer.get("company_name_en") and customer.get("vat_number")),
        "district_name_en": home.get("name_en"),
        "district_name_ar": home.get("name_ar"),
    })

    return {
        "customer": cust_out,
        "rules": refs["rules"],
        "active_shipments": active,
        "delivered_shipments": delivered,
        "open_cases": [
            _strip_one(c) | labels("due", parse_date(c.get("due_at")))
            for c in cases if c.get("status") in OPEN_CASE_STATUSES
        ],
        "closed_cases": [_strip_one(c) for c in cases if c.get("status") not in OPEN_CASE_STATUSES][:5],
        "returns": [
            _strip_one(r) | labels("pickup_date", parse_date(r.get("pickup_date")))
            | window_labels(r.get("pickup_window"), refs["windows"])
            for r in returns_
        ],
        "payment_requests": {"pending": pending, "paid": paid},
        "tax_invoices": [
            _strip_one(t) | {"subtotal_sar": money(t.get("subtotal_sar")), "vat_sar": money(t.get("vat_sar")),
                             "total_sar": money(t.get("total_sar"))}
            for t in tax_invoices
        ],
        "store_alerts": [
            _strip_one(a) | {"store_name_en": (stores.get(a.get("store_id")) or {}).get("name_en"),
                             "store_name_ar": (stores.get(a.get("store_id")) or {}).get("name_ar")}
            for a in alerts
        ],
        "summary": {
            "active_count": len(active),
            "delivered_count": len(delivered),
            "pending_payments_total_sar": money(sum(p["amount_sar"] for p in pending)),
            "today": today.isoformat(),
            "today_label_en": date_label_en(today),
            "today_label_ar": date_label_ar(today),
        },
    }


# ============================================================
# READ: /delivery/slots
# ============================================================

@app.get("/delivery/slots")
async def get_delivery_slots(
    tracking_number: str = Query(...),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD; default tomorrow"),
    avoid_prayer: Optional[str] = Query(None, description="Comma list (maghrib,isha) or true (= maghrib)"),
    caller_phone: Optional[str] = Query(None),
):
    """Next 5 delivery days (Sat–Thu) × windows W1–W4, with prayer-conflict
    flags and prayer times for the delivery city. Windows that break the
    shipment's time_rule, the avoid_prayer filter or the customer's saved
    earliest_time are moved to `excluded` (with the reason) rather than
    silently dropped, so the agent can explain why a window is missing."""
    owner = await resolve_owner(caller_phone)
    s, refs = await asyncio.gather(_require_shipment(tracking_number, owner), get_refs())
    cust = await sb_get_one("customers", {"customer_id": f"eq.{s['customer_id']}",
                                          "select": "customer_id,preferences"}, owner=owner)
    today = _today()
    tomorrow = today + timedelta(days=1)
    start = parse_date(from_date) if from_date else tomorrow
    if start is None:
        raise bad_request("invalid_date", "from_date must be YYYY-MM-DD")
    start = max(start, tomorrow)
    tr = s.get("time_rule") or {}
    prefs = (cust or {}).get("preferences") or {}
    avoid = sorted(set(parse_avoid_prayers(tr.get("avoid_prayers")) + parse_avoid_prayers(avoid_prayer)))
    lat, lng = shipment_coords(s, refs["districts"])
    slots, excluded, prayers = compute_slots(
        start, 5, refs["windows"], lat, lng, avoid,
        hhmm_to_min(tr.get("not_before")), hhmm_to_min(tr.get("not_after")), hhmm_to_min(prefs.get("earliest_time")))
    cur_d, cur_w = s.get("scheduled_date"), s.get("scheduled_window")
    for sl in slots:
        sl["is_current"] = sl["date"] == cur_d and sl["window_code"] == cur_w
    out = {
        "tracking_number": s["tracking_number"],
        "status": s.get("status"),
        "city_en": shipment_city(s, refs["districts"]),
        **labels("current_scheduled_date", parse_date(cur_d)),
        "current_window_code": cur_w,
        **{f"current_{k}": v for k, v in window_labels(cur_w, refs["windows"]).items()},
        "slots": slots,
        "excluded": excluded,
        "filters_applied": {"avoid_prayers": avoid, "not_before": tr.get("not_before"),
                            "not_after": tr.get("not_after"), "customer_earliest_time": prefs.get("earliest_time")},
        "prayer_times_today": prayer_row(today, lat, lng),
        "prayer_times": prayers,
        "can_reschedule": s.get("status") in MOVABLE_STATUSES,
    }
    rts = parse_date(s.get("return_to_sender_date"))
    out["days_until_return_to_sender"] = (rts - today).days if rts else None
    return out


# ============================================================
# READ: /pickup-points
# ============================================================

@app.get("/pickup-points")
async def find_pickup_points(
    tracking_number: str = Query(...),
    lat: Optional[float] = Query(None),
    lng: Optional[float] = Query(None),
    caller_phone: Optional[str] = Query(None),
):
    """Nearest 3 pickup points with free capacity, from the customer's pin
    (snapped when abroad) or the shipment's delivery point."""
    owner = await resolve_owner(caller_phone)
    s, refs = await asyncio.gather(_require_shipment(tracking_number, owner), get_refs())
    snapped = False
    if lat is None or lng is None:
        olat, olng = shipment_coords(s, refs["districts"])
    elif DEMO_LOCATION_SNAP and needs_snap(lat, lng, refs["districts"]):
        olat, olng = shipment_coords(s, refs["districts"])
        snapped = True
    else:
        olat, olng = lat, lng
    pts = []
    for p in refs["pickup_points"].values():
        if p.get("lat") is None or int(p.get("capacity") or 0) <= 0:
            continue
        dist = haversine_m(olat, olng, p["lat"], p["lng"])
        pts.append((dist, p))
    pts.sort(key=lambda x: x[0])
    out = []
    for dist, p in pts[:3]:
        out.append({
            "pickup_point_id": p["pickup_point_id"], "type": p.get("type"),
            "name_en": p.get("name_en"), "name_ar": p.get("name_ar"),
            "address_en": p.get("address_en"), "address_ar": p.get("address_ar"),
            "city_en": p.get("city_en"),
            **distance_labels(dist),
            "hold_hours": p.get("hold_hours"),
            "hours_en": p.get("hours_en"), "hours_ar": p.get("hours_ar"),
        })
    return {"tracking_number": s["tracking_number"], "origin": {"lat": olat, "lng": olng},
            "location_snapped": snapped, "pickup_points": out, "count": len(out)}


# ============================================================
# READ: /address/check
# ============================================================

async def resolve_address(s: dict, refs: dict, lat: Optional[float], lng: Optional[float],
                          address_text: Optional[str], customer: Optional[dict] = None) -> dict:
    """Pin / text → district, served, short address, fee. Shared by
    /address/check and the change_address / back_to_home_delivery writes so
    the confirm step and the write always agree."""
    districts, rules = refs["districts"], refs["rules"]
    snapped = False
    district = None
    plat = plng = None
    text_district = match_district_text(address_text, districts, prefer_city=shipment_city(s, districts)) \
        if address_text else None
    if lat is not None and lng is not None:
        lat, lng = float(lat), float(lng)
        if DEMO_LOCATION_SNAP and needs_snap(lat, lng, districts):
            snapped = True
            # Abroad: the typed district name (L-03 "Al Malqa") beats the anchor.
            anchor = text_district or districts.get(s.get("delivery_district_id")) \
                or districts.get((customer or {}).get("district_id"))
            if not anchor:
                anchor, _ = nearest_district(24.7136, 46.6753, districts)
            district = anchor
            plat, plng = float(anchor["centroid_lat"]), float(anchor["centroid_lng"])
        else:
            district, _ = nearest_district(lat, lng, districts)
            plat, plng = lat, lng
    elif text_district:
        district = text_district
        plat, plng = float(district["centroid_lat"]), float(district["centroid_lng"])
    elif address_text:
        raise not_found("address_not_resolved",
                        f"Could not match '{address_text}' to a district we know. Ask for a location pin.")
    else:
        raise bad_request("validation_error", "Provide lat/lng or address_text")
    if not district:
        raise not_found("address_not_resolved", "No district found for this location")
    cur_city = shipment_city(s, districts)
    same_city = (cur_city is None) or district.get("city_en") == cur_city
    fee = rule(rules, "redirect_same_city_fee_sar") if same_city else rule(rules, "redirect_other_city_fee_sar")
    centre = refs["centres"].get(district.get("centre_id")) or {}
    return {
        "district_id": district["district_id"],
        "name_en": district.get("name_en"), "name_ar": district.get("name_ar"),
        "city_en": district.get("city_en"), "city_ar": district.get("city_ar"),
        "zone": district.get("zone"),
        "served": bool(district.get("served")),
        "short_address": short_address_for(district.get("short_code_prefix"), plat, plng),
        "lat": round(plat, 6), "lng": round(plng, 6),
        "same_city_as_current": same_city,
        "redirect_fee_sar": _num(fee),
        "centre_id": district.get("centre_id"),
        "centre_name_en": centre.get("name_en"), "centre_name_ar": centre.get("name_ar"),
        "location_snapped": snapped,
        "address_label_en": f"{district.get('name_en')}, {district.get('city_en')}",
        "address_label_ar": f"{district.get('name_ar')}، {district.get('city_ar')}",
    }


@app.get("/address/check")
async def check_address(
    tracking_number: str = Query(...),
    lat: Optional[float] = Query(None),
    lng: Optional[float] = Query(None),
    address_text: Optional[str] = Query(None),
    caller_phone: Optional[str] = Query(None),
):
    owner = await resolve_owner(caller_phone)
    s, refs = await asyncio.gather(_require_shipment(tracking_number, owner), get_refs())
    return {"tracking_number": s["tracking_number"], **(await resolve_address(s, refs, lat, lng, address_text))}


# ============================================================
# WRITE: /shipment/update — the action multiplexer
# ============================================================

UPDATE_ACTIONS = {"reschedule", "change_address", "redirect_pickup_point", "back_to_home_delivery",
                  "hold", "consolidate", "driver_note", "time_rule"}
_NOTE_ONLY_ACTIONS = {"driver_note", "time_rule"}


def _require_status(s: dict, action: str):
    st = s.get("status")
    if action in _NOTE_ONLY_ACTIONS:
        if not is_active(s):
            raise conflict("not_allowed_in_status", f"Shipment is {st}; it can no longer be changed.",
                           status=st, tracking_number=s["tracking_number"])
        return
    if action == "back_to_home_delivery":
        if st != "At Branch":
            raise conflict("not_allowed_in_status", f"Only shipments waiting At Branch can go back to home delivery (this one is {st}).",
                           status=st, tracking_number=s["tracking_number"])
        return
    if st not in MOVABLE_STATUSES or is_delivered(s):
        msg = (f"Shipment is {st} — the driver already has it, so only a driver note, a time rule or an "
               f"authorised receiver can be added now.") if st == "Out for Delivery" else f"Shipment is {st}; this change is not possible."
        raise conflict("not_allowed_in_status", msg, status=st, tracking_number=s["tracking_number"])


def _append_note(notes, note: str) -> list:
    notes = list(notes or [])
    if note not in notes:
        notes.append(note)
    return notes


def _sched_response(d: Optional[date], code: Optional[str], windows: dict, prefix: str = "scheduled_date") -> dict:
    return {**labels(prefix, d), "window_code": code, **window_labels(code, windows),
            "scheduled_in_days": (d - _today()).days if d else None}


@app.post("/shipment/update")
async def update_delivery(payload: dict = Body(...)):
    """All customer-driven shipment changes. Body: {tracking_number, action,
    caller_phone, …action fields}. See README for per-action fields."""
    owner = await resolve_owner(payload.get("caller_phone"))
    action = str(payload.get("action") or "").strip().lower()
    if action not in UPDATE_ACTIONS:
        raise bad_request("invalid_action", f"action must be one of {sorted(UPDATE_ACTIONS)}")
    s, refs = await asyncio.gather(_require_shipment(payload.get("tracking_number"), owner), get_refs())
    _require_status(s, action)
    handler = {
        "reschedule": _act_reschedule, "change_address": _act_change_address,
        "redirect_pickup_point": _act_redirect_pickup, "back_to_home_delivery": _act_change_address,
        "hold": _act_hold, "consolidate": _act_consolidate, "driver_note": _act_driver_note,
        "time_rule": _act_time_rule,
    }[action]
    return await handler(s, refs, payload, owner, action)


async def _customer_prefs(s: dict, owner: str) -> dict:
    c = await sb_get_one("customers", {"customer_id": f"eq.{s['customer_id']}", "select": "*"}, owner=owner)
    return c or {}


async def _act_reschedule(s, refs, p, owner, action):
    d = parse_date(p.get("date"))
    code = str(p.get("window_code") or "").upper() or None
    windows = refs["windows"]
    if not d or code not in windows:
        raise bad_request("validation_error", "reschedule needs date (YYYY-MM-DD) and window_code (W1–W4)")
    today = _today()
    tr = s.get("time_rule") or {}
    cust = await _customer_prefs(s, owner)
    prefs = cust.get("preferences") or {}
    lat, lng = shipment_coords(s, refs["districts"])
    reason = None
    if d <= today:
        reason = "The earliest reschedule date is tomorrow."
    elif not is_delivery_day(d):
        reason = "We don't deliver on Fridays."
    elif (d - today).days > 30:
        reason = "That date is too far ahead."
    else:
        avoid = parse_avoid_prayers(tr.get("avoid_prayers"))
        _, r = slot_filter_reason(windows[code], prayer_times_for(d, lat, lng), avoid,
                                  hhmm_to_min(tr.get("not_before")), hhmm_to_min(tr.get("not_after")),
                                  hhmm_to_min(prefs.get("earliest_time")))
        if r:
            reason = {"prayer": "That window overlaps a prayer time the customer asked us to avoid.",
                      "not_before": "That window starts before the customer's time rule allows.",
                      "not_after": "That window ends after the customer's time rule allows.",
                      "customer_earliest_time": "That window starts before the customer's saved earliest time."}[r]
    if reason:
        raise conflict("slot_unavailable", reason, date=d.isoformat(), window_code=code,
                       date_label_en=date_label_en(d), date_label_ar=date_label_ar(d))
    old_d, old_w = parse_date(s.get("scheduled_date")), s.get("scheduled_window")
    notes = _append_note(s.get("driver_notes"), "Rescheduled by customer")
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"},
                    {"scheduled_date": d.isoformat(), "scheduled_window": code, "driver_notes": notes}, owner=owner)
    wl = window_labels(code, windows)
    await add_events([{"tracking_number": s["tracking_number"], "status": s.get("status"),
                       "location_en": s.get("last_scan_location_en"), "location_ar": s.get("last_scan_location_ar"),
                       "note_en": f"Delivery rescheduled by customer to {date_label_en(d)}, {wl['window_label_en']}",
                       "note_ar": f"أعاد العميل جدولة التوصيل إلى {date_label_ar(d)}، {wl['window_label_ar']}"}], owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Delivery Rescheduled",
                           f"{s['tracking_number']} moved from {date_label_en(old_d) or 'unscheduled'} "
                           f"{old_w or ''} to {date_label_en(d)} {code} ({wl['window_label_en']})",
                           {"from_date": old_d.isoformat() if old_d else None, "from_window": old_w,
                            "to_date": d.isoformat(), "to_window": code}, owner=owner)
    rts = parse_date(s.get("return_to_sender_date"))
    return {"ok": True, "tracking_number": s["tracking_number"], "action": action,
            **_sched_response(d, code, windows),
            "date_label_en": date_label_en(d), "date_label_ar": date_label_ar(d),
            "days_until_return_to_sender": (rts - _today()).days if rts
            else int(rule(refs["rules"], "return_to_sender_after_days")),
            "driver_notified": True}


async def _act_change_address(s, refs, p, owner, action):
    """change_address and back_to_home_delivery share this path."""
    lat, lng = p.get("lat"), p.get("lng")
    lat = float(lat) if lat not in (None, "") else None
    lng = float(lng) if lng not in (None, "") else None
    cust = await _customer_prefs(s, owner)
    addr_text = p.get("address_text")
    if action == "back_to_home_delivery" and lat is None and not addr_text:
        # No new pin: keep the existing address (the problem was the driver).
        if s.get("delivery_district_id"):
            d0 = refs["districts"].get(s["delivery_district_id"]) or {}
            addr_text = d0.get("name_en")
    res = await resolve_address(s, refs, lat, lng, addr_text, customer=cust)
    if not res["served"]:
        raise conflict("address_not_served",
                       f"We don't deliver to {res['name_en']} yet. A nearby pickup point can be used instead.",
                       district_id=res["district_id"], name_en=res["name_en"], name_ar=res["name_ar"])
    windows = refs["windows"]
    prefs = cust.get("preferences") or {}
    st = s.get("status")
    hold_reason = (s.get("hold_reason_en") or "").lower()
    address_hold = st == "On Hold" and any(k in hold_reason for k in ("address", "building", "locate", "location"))
    upd = {
        "delivery_district_id": res["district_id"],
        "delivery_address_en": p.get("address_en") or res["address_label_en"],
        "delivery_address_ar": p.get("address_ar") or res["address_label_ar"],
        "delivery_short_address": res["short_address"],
        "delivery_lat": res["lat"], "delivery_lng": res["lng"],
    }
    hold_removed = False
    new_d, new_w = parse_date(s.get("scheduled_date")), s.get("scheduled_window")
    reschedule = address_hold or action == "back_to_home_delivery" or not new_d
    if action == "back_to_home_delivery":
        upd.update({"service_type": "Home Delivery", "pickup_point_id": None, "locker_code": None,
                    "status": "At Sorting Centre", "hold_reason_en": None, "hold_reason_ar": None})
        hold_removed = True
    elif address_hold:
        upd.update({"status": "At Sorting Centre", "hold_reason_en": None, "hold_reason_ar": None})
        hold_removed = True
    if hold_removed:
        upd["current_centre_id"] = res.get("centre_id") or s.get("current_centre_id")
    if reschedule and st in MOVABLE_STATUSES:
        tr = s.get("time_rule") or {}
        pick = pick_next_slot(_today() + timedelta(days=1), windows, res["lat"], res["lng"],
                              prefs.get("preferred_window"), parse_avoid_prayers(tr.get("avoid_prayers")),
                              hhmm_to_min(tr.get("not_before")), hhmm_to_min(tr.get("not_after")),
                              hhmm_to_min(prefs.get("earliest_time")))
        if pick:
            new_d, new_w = pick
            upd.update({"scheduled_date": new_d.isoformat(), "scheduled_window": new_w})
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, upd, owner=owner)
    centre = refs["centres"].get(upd.get("current_centre_id") or s.get("current_centre_id")) or {}
    what_en = "Back to home delivery" if action == "back_to_home_delivery" else "Delivery address changed"
    what_ar = "إعادة للتوصيل المنزلي" if action == "back_to_home_delivery" else "تم تغيير عنوان التوصيل"
    await add_events([{"tracking_number": s["tracking_number"], "status": upd.get("status", st),
                       "location_en": centre.get("name_en") or s.get("last_scan_location_en"),
                       "location_ar": centre.get("name_ar") or s.get("last_scan_location_ar"),
                       "note_en": f"{what_en} by customer: {res['address_label_en']} ({res['short_address']})"
                                  + (" — hold removed" if hold_removed else ""),
                       "note_ar": f"{what_ar} بطلب العميل: {res['address_label_ar']} ({res['short_address']})"
                                  + (" — تم رفع التعليق" if hold_removed else "")}], owner)
    await log_agent_action(s["customer_id"], s["tracking_number"],
                           "Back To Home Delivery" if action == "back_to_home_delivery" else "Address Changed",
                           f"{s['tracking_number']}: {what_en.lower()} to {res['address_label_en']} "
                           f"(short address {res['short_address']})" + ("; hold removed" if hold_removed else "")
                           + (f"; scheduled {date_label_en(new_d)} {new_w}" if new_d else ""),
                           {"district_id": res["district_id"], "short_address": res["short_address"],
                            "hold_removed": hold_removed, "location_snapped": res["location_snapped"],
                            "fee_sar": res["redirect_fee_sar"]}, owner=owner)
    return {"ok": True, "tracking_number": s["tracking_number"], "action": action,
            "new_address": {k: res[k] for k in ("district_id", "name_en", "name_ar", "city_en", "city_ar",
                                                 "address_label_en", "address_label_ar")},
            "short_address": res["short_address"], "hold_removed": hold_removed,
            "status": upd.get("status", st), "service_type": upd.get("service_type", s.get("service_type")),
            "same_city_as_current": res["same_city_as_current"], "fee_sar": res["redirect_fee_sar"],
            "location_snapped": res["location_snapped"],
            "centre_name_en": centre.get("name_en"), "centre_name_ar": centre.get("name_ar"),
            **_sched_response(new_d, new_w, windows)}


async def _act_redirect_pickup(s, refs, p, owner, action):
    pid = str(p.get("pickup_point_id") or "").strip().upper()
    pp = refs["pickup_points"].get(pid)
    if not pp:
        raise not_found("pickup_point_not_found", f"No pickup point {pid}")
    if int(pp.get("capacity") or 0) <= 0:
        raise conflict("slot_unavailable", f"{pp.get('name_en')} has no free space right now.", pickup_point_id=pid)
    cur_city = shipment_city(s, refs["districts"])
    if cur_city and pp.get("city_en") and pp["city_en"] != cur_city:
        raise conflict("not_same_city", f"{pp.get('name_en')} is in {pp['city_en']}, the parcel is for {cur_city}.",
                       pickup_point_city=pp["city_en"], shipment_city=cur_city)
    is_locker = pp.get("type") == "Locker"
    code = f"{secrets.randbelow(9000) + 1000}" if is_locker else None
    d = next_delivery_day(_today())
    hold_hours = int(pp.get("hold_hours") or (rule(refs["rules"], "locker_hold_hours") if is_locker else 72))
    upd = {"service_type": "Pickup Point", "pickup_point_id": pid, "locker_code": code,
           "scheduled_date": d.isoformat(), "scheduled_window": "W1"}
    if s.get("status") == "On Hold":
        upd.update({"status": "At Sorting Centre", "hold_reason_en": None, "hold_reason_ar": None})
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, upd, owner=owner)
    await add_events([{"tracking_number": s["tracking_number"], "status": upd.get("status", s.get("status")),
                       "location_en": s.get("last_scan_location_en"), "location_ar": s.get("last_scan_location_ar"),
                       "note_en": f"Redirected by customer to {pp.get('name_en')}",
                       "note_ar": f"تم التحويل بطلب العميل إلى {pp.get('name_ar')}"}], owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Redirected To Pickup Point",
                           f"{s['tracking_number']} redirected to {pp.get('name_en')} ({pp.get('type')}); "
                           f"ready {date_label_en(d)}, held {hold_hours}h",
                           {"pickup_point_id": pid, "type": pp.get("type"), "locker_code_issued": bool(code)},
                           owner=owner)
    return {"ok": True, "tracking_number": s["tracking_number"], "action": action,
            "pickup_point": {"pickup_point_id": pid, "type": pp.get("type"), "name_en": pp.get("name_en"),
                             "name_ar": pp.get("name_ar"), "address_en": pp.get("address_en"),
                             "address_ar": pp.get("address_ar"), "hours_en": pp.get("hours_en"),
                             "hours_ar": pp.get("hours_ar"), "lat": pp.get("lat"), "lng": pp.get("lng")},
            "locker_code": code, "hold_hours": hold_hours,
            **labels("ready_date", d), **window_labels("W1", refs["windows"]),
            "ready_in_days": (d - _today()).days}


async def _act_hold(s, refs, p, owner, action):
    until = parse_date(p.get("until_date"))
    today = _today()
    if not until:
        raise bad_request("validation_error", "hold needs until_date (YYYY-MM-DD)")
    if until <= today:
        raise bad_request("invalid_date", "until_date must be after today")
    max_days = int(rule(refs["rules"], "hold_max_days"))
    if (until - today).days > max_days:
        latest = today + timedelta(days=max_days)
        raise conflict("hold_too_long", f"We can hold a parcel for up to {max_days} days.",
                       max_days=max_days, requested_days=(until - today).days,
                       **labels("latest_hold_date", latest))
    had_return = bool(s.get("return_to_sender_date"))
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"},
                    {"status": "Held — Customer Request", "hold_until": until.isoformat(),
                     "return_to_sender_date": None, "scheduled_date": None, "scheduled_window": None}, owner=owner)
    await add_events([{"tracking_number": s["tracking_number"], "status": "Held — Customer Request",
                       "location_en": s.get("last_scan_location_en"), "location_ar": s.get("last_scan_location_ar"),
                       "note_en": f"Held at customer request until {date_label_en(until)}"
                                  + (" — return to sender cancelled" if had_return else ""),
                       "note_ar": f"محفوظة بطلب العميل حتى {date_label_ar(until)}"
                                  + (" — تم إلغاء الإرجاع للمرسل" if had_return else "")}], owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Shipment Held",
                           f"{s['tracking_number']} held until {date_label_en(until)}"
                           + ("; return to sender cancelled" if had_return else ""),
                           {"hold_until": until.isoformat(), "return_cancelled": had_return}, owner=owner)
    return {"ok": True, "tracking_number": s["tracking_number"], "action": action,
            "status": "Held — Customer Request", **labels("hold_until", until),
            "hold_days": (until - today).days, "storage_fee_sar": 0, "return_cancelled": had_return}


async def _act_consolidate(s, refs, p, owner, action):
    tns = [str(t).strip().upper().replace(" ", "") for t in _as_list(p.get("tracking_numbers"))]
    if s["tracking_number"] not in tns:
        tns.insert(0, s["tracking_number"])
    tns = list(dict.fromkeys(t for t in tns if t))
    if len(tns) < 2:
        raise bad_request("validation_error", "consolidate needs at least two tracking_numbers")
    rows = await sb_get("shipments", {"tracking_number": f"in.({','.join(tns)})"}, owner=owner)
    found = {r["tracking_number"]: r for r in rows}
    missing = [t for t in tns if t not in found or found[t].get("customer_id") != s["customer_id"]]
    if missing:
        raise not_found("shipment_not_found", f"Not this customer's shipment(s): {', '.join(missing)}", missing=missing)
    for t in tns:
        _require_status(found[t], "consolidate")
    cities = {shipment_city(found[t], refs["districts"]) for t in tns}
    if len(cities) > 1:
        raise conflict("not_same_city", "These parcels are going to different cities, so they can't come together.",
                       cities=sorted(c for c in cities if c))
    today = _today()
    latest = max((parse_date(found[t].get("scheduled_date")) or parse_date(found[t].get("eta_date"))
                  or today + timedelta(days=1)) for t in tns)
    target = next_delivery_day(max(latest, today))
    cust = await _customer_prefs(s, owner)
    prefs = cust.get("preferences") or {}
    lat, lng = shipment_coords(s, refs["districts"])
    avoid = sorted({a for t in tns for a in parse_avoid_prayers((found[t].get("time_rule") or {}).get("avoid_prayers"))})
    pick = pick_next_slot(target, refs["windows"], lat, lng, prefs.get("preferred_window"), avoid,
                          None, None, hhmm_to_min(prefs.get("earliest_time")))
    target, code = pick if pick else (target, "W3")
    group = "GRP-" + hashlib.sha1(("|".join(sorted(tns)) + owner).encode()).hexdigest()[:8].upper()
    wl = window_labels(code, refs["windows"])
    per = []
    for t in tns:
        r = found[t]
        await sb_update("shipments", {"tracking_number": f"eq.{t}"},
                        {"consolidation_group": group, "scheduled_date": target.isoformat(),
                         "scheduled_window": code,
                         "driver_notes": _append_note(r.get("driver_notes"), f"Deliver together with group {group}")},
                        owner=owner)
        per.append({"tracking_number": t, "status": r.get("status"),
                    "previous": labels("date", parse_date(r.get("scheduled_date")) or parse_date(r.get("eta_date"))),
                    **labels("scheduled_date", target), "window_code": code, **wl})
    await add_events([{"tracking_number": t, "status": found[t].get("status"),
                       "location_en": found[t].get("last_scan_location_en"),
                       "location_ar": found[t].get("last_scan_location_ar"),
                       "note_en": f"Consolidated ({len(tns)} parcels) for delivery on {date_label_en(target)}, {wl['window_label_en']}",
                       "note_ar": f"تم تجميع ({len(tns)} شحنات) للتوصيل يوم {date_label_ar(target)}، {wl['window_label_ar']}"}
                      for t in tns], owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Shipments Consolidated",
                           f"{len(tns)} parcels ({', '.join(tns)}) grouped as {group} for {date_label_en(target)} {code}",
                           {"group": group, "tracking_numbers": tns, "date": target.isoformat(), "window": code},
                           owner=owner)
    return {"ok": True, "action": action, "consolidation_group": group, "count": len(tns),
            **labels("scheduled_date", target), "window_code": code, **wl, "shipments": per}


async def _act_driver_note(s, refs, p, owner, action):
    note = str(p.get("note") or "").strip()
    if not note:
        raise bad_request("validation_error", "driver_note needs a note")
    note = note[:300]
    notes = _append_note(s.get("driver_notes"), note)
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, {"driver_notes": notes}, owner=owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Driver Note Added",
                           f"Note for the driver on {s['tracking_number']}: \"{note}\"", {"note": note}, owner=owner)
    return {"ok": True, "tracking_number": s["tracking_number"], "action": action, "driver_notes": notes,
            "driver_notified": True}


async def _act_time_rule(s, refs, p, owner, action):
    avoid = parse_avoid_prayers(p.get("avoid_prayers"))
    nb, na = p.get("not_before"), p.get("not_after")
    if nb not in (None, "") and hhmm_to_min(nb) is None:
        raise bad_request("validation_error", "not_before must be HH:MM")
    if na not in (None, "") and hhmm_to_min(na) is None:
        raise bad_request("validation_error", "not_after must be HH:MM")
    rule_obj = {"avoid_prayers": avoid}
    if nb:
        rule_obj["not_before"] = min_to_hhmm(hhmm_to_min(nb))
    if na:
        rule_obj["not_after"] = min_to_hhmm(hhmm_to_min(na))
    windows = refs["windows"]
    lat, lng = shipment_coords(s, refs["districts"])
    d, code = parse_date(s.get("scheduled_date")), s.get("scheduled_window")
    upd = {"time_rule": rule_obj}
    changed, conflicts_now = False, []
    cust = await _customer_prefs(s, owner)
    prefs = cust.get("preferences") or {}
    if d and code in windows and s.get("status") != "Out for Delivery":
        conflicts_now, reason = slot_filter_reason(windows[code], prayer_times_for(d, lat, lng), avoid,
                                                   hhmm_to_min(rule_obj.get("not_before")),
                                                   hhmm_to_min(rule_obj.get("not_after")), None)
        if reason:
            pick = pick_next_slot(d, windows, lat, lng, prefs.get("preferred_window") or code, avoid,
                                  hhmm_to_min(rule_obj.get("not_before")), hhmm_to_min(rule_obj.get("not_after")),
                                  hhmm_to_min(prefs.get("earliest_time")))
            if pick:
                d, code = pick
                upd.update({"scheduled_date": d.isoformat(), "scheduled_window": code})
                changed = True
    parts = []
    if avoid:
        parts.append("avoid " + ", ".join(PRAYER_NAMES_EN[a] for a in avoid))
    if rule_obj.get("not_before"):
        parts.append(f"not before {rule_obj['not_before']}")
    if rule_obj.get("not_after"):
        parts.append(f"not after {rule_obj['not_after']}")
    note = "Time rule: " + ("; ".join(parts) if parts else "none")
    upd["driver_notes"] = _append_note([n for n in (s.get("driver_notes") or []) if not str(n).startswith("Time rule:")], note)
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, upd, owner=owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Time Rule Set",
                           f"{s['tracking_number']}: {note.lower()} — added to the driver's route"
                           + (f"; moved to {date_label_en(d)} {code}" if changed else ""),
                           {"time_rule": rule_obj, "window_changed": changed}, owner=owner)
    today = _today()
    return {"ok": True, "tracking_number": s["tracking_number"], "action": action, "time_rule": rule_obj,
            "window_changed": changed, **_sched_response(d, code, windows),
            "prayer_times_today": prayer_row(today, lat, lng),
            "prayer_times_on_scheduled_date": prayer_row(d, lat, lng) if d else None}


# ============================================================
# WRITE: /shipment/receiver
# ============================================================

_REL_AR = {"brother": "أخ", "sister": "أخت", "father": "أب", "mother": "أم", "wife": "زوجة",
           "husband": "زوج", "son": "ابن", "daughter": "ابنة", "friend": "صديق", "colleague": "زميل",
           "neighbour": "جار", "neighbor": "جار", "cousin": "ابن عم", "uncle": "عم", "driver": "سائق",
           "building security": "حارس العمارة", "security": "حارس العمارة", "family": "أحد أفراد الأسرة"}


@app.post("/shipment/receiver")
async def add_authorized_receiver(
    tracking_number: str = Body(..., embed=True),
    receiver_type: str = Body(..., embed=True),
    full_name: Optional[str] = Body(None, embed=True),
    id_number: Optional[str] = Body(None, embed=True),
    relationship_en: Optional[str] = Body(None, embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    owner = await resolve_owner(caller_phone)
    rt = {"building security": "Building Security", "security": "Building Security",
          "named person": "Named Person", "person": "Named Person"}.get((receiver_type or "").strip().lower())
    if not rt:
        raise bad_request("validation_error", "receiver_type must be 'Building Security' or 'Named Person'")
    last4 = None
    name = (full_name or "").strip()
    if rt == "Named Person":
        if len(name) < 3:
            raise bad_request("validation_error", "A Named Person needs their full name")
        last4 = validate_national_id(id_number)[-4:]
    s = await _require_shipment(tracking_number, owner)
    if not is_active(s):
        raise conflict("not_allowed_in_status", f"Shipment is {s.get('status')}.", status=s.get("status"))
    existing = await sb_get("authorized_receivers", {"tracking_number": f"eq.{s['tracking_number']}",
                                                     "status": "eq.Active"}, owner=owner)
    for r in existing:
        if r.get("receiver_type") == rt and (rt == "Building Security" or
                                             (r.get("id_last4") == last4 and
                                              _norm_text(r.get("full_name")) == _norm_text(name))):
            raise conflict("receiver_exists", "This receiver is already authorised on the shipment.",
                           receiver_id=r.get("receiver_id"))
    rel_en = (relationship_en or ("Building Security" if rt == "Building Security" else "")).strip() or None
    rel_ar = _REL_AR.get((rel_en or "").lower()) if rel_en else None
    now = _now()
    row = await insert_with_next_id("authorized_receivers", "receiver_id", "RCV-", 5, {
        "tracking_number": s["tracking_number"], "receiver_type": rt,
        "full_name": name or ("Building security" if rt == "Building Security" else None),
        "id_last4": last4, "relationship_en": rel_en, "relationship_ar": rel_ar,
        "consent_at": now.isoformat(), "status": "Active",
    }, owner)
    note = ("Customer consents: building security may receive (counts as delivered)" if rt == "Building Security"
            else f"Authorised receiver: {name} — check ID ending {last4}")
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"},
                    {"driver_notes": _append_note(s.get("driver_notes"), note)}, owner=owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Authorised Receiver Added",
                           f"{rt}{' ' + name if rt == 'Named Person' else ''} authorised to receive "
                           f"{s['tracking_number']}" + (f" (ID ••••{last4})" if last4 else "") + " — customer consent recorded",
                           {"receiver_id": row.get("receiver_id"), "receiver_type": rt, "id_last4": last4}, owner=owner)
    return {"ok": True, "receiver_id": row.get("receiver_id"), "tracking_number": s["tracking_number"],
            "receiver_type": rt, "full_name": row.get("full_name"), "id_last4": last4,
            "relationship_en": rel_en, "relationship_ar": rel_ar,
            "consent_at_label_en": dt_label_en(now), "consent_at_label_ar": dt_label_ar(now),
            "driver_notified": True}


# ============================================================
# WRITE: /customer/preferences
# ============================================================

@app.post("/customer/preferences")
async def update_preferences(
    customer_id: str = Body(..., embed=True),
    preferred_window: Optional[str] = Body(None, embed=True),
    earliest_time: Optional[str] = Body(None, embed=True),
    call_before_arrival: Optional[bool] = Body(None, embed=True),
    avoid_prayer_times: Optional[bool] = Body(None, embed=True),
    leave_with_security: Optional[bool] = Body(None, embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    """Merge account-level preferences and apply them to every active
    shipment the driver doesn't already have."""
    owner = await resolve_owner(caller_phone)
    refs = await get_refs()
    windows = refs["windows"]
    changes = {}
    if preferred_window not in (None, ""):
        pw = preferred_window.strip().upper()
        if pw not in windows:
            raise bad_request("validation_error", "preferred_window must be W1–W4")
        changes["preferred_window"] = pw
    if earliest_time not in (None, ""):
        m = hhmm_to_min(earliest_time)
        if m is None:
            raise bad_request("validation_error", "earliest_time must be HH:MM")
        changes["earliest_time"] = min_to_hhmm(m)
    for k, v in (("call_before_arrival", call_before_arrival), ("avoid_prayer_times", avoid_prayer_times),
                 ("leave_with_security", leave_with_security)):
        if v is not None:
            changes[k] = bool(v)
    if not changes:
        raise bad_request("validation_error", "Provide at least one preference")
    cust = await _require_customer(customer_id, owner)
    prefs = {"preferred_window": None, "earliest_time": None, "call_before_arrival": False,
             "avoid_prayer_times": False, "leave_with_security": False, **(cust.get("preferences") or {}), **changes}
    await sb_update("customers", {"customer_id": f"eq.{cust['customer_id']}"}, {"preferences": prefs}, owner=owner)

    shipments = await sb_get("shipments", {"customer_id": f"eq.{cust['customer_id']}"}, owner=owner)
    earliest = hhmm_to_min(prefs.get("earliest_time"))
    touched = []
    for s in shipments:
        if not is_active(s) or s.get("status") == "Out for Delivery":
            continue
        upd, what = {}, []
        notes = list(s.get("driver_notes") or [])
        for flag, note, label in (("call_before_arrival", "Call 10 minutes before arrival", "call before arrival"),
                                  ("leave_with_security", "Customer allows delivery to building security",
                                   "may leave with security")):
            if prefs.get(flag) and note not in notes:
                notes = _append_note(notes, note)
                what.append(label)
        if prefs.get("avoid_prayer_times"):
            tr = dict(s.get("time_rule") or {})
            if "maghrib" not in (tr.get("avoid_prayers") or []):
                tr["avoid_prayers"] = sorted(set((tr.get("avoid_prayers") or []) + ["maghrib"]))
                upd["time_rule"] = tr
                what.append("avoid Maghrib")
        code = s.get("scheduled_window")
        if code in windows and s.get("scheduled_date"):
            a, _ = window_bounds(windows[code])
            target = code
            if prefs.get("preferred_window") and prefs["preferred_window"] != code:
                target = prefs["preferred_window"]
            if earliest is not None and window_bounds(windows[target])[0] < earliest:
                later = [w for w in sorted(windows.values(), key=lambda w: window_bounds(w)[0])
                         if window_bounds(w)[0] >= earliest]
                target = later[0]["window_code"] if later else target
            if target != code:
                upd["scheduled_window"] = target
                what.append(f"window {code} → {target}")
        if notes != list(s.get("driver_notes") or []):
            upd["driver_notes"] = notes
        if upd:
            await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, upd, owner=owner)
            wl = window_labels(upd.get("scheduled_window") or code, windows)
            touched.append({"tracking_number": s["tracking_number"], "changes": what,
                            "window_code": upd.get("scheduled_window") or code, **wl,
                            **labels("scheduled_date", parse_date(s.get("scheduled_date")))})
    await log_agent_action(cust["customer_id"], None, "Preferences Saved",
                           f"Delivery preferences saved for {cust.get('full_name_en')}: "
                           + ", ".join(f"{k}={v}" for k, v in changes.items())
                           + f"; applied to {len(touched)} active shipment(s)",
                           {"changes": changes, "shipments_touched": [t["tracking_number"] for t in touched]},
                           owner=owner)
    return {"ok": True, "customer_id": cust["customer_id"], "preferences": prefs,
            "preferred_window_label_en": window_labels(prefs.get("preferred_window"), windows)["window_label_en"],
            "preferred_window_label_ar": window_labels(prefs.get("preferred_window"), windows)["window_label_ar"],
            "earliest_time_label_en": time_label_en(earliest) if earliest is not None else None,
            "earliest_time_label_ar": time_label_ar(earliest) if earliest is not None else None,
            "shipments_touched": touched}


# ============================================================
# WRITE: /shipment/delivery-code
# ============================================================

@app.post("/shipment/delivery-code")
async def resend_delivery_code(
    tracking_number: str = Body(..., embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    owner = await resolve_owner(caller_phone)
    s = await _require_shipment(tracking_number, owner)
    if not s.get("requires_delivery_code"):
        raise conflict("code_not_required", "This parcel doesn't need a delivery code — the driver can hand it over without one.")
    if not is_active(s):
        raise conflict("not_allowed_in_status", f"Shipment is {s.get('status')}.", status=s.get("status"))
    old = s.get("delivery_code")
    code = old
    while code == old:
        code = f"{secrets.randbelow(900000) + 100000}"
    now = _now()
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"},
                    {"delivery_code": code, "delivery_code_issued_at": now.isoformat()}, owner=owner)
    await add_events([{"tracking_number": s["tracking_number"], "status": s.get("status"),
                       "location_en": s.get("last_scan_location_en"), "location_ar": s.get("last_scan_location_ar"),
                       "note_en": "New delivery code sent to customer via WhatsApp; previous code cancelled",
                       "note_ar": "تم إرسال رمز تسليم جديد للعميل عبر واتساب وإلغاء الرمز السابق"}], owner)
    # The code itself is NOT logged — staff see that one was issued, not what it is.
    await log_agent_action(s["customer_id"], s["tracking_number"], "Delivery Code Reissued",
                           f"New delivery code issued for {s['tracking_number']}; previous code invalidated",
                           {"previous_invalidated": bool(old)}, owner=owner)
    return {"ok": True, "tracking_number": s["tracking_number"], "delivery_code": code,
            "issued_at": now.isoformat(), "issued_at_label_en": dt_label_en(now),
            "issued_at_label_ar": dt_label_ar(now), "previous_invalidated": bool(old)}


# ============================================================
# Payments: /payment/request + public pay page
# ============================================================

def _customs_payment_lines(customs: dict) -> list:
    return [
        {"label_en": "Customs duty", "label_ar": "الرسوم الجمركية", "amount_sar": money(customs.get("duty_sar"))},
        {"label_en": "VAT 15%", "label_ar": "ضريبة القيمة المضافة 15%", "amount_sar": money(customs.get("vat_sar"))},
        {"label_en": "Customs clearance fee", "label_ar": "رسوم التخليص الجمركي",
         "amount_sar": money(customs.get("clearance_fee_sar"))},
    ]


@app.post("/payment/request")
async def create_payment_link(
    tracking_number: str = Body(..., embed=True),
    purpose: str = Body(..., embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    """COD → prepay, or customs charges. Idempotent: an existing Pending
    request for the same shipment + purpose is returned, not duplicated."""
    owner = await resolve_owner(caller_phone)
    purpose_n = {"cod": "COD", "customs": "Customs"}.get((purpose or "").strip().lower())
    if not purpose_n:
        raise bad_request("validation_error", "purpose must be 'COD' or 'Customs'")
    s = await _require_shipment(tracking_number, owner)
    refs = await get_refs()
    store = refs["stores"].get(s.get("store_id")) or {}
    if purpose_n == "COD":
        amount = money(s.get("cod_amount_sar"))
        if s.get("cod_status") != "Pending" or amount <= 0:
            raise conflict("nothing_to_pay", "There is no cash-on-delivery amount pending on this parcel.",
                           cod_status=s.get("cod_status"))
        lines = [{"label_en": f"Cash on delivery — {store.get('name_en') or 'order'} {s.get('order_ref') or ''}".strip(),
                  "label_ar": f"الدفع عند الاستلام — {store.get('name_ar') or 'الطلب'} {s.get('order_ref') or ''}".strip(),
                  "amount_sar": amount}]
    else:
        customs = s.get("customs") or {}
        pay_req = next((r for r in customs.get("requirements") or [] if r.get("code") == "payment"), None)
        amount = money(customs.get("total_due_sar"))
        if not customs or amount <= 0 or (pay_req and pay_req.get("status") == "Paid"):
            raise conflict("nothing_to_pay", "There are no customs charges pending on this parcel.")
        lines = _customs_payment_lines(customs)
    existing = await sb_get("payment_requests", {"tracking_number": f"eq.{s['tracking_number']}",
                                                 "purpose": f"eq.{purpose_n}", "status": "eq.Pending"}, owner=owner)
    if existing:
        p = existing[0]
        return {"ok": True, "reused": True, "payment_id": p["payment_id"], "amount_sar": money(p.get("amount_sar")),
                "pay_url": _pay_url(p.get("pay_token")), "line_items": p.get("line_items") or lines,
                "status": p.get("status"), "purpose": purpose_n, "tracking_number": s["tracking_number"]}
    token = secrets.token_urlsafe(18)  # 24 url-safe chars
    row = await insert_with_next_id("payment_requests", "payment_id", "PAY-", 5, {
        "customer_id": s["customer_id"], "tracking_number": s["tracking_number"], "purpose": purpose_n,
        "amount_sar": amount, "status": "Pending", "pay_token": token, "line_items": lines,
        "created_at": _now().isoformat(),
    }, owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Payment Link Sent",
                           f"{purpose_n} payment link for SAR {amount:.2f} sent for {s['tracking_number']} ({row['payment_id']})",
                           {"payment_id": row["payment_id"], "purpose": purpose_n, "amount_sar": amount}, owner=owner)
    return {"ok": True, "reused": False, "payment_id": row["payment_id"], "amount_sar": amount,
            "pay_url": _pay_url(token), "line_items": lines, "status": "Pending", "purpose": purpose_n,
            "tracking_number": s["tracking_number"], "methods": PAY_METHODS}


PAY_METHODS = ["mada", "Apple Pay", "Credit Card", "STC Pay"]


async def _payment_by_token(token: str) -> dict:
    """Token-scoped lookup. The public pay page has no caller_phone — the
    unguessable 24-char token IS the scope — so this is the one deliberate
    unscoped read of a per-tenant table; every write after it is re-scoped to
    the row's own owner_id."""
    if not token or not re.fullmatch(r"[A-Za-z0-9_\-]{16,64}", token):
        raise not_found("payment_not_found", "Payment link not found")
    rows = await _sb_raw_get("payment_requests", {"pay_token": f"eq.{token}", "limit": "1"})
    if not rows:
        raise not_found("payment_not_found", "Payment link not found")
    return rows[0]


async def _apply_customs_clearance(s: dict, customs: dict, owner: str, source: str) -> dict:
    """If every customs requirement is satisfied, clear the parcel: status In
    Transit, ETA today + 2 delivery days, 'Released by customs' event."""
    reqs = customs.get("requirements") or []
    if not reqs or any(r.get("status") == "Missing" for r in reqs):
        return {"cleared": False, "eta_date": None}
    now = _now()
    eta = add_delivery_days(now.date(), 2)
    customs = {**customs, "status": "Cleared", "cleared_at": now.isoformat()}
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"},
                    {"customs": customs, "status": "In Transit", "hold_reason_en": None, "hold_reason_ar": None,
                     "eta_date": eta.isoformat(), "last_scan_at": now.isoformat(),
                     "last_scan_location_en": "Riyadh Customs Clearance", "last_scan_location_ar": "التخليص الجمركي بالرياض"},
                    owner=owner)
    await add_events([{"tracking_number": s["tracking_number"], "status": "In Transit", "source": "System",
                       "location_en": "Riyadh Customs Clearance", "location_ar": "التخليص الجمركي بالرياض",
                       "note_en": "Released by customs", "note_ar": "تم الإفراج الجمركي"}], owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Customs Cleared",
                           f"{s['tracking_number']} released by customs — back in transit, ETA {date_label_en(eta)}",
                           {"eta_date": eta.isoformat()}, owner=owner, source=source)
    return {"cleared": True, "eta_date": eta, "customs": customs}


@app.get("/public/pay/{token}")
async def public_pay_get(token: str):
    """Display payload for the portal's /pay/{token} page."""
    p = await _payment_by_token(token)
    owner = p["owner_id"]
    refs = await get_refs()
    s = await sb_get_one("shipments", {"tracking_number": f"eq.{p['tracking_number']}"}, owner=owner) or {}
    c = await sb_get_one("customers", {"customer_id": f"eq.{p['customer_id']}",
                                       "select": "full_name_en,full_name_ar"}, owner=owner) or {}
    store = refs["stores"].get(s.get("store_id")) or {}
    return {
        "payment_id": p["payment_id"], "status": p.get("status"), "purpose": p.get("purpose"),
        "amount_sar": money(p.get("amount_sar")), "currency": "SAR", "line_items": p.get("line_items") or [],
        "tracking_number": p.get("tracking_number"),
        "description_en": s.get("description_en"), "description_ar": s.get("description_ar"),
        "store_name_en": store.get("name_en"), "store_name_ar": store.get("name_ar"),
        "customer_first_name_en": (c.get("full_name_en") or "").split(" ")[0] or None,
        "customer_first_name_ar": (c.get("full_name_ar") or "").split(" ")[0] or None,
        "merchant_en": "Barq Express", "merchant_ar": "برق إكسبرس",
        "methods": PAY_METHODS, "method": p.get("method"), "paid_at": p.get("paid_at"),
        "paid_at_label_en": dt_label_en(parse_dt(p.get("paid_at"))),
        "paid_at_label_ar": dt_label_ar(parse_dt(p.get("paid_at"))),
    }


@app.post("/public/pay/{token}")
async def public_pay_post(token: str, method: str = Body("mada", embed=True)):
    """Mark paid (demo — no PSP) and apply side effects."""
    m = next((x for x in PAY_METHODS if x.lower() == (method or "").strip().lower()), None)
    if not m:
        raise bad_request("invalid_method", f"method must be one of {PAY_METHODS}")
    p = await _payment_by_token(token)
    owner = p["owner_id"]
    if p.get("status") == "Paid":
        raise conflict("already_paid", "This payment has already been made.", payment_id=p["payment_id"])
    if p.get("status") != "Pending":
        raise conflict("payment_cancelled", "This payment link is no longer valid.", payment_id=p["payment_id"])
    now = _now()
    await sb_update("payment_requests", {"payment_id": f"eq.{p['payment_id']}"},
                    {"status": "Paid", "method": m, "paid_at": now.isoformat()}, owner=owner)
    s = await sb_get_one("shipments", {"tracking_number": f"eq.{p['tracking_number']}"}, owner=owner) or {}
    amount = money(p.get("amount_sar"))
    result = {"cleared": False, "eta_date": None}
    if s and p.get("purpose") == "COD":
        await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"},
                        {"cod_status": "Paid Online",
                         "driver_notes": _append_note(s.get("driver_notes"), "Prepaid — do not collect cash")},
                        owner=owner)
        await add_events([{"tracking_number": s["tracking_number"], "status": s.get("status"), "source": "System",
                           "location_en": s.get("last_scan_location_en"), "location_ar": s.get("last_scan_location_ar"),
                           "note_en": f"COD SAR {amount:.2f} paid online ({m}) — cash collection removed",
                           "note_ar": f"تم دفع مبلغ الاستلام {amount:.2f} ريال إلكترونياً ({m}) — أُلغي التحصيل النقدي"}],
                         owner)
    elif s and p.get("purpose") == "Customs":
        customs = dict(s.get("customs") or {})
        customs["requirements"] = [
            {**r, "status": "Paid"} if r.get("code") == "payment" else r for r in (customs.get("requirements") or [])]
        await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, {"customs": customs}, owner=owner)
        await add_events([{"tracking_number": s["tracking_number"], "status": s.get("status"), "source": "System",
                           "location_en": s.get("last_scan_location_en"), "location_ar": s.get("last_scan_location_ar"),
                           "note_en": f"Customs charges SAR {amount:.2f} paid ({m})",
                           "note_ar": f"تم دفع الرسوم الجمركية {amount:.2f} ريال ({m})"}], owner)
        result = await _apply_customs_clearance(s, customs, owner, source="Pay Page")
    await log_agent_action(p.get("customer_id"), p.get("tracking_number"), "Payment Received",
                           f"{p.get('purpose')} payment {p['payment_id']} of SAR {amount:.2f} received via {m} (pay page)"
                           + ("; customs cleared" if result.get("cleared") else ""),
                           {"payment_id": p["payment_id"], "method": m, "amount_sar": amount,
                            "purpose": p.get("purpose"), "customs_cleared": result.get("cleared")},
                           owner=owner, source="Pay Page")
    eta = result.get("eta_date")
    return {"ok": True, "payment_id": p["payment_id"], "status": "Paid", "method": m, "amount_sar": amount,
            "paid_at": now.isoformat(), "paid_at_label_en": dt_label_en(now), "paid_at_label_ar": dt_label_ar(now),
            "purpose": p.get("purpose"), "customs_cleared": result.get("cleared"), **labels("eta_date", eta),
            "receipt_document": {"type": "payment_receipt", "ref": p["payment_id"]}}


# ============================================================
# WRITE: /customs/submit
# ============================================================

@app.post("/customs/submit")
async def submit_customs_item(
    tracking_number: str = Body(..., embed=True),
    item: str = Body(..., embed=True),
    national_id: Optional[str] = Body(None, embed=True),
    attachment_urls: Optional[Any] = Body(None, embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    """Customs requirement: national_id (validated, ONLY the last 4 digits are
    ever stored or returned) or purchase_invoice (≥1 attachment)."""
    owner = await resolve_owner(caller_phone)
    item_n = (item or "").strip().lower()
    if item_n not in ("national_id", "purchase_invoice"):
        raise bad_request("validation_error", "item must be 'national_id' or 'purchase_invoice'")
    nid = validate_national_id(national_id) if item_n == "national_id" else None
    urls = _as_list(attachment_urls)
    if item_n == "purchase_invoice" and not urls:
        raise bad_request("attachments_required", "Send a photo or PDF of the purchase invoice.")
    s = await _require_shipment(tracking_number, owner)
    customs = dict(s.get("customs") or {})
    if not customs:
        raise conflict("not_allowed_in_status", "This parcel is not held at customs.", status=s.get("status"))
    if customs.get("status") == "Cleared":
        raise conflict("not_allowed_in_status", "This parcel has already cleared customs.", status=s.get("status"))
    stored_docs = []
    if item_n == "national_id":
        customs["national_id_last4"] = nid[-4:]
        nid = None  # drop the full number as early as possible
    else:
        stored_docs = await store_attachments(urls, owner, s["tracking_number"])
        customs["documents"] = list(customs.get("documents") or []) + [
            {"code": "purchase_invoice", "path": d["path"], "stored": d["stored"],
             "content_type": d.get("content_type"), "received_at": _now().isoformat()} for d in stored_docs]
    customs["requirements"] = [{**r, "status": "Received"} if r.get("code") == item_n and r.get("status") == "Missing"
                               else r for r in (customs.get("requirements") or [])]
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, {"customs": customs}, owner=owner)
    label = "ID number" if item_n == "national_id" else "purchase invoice"
    await add_events([{"tracking_number": s["tracking_number"], "status": s.get("status"),
                       "location_en": s.get("last_scan_location_en"), "location_ar": s.get("last_scan_location_ar"),
                       "note_en": f"Customer sent {label} to customs",
                       "note_ar": "أرسل العميل " + ("رقم الهوية" if item_n == "national_id" else "فاتورة الشراء") + " للجمارك"}],
                     owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Customs Document Received",
                           f"{label.capitalize()} received for {s['tracking_number']}"
                           + (f" (ID ••••{customs.get('national_id_last4')})" if item_n == "national_id" else
                              f" ({sum(1 for d in stored_docs if d['stored'])}/{len(stored_docs)} file(s) stored)"),
                           {"item": item_n, "national_id_last4": customs.get("national_id_last4") if item_n == "national_id" else None,
                            "files": len(stored_docs)}, owner=owner)
    res = await _apply_customs_clearance(s, customs, owner, source="Agent")
    if res.get("cleared"):
        customs = res["customs"]
    remaining = [{k: r.get(k) for k in ("code", "label_en", "label_ar", "status")}
                 for r in (customs.get("requirements") or []) if r.get("status") == "Missing"]
    pending_pay = await sb_get("payment_requests", {"tracking_number": f"eq.{s['tracking_number']}",
                                                    "purpose": "eq.Customs", "status": "eq.Pending"}, owner=owner)
    return {"ok": True, "tracking_number": s["tracking_number"], "item": item_n,
            "national_id_masked": f"******{customs.get('national_id_last4')}" if customs.get("national_id_last4") else None,
            "files_stored": sum(1 for d in stored_docs if d["stored"]), "files_received": len(stored_docs),
            "remaining_requirements": remaining, "customs_status": customs.get("status"),
            "cleared": bool(res.get("cleared")), **labels("eta_date", res.get("eta_date")),
            "pending_payment": ({"payment_id": pending_pay[0]["payment_id"],
                                 "amount_sar": money(pending_pay[0].get("amount_sar")),
                                 "pay_url": _pay_url(pending_pay[0].get("pay_token"))} if pending_pay else None)}


# ============================================================
# WRITE: /case/open
# ============================================================

CASE_TYPES = {"damage claim": "Damage Claim", "damage": "Damage Claim",
              "non-delivery investigation": "Non-Delivery Investigation", "investigation": "Non-Delivery Investigation",
              "complaint": "Complaint"}
CASE_PREFIX = {"Damage Claim": "CLM-", "Non-Delivery Investigation": "INV-", "Complaint": "CMP-"}


def _due_at(d: date, hour: int = 17) -> datetime:
    return datetime(d.year, d.month, d.day, hour, 0, tzinfo=TZ)


@app.post("/case/open")
async def open_case(
    customer_id: str = Body(..., embed=True),
    case_type: str = Body(..., embed=True),
    description: Optional[str] = Body(None, embed=True),
    tracking_number: Optional[str] = Body(None, embed=True),
    attachment_urls: Optional[Any] = Body(None, embed=True),
    store_id: Optional[str] = Body(None, embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    owner = await resolve_owner(caller_phone)
    ct = CASE_TYPES.get((case_type or "").strip().lower())
    if not ct:
        raise bad_request("invalid_case_type", "case_type must be 'Damage Claim', 'Non-Delivery Investigation' or 'Complaint'")
    if ct != "Complaint" and not tracking_number:
        raise bad_request("validation_error", f"A {ct} needs the tracking_number")
    cust = await _require_customer(customer_id, owner)
    refs = await get_refs()
    rules = refs["rules"]
    now = _now()
    today = now.date()
    s = await _require_shipment(tracking_number, owner) if tracking_number else None
    if s and s.get("customer_id") != cust["customer_id"]:
        raise not_found("shipment_not_found", f"{s['tracking_number']} is not on this customer's account")
    # Duplicate check first: a second "it's broken" message must return the
    # existing claim, not a window error or a second claim.
    if s:
        dup = await sb_get("cases", {"tracking_number": f"eq.{s['tracking_number']}", "case_type": f"eq.{ct}"}, owner=owner)
        live = [c for c in dup if c.get("status") in OPEN_CASE_STATUSES]
        if live:
            c = live[0]
            raise conflict("case_exists", f"{c['case_id']} is already open for this parcel.", case_id=c["case_id"],
                           status=c.get("status"), **labels("due", parse_date(c.get("due_at"))))
    urls = _as_list(attachment_urls)
    extra, shipment_upd = {}, {}
    store = refs["stores"].get((s or {}).get("store_id") or (store_id or "").upper()) or {}
    if ct == "Damage Claim":
        dd = delivered_date(s)
        if not dd:
            raise conflict("not_allowed_in_status", "A damage claim can only be opened once the parcel is delivered.",
                           status=s.get("status"))
        window = int(rule(rules, "damage_claim_window_days"))
        days = (today - dd).days
        if days > window:
            raise conflict("claim_window_expired",
                           f"Damage claims must be opened within {window} days of delivery; this was delivered "
                           f"{days} days ago.", **labels("delivered_on", dd), days_since_delivery=days,
                           window_days=window, **labels("claim_deadline", dd + timedelta(days=window)))
        if not urls:
            raise bad_request("photos_required", "Photos of the item and the box are needed to open a damage claim.")
        due = _due_at(add_working_days(today, 5))
        assigned_en, assigned_ar = "Barq Claims Team", "فريق المطالبات في برق"
        extra["store_notified"] = True
    elif ct == "Non-Delivery Investigation":
        if not is_delivered(s):
            raise conflict("not_allowed_in_status", "An investigation is for parcels marked delivered.",
                           status=s.get("status"))
        city = shipment_city(s, refs["districts"]) or "Riyadh"
        city_ar = (next((d for d in refs["districts"].values() if d.get("city_en") == city), {}) or {}).get("city_ar") or "الرياض"
        due = now + timedelta(hours=int(rule(rules, "investigation_callback_hours")))
        assigned_en, assigned_ar = f"Barq {city} Centre — Investigations", f"مركز برق {city_ar} — التحقيقات"
        shipment_upd["pod_locked"] = True
        extra["store_notified"] = False
    else:  # Complaint
        due = _due_at(add_working_days(today, int(rule(rules, "complaint_sla_working_days"))))
        centre = refs["centres"].get((s or {}).get("current_centre_id")) or {}
        if centre:
            assigned_en, assigned_ar = f"{centre.get('name_en')} manager", f"مدير {centre.get('name_ar')}"
        else:
            assigned_en, assigned_ar = "Barq Customer Care manager", "مدير خدمة عملاء برق"
        extra["regulator_phone"] = str(rule(rules, "tga_phone"))
        extra["store_notified"] = False
    case_id_prefix = CASE_PREFIX[ct]
    # Media first (the case row stores the paths). Keyed by shipment — the
    # case id doesn't exist until the insert succeeds.
    stored = await store_attachments(urls, owner, (s or {}).get("tracking_number") or cust["customer_id"]) if urls else []
    desc = (description or "").strip() or {"Damage Claim": "Item arrived damaged",
                                           "Non-Delivery Investigation": "Marked delivered but not received",
                                           "Complaint": "Customer complaint"}[ct]
    row = await insert_with_next_id("cases", "case_id", case_id_prefix, 5, {
        "case_type": ct, "tracking_number": (s or {}).get("tracking_number"), "customer_id": cust["customer_id"],
        "status": "Open", "description_en": desc, "description_ar": desc if re.search(r"[؀-ۿ]", desc) else None,
        "opened_at": now.isoformat(), "due_at": due.isoformat(), "assigned_to_en": assigned_en,
        "assigned_to_ar": assigned_ar,
        "attachments": [{"path": a["path"], "stored": a["stored"], "content_type": a.get("content_type")} for a in stored],
        "store_notified": extra.get("store_notified", False), "regulator_phone": extra.get("regulator_phone"),
    }, owner)
    case_id = row["case_id"]
    if s:
        if shipment_upd:
            await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, shipment_upd, owner=owner)
        note = {"Damage Claim": ("Damage claim {} opened; store notified", "فُتحت مطالبة تلف {} وتم إبلاغ المتجر"),
                "Non-Delivery Investigation": ("Investigation {} opened — delivery record locked",
                                               "فُتح تحقيق {} — تم قفل سجل التسليم"),
                "Complaint": ("Complaint {} opened", "فُتحت شكوى {}")}[ct]
        await add_events([{"tracking_number": s["tracking_number"], "status": s.get("status"),
                           "location_en": s.get("last_scan_location_en"), "location_ar": s.get("last_scan_location_ar"),
                           "note_en": note[0].format(case_id), "note_ar": note[1].format(case_id)}], owner)
    due_d = due.date()
    await log_agent_action(cust["customer_id"], (s or {}).get("tracking_number"), f"{ct} Opened",
                           f"{ct} {case_id} opened" + (f" for {s['tracking_number']}" if s else "")
                           + f"; assigned to {assigned_en}; due {date_label_en(due_d)}"
                           + (f"; {sum(1 for a in stored if a['stored'])} photo(s) stored" if stored else "")
                           + ("; delivery record locked" if shipment_upd.get("pod_locked") else ""),
                           {"case_id": case_id, "case_type": ct, "files": len(stored), "store_id": store.get("store_id")},
                           owner=owner)
    return {"ok": True, "case_id": case_id, "case_type": ct, "status": "Open",
            "tracking_number": (s or {}).get("tracking_number"),
            "due_at": due.isoformat(), "due_label_en": date_label_en(due_d), "due_label_ar": date_label_ar(due_d),
            "due_time_label_en": dt_label_en(due) if ct == "Non-Delivery Investigation" else None,
            "due_time_label_ar": dt_label_ar(due) if ct == "Non-Delivery Investigation" else None,
            "callback_within_hours": int(rule(rules, "investigation_callback_hours")) if ct == "Non-Delivery Investigation" else None,
            "sla_working_days": int(rule(rules, "complaint_sla_working_days")) if ct == "Complaint" else
            (5 if ct == "Damage Claim" else None),
            "assigned_to_en": assigned_en, "assigned_to_ar": assigned_ar,
            "regulator_phone": extra.get("regulator_phone"), "store_notified": extra.get("store_notified", False),
            "store_name_en": store.get("name_en"), "store_name_ar": store.get("name_ar"),
            "pod_locked": bool(shipment_upd.get("pod_locked")),
            "files_received": len(stored), "files_stored": sum(1 for a in stored if a["stored"])}


# ============================================================
# WRITE: /return/create
# ============================================================

RETURN_REASONS = {"wrong item": "Wrong Item", "wrong size": "Wrong Size", "damaged": "Damaged",
                  "changed mind": "Changed Mind", "other": "Other"}


@app.post("/return/create")
async def create_return(
    tracking_number: str = Body(..., embed=True),
    reason_code: str = Body(..., embed=True),
    reason_detail: Optional[str] = Body(None, embed=True),
    pickup_date: Optional[str] = Body(None, embed=True),
    window_code: Optional[str] = Body(None, embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    owner = await resolve_owner(caller_phone)
    rc = RETURN_REASONS.get((reason_code or "").strip().lower().replace("_", " "))
    if not rc:
        raise bad_request("invalid_reason_code", f"reason_code must be one of {sorted(RETURN_REASONS.values())}")
    s, refs = await asyncio.gather(_require_shipment(tracking_number, owner), get_refs())
    today = _today()
    dd = delivered_date(s)
    if not dd:
        raise conflict("not_allowed_in_status", "Returns can be booked once the parcel is delivered.", status=s.get("status"))
    existing = await sb_get("returns", {"tracking_number": f"eq.{s['tracking_number']}"}, owner=owner)
    live = [r for r in existing if r.get("status") != "Returned"]
    if live:
        r = live[0]
        raise conflict("return_exists", f"Return {r['return_id']} is already booked for this parcel.",
                       return_id=r["return_id"], status=r.get("status"),
                       **labels("pickup_date", parse_date(r.get("pickup_date"))))
    store = refs["stores"].get(s.get("store_id")) or {}
    window = int(store.get("return_window_days") or rule(refs["rules"], "default_return_window_days"))
    days = (today - dd).days
    if days > window:
        raise conflict("return_window_expired",
                       f"{store.get('name_en') or 'The store'} accepts returns within {window} days of delivery; "
                       f"this was delivered {days} days ago.", **labels("delivered_on", dd),
                       days_since_delivery=days, window_days=window,
                       **labels("return_deadline", dd + timedelta(days=window)))
    windows = refs["windows"]
    code = (window_code or "W1").strip().upper()
    if code not in windows:
        raise bad_request("validation_error", "window_code must be W1–W4")
    d = parse_date(pickup_date) if pickup_date else next_delivery_day(today)
    if not d or d <= today or not is_delivery_day(d) or (d - today).days > 14:
        raise conflict("slot_unavailable", "Pickups can be booked on delivery days (not Friday), from tomorrow up to 14 days ahead.",
                       **labels("earliest_pickup_date", next_delivery_day(today)))
    row = await insert_with_next_id("returns", "return_id", "RET-", 5, {
        "tracking_number": s["tracking_number"], "customer_id": s["customer_id"], "reason_code": rc,
        "reason_detail": (reason_detail or "").strip() or None, "status": "Pickup Booked",
        "pickup_date": d.isoformat(), "pickup_window": code, "created_at": _now().isoformat(),
    }, owner)
    rid = row["return_id"]
    await sb_update("returns", {"return_id": f"eq.{rid}"}, {"label_ref": rid}, owner=owner)
    await sb_update("shipments", {"tracking_number": f"eq.{s['tracking_number']}"}, {"status": "Return Requested"}, owner=owner)
    wl = window_labels(code, windows)
    await add_events([{"tracking_number": s["tracking_number"], "status": "Return Requested",
                       "location_en": s.get("delivery_address_en"), "location_ar": s.get("delivery_address_ar"),
                       "note_en": f"Return {rid} ({rc}) — pickup booked {date_label_en(d)}, {wl['window_label_en']}",
                       "note_ar": f"إرجاع {rid} — تم حجز الاستلام {date_label_ar(d)}، {wl['window_label_ar']}"}], owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Return Booked",
                           f"Return {rid} for {s['tracking_number']} ({rc}"
                           + (f": {reason_detail.strip()}" if reason_detail else "")
                           + f"); pickup {date_label_en(d)} {code}; {store.get('name_en') or 'store'} can see it",
                           {"return_id": rid, "reason_code": rc, "pickup_date": d.isoformat(), "window": code},
                           owner=owner)
    return {"ok": True, "return_id": rid, "tracking_number": s["tracking_number"], "reason_code": rc,
            "status": "Pickup Booked", **labels("pickup_date", d), "window_code": code, **wl,
            "label_ref": rid, "label_document_type": "return_label",
            "store_name_en": store.get("name_en"), "store_name_ar": store.get("name_ar")}


# ============================================================
# WRITE: /tax-invoice
# ============================================================

SELLER_NAME_EN = "Barq Express Logistics Co."
SELLER_NAME_AR = "شركة برق إكسبرس للخدمات اللوجستية"
SELLER_VAT = "300000000000003"


@app.post("/tax-invoice")
async def issue_tax_invoice(
    tracking_number: str = Body(..., embed=True),
    company_name_en: str = Body(..., embed=True),
    vat_number: str = Body(..., embed=True),
    company_name_ar: Optional[str] = Body(None, embed=True),
    save_to_profile: bool = Body(True, embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    owner = await resolve_owner(caller_phone)
    vat = re.sub(r"\s", "", vat_number or "")
    if not is_valid_vat(vat):
        raise bad_request("invalid_vat_number", "A Saudi VAT number is 15 digits and starts and ends with 3.")
    name_en = (company_name_en or "").strip()
    if len(name_en) < 2:
        raise bad_request("validation_error", "company_name_en is required")
    s = await _require_shipment(tracking_number, owner)
    fee = money(s.get("shipping_fee_sar"))
    if s.get("shipping_paid_by") != "Customer" or fee <= 0:
        raise conflict("no_customer_charge",
                       "The store paid the shipping on this parcel, so Barq has no charge to invoice you for.",
                       shipping_paid_by=s.get("shipping_paid_by"))
    existing = await sb_get_one("tax_invoices", {"tracking_number": f"eq.{s['tracking_number']}"}, owner=owner)
    if existing:
        return {"ok": True, "reused": True, "tax_invoice_id": existing["tax_invoice_id"],
                "subtotal_sar": money(existing.get("subtotal_sar")), "vat_sar": money(existing.get("vat_sar")),
                "total_sar": money(existing.get("total_sar")), "company_name_en": existing.get("company_name_en"),
                "vat_number": existing.get("vat_number"), "document_type": "tax_invoice", "saved_to_profile": False}
    refs = await get_refs()
    sub, vat_amt = vat_split(fee, float(rule(refs["rules"], "customs_vat_percent")))
    now = _now()
    row = await insert_with_next_id("tax_invoices", "tax_invoice_id", "TAX-", 5, {
        "customer_id": s["customer_id"], "tracking_number": s["tracking_number"], "company_name_en": name_en,
        "company_name_ar": (company_name_ar or "").strip() or None, "vat_number": vat,
        "subtotal_sar": sub, "vat_sar": vat_amt, "total_sar": fee, "issued_at": now.isoformat(),
    }, owner)
    if save_to_profile:
        await sb_update("customers", {"customer_id": f"eq.{s['customer_id']}"},
                        {"company_name_en": name_en, "company_name_ar": (company_name_ar or "").strip() or None,
                         "vat_number": vat, "customer_type": "Business"}, owner=owner)
    await log_agent_action(s["customer_id"], s["tracking_number"], "Tax Invoice Issued",
                           f"Tax invoice {row['tax_invoice_id']} for {name_en} (VAT {vat}): SAR {sub:.2f} + VAT "
                           f"{vat_amt:.2f} = {fee:.2f}" + ("; company saved to profile" if save_to_profile else ""),
                           {"tax_invoice_id": row["tax_invoice_id"], "total_sar": fee, "saved_to_profile": save_to_profile},
                           owner=owner)
    return {"ok": True, "reused": False, "tax_invoice_id": row["tax_invoice_id"], "subtotal_sar": sub,
            "vat_sar": vat_amt, "total_sar": fee, "company_name_en": name_en, "vat_number": vat,
            "issued_at_label_en": dt_label_en(now), "issued_at_label_ar": dt_label_ar(now),
            "document_type": "tax_invoice", "saved_to_profile": bool(save_to_profile)}


# ============================================================
# WRITE: /store-alert
# ============================================================

def match_store(name: str, stores: dict) -> Optional[dict]:
    best, best_s = None, 0.0
    for st in stores.values():
        sc = max(_fuzzy_score(name, st.get("name_en") or ""), _fuzzy_score(name, st.get("name_ar") or ""),
                 _fuzzy_score(st.get("name_en") or "", name), _fuzzy_score(st.get("name_ar") or "", name))
        if sc > best_s:
            best, best_s = st, sc
    return best if best_s >= 0.75 else None


@app.post("/store-alert")
async def create_store_alert(
    customer_id: str = Body(..., embed=True),
    store_name: Optional[str] = Body(None, embed=True),
    store_id: Optional[str] = Body(None, embed=True),
    order_ref: Optional[str] = Body(None, embed=True),
    caller_phone: Optional[str] = Body(None, embed=True),
):
    """L-20: the store hasn't handed the parcel over. Confirms there's no
    shipment, then sets an alert that fires when one arrives."""
    owner = await resolve_owner(caller_phone)
    refs = await get_refs()
    rules = refs["rules"]
    store = refs["stores"].get((store_id or "").strip().upper()) if store_id else None
    if not store and store_name:
        store = match_store(store_name, refs["stores"])
    if not store:
        raise not_found("store_not_found", f"No merchant matching '{store_name or store_id}' ships with Barq.")
    cust = await _require_customer(customer_id, owner)
    shipments = await sb_get("shipments", {"customer_id": f"eq.{cust['customer_id']}",
                                           "store_id": f"eq.{store['store_id']}"}, owner=owner)
    live = [s for s in shipments if is_active(s)]
    if live:
        s = live[0]
        raise conflict("shipment_exists", f"There is a shipment from {store.get('name_en')} already with Barq.",
                       tracking_number=s["tracking_number"], status=s.get("status"),
                       store_name_en=store.get("name_en"), store_name_ar=store.get("name_ar"))
    rules_out = {"store_legal_delivery_days": rule(rules, "store_legal_delivery_days"),
                 "moc_phone": str(rule(rules, "moc_phone"))}
    alerts = await sb_get("store_alerts", {"customer_id": f"eq.{cust['customer_id']}",
                                           "store_id": f"eq.{store['store_id']}", "status": "eq.Active"}, owner=owner)
    if alerts:
        a = alerts[0]
        return {"ok": True, "reused": True, "alert_id": a["alert_id"], "store_id": store["store_id"],
                "store_name_en": store.get("name_en"), "store_name_ar": store.get("name_ar"),
                "found_shipment": False, "rules": rules_out}
    row = await insert_with_next_id("store_alerts", "alert_id", "ALR-", 5, {
        "customer_id": cust["customer_id"], "store_id": store["store_id"],
        "order_ref": (order_ref or "").strip() or None, "status": "Active", "created_at": _now().isoformat(),
        "note_en": f"Notify customer via WhatsApp when {store.get('name_en')} hands over a parcel"
                   + (f" (order {order_ref})" if order_ref else ""),
    }, owner)
    await log_agent_action(cust["customer_id"], None, "Store Alert Set",
                           f"No shipment from {store.get('name_en')} yet — alert {row['alert_id']} set to message "
                           f"{cust.get('full_name_en')} when the store hands it over",
                           {"alert_id": row["alert_id"], "store_id": store["store_id"], "order_ref": order_ref}, owner=owner)
    return {"ok": True, "reused": False, "alert_id": row["alert_id"], "store_id": store["store_id"],
            "store_name_en": store.get("name_en"), "store_name_ar": store.get("name_ar"),
            "store_contact_phone": store.get("contact_phone"), "store_website": store.get("website"),
            "found_shipment": False, "rules": rules_out}


# ============================================================
# PUBLIC: /public/track/{token}
# ============================================================

@app.get("/public/track/{token}")
async def public_track(token: str):
    """Live tracking page payload (Lovable /track/{token}). The tracking
    token is the scope; no personal data beyond what the page shows."""
    if not token or not re.fullmatch(r"[A-Za-z0-9_\-]{8,64}", token):
        raise not_found("tracking_not_found", "Tracking link not found")
    rows = await _sb_raw_get("shipments", {"tracking_token": f"eq.{token}", "limit": "1"})
    if not rows:
        raise not_found("tracking_not_found", "Tracking link not found")
    s = rows[0]
    owner = s["owner_id"]
    refs, events = await asyncio.gather(
        get_refs(),
        sb_get("shipment_events", {"tracking_number": f"eq.{s['tracking_number']}", "order": "event_at.desc"}, owner=owner),
    )
    now = _now()
    store = refs["stores"].get(s.get("store_id")) or {}
    dlat, dlng = shipment_coords(s, refs["districts"])
    out = {
        "tracking_number": s["tracking_number"], "status": s.get("status"),
        "store_name_en": store.get("name_en"), "store_name_ar": store.get("name_ar"),
        "description_en": s.get("description_en"), "description_ar": s.get("description_ar"),
        "service_type": s.get("service_type"),
        "delivery_lat": dlat, "delivery_lng": dlng,
        "delivery_area_en": (refs["districts"].get(s.get("delivery_district_id")) or {}).get("name_en"),
        "delivery_area_ar": (refs["districts"].get(s.get("delivery_district_id")) or {}).get("name_ar"),
        **labels("scheduled_date", parse_date(s.get("scheduled_date"))),
        **window_labels(s.get("scheduled_window"), refs["windows"]),
        "eta": None, "driver": None, "route_stop": s.get("route_stop"), "route_total": s.get("route_total"),
        "driver_lat": None, "driver_lng": None,
        "delivered_at": s.get("delivered_at"),
        "delivered_at_label_en": dt_label_en(parse_dt(s.get("delivered_at"))),
        "delivered_at_label_ar": dt_label_ar(parse_dt(s.get("delivered_at"))),
        "events": [
            {k: e.get(k) for k in ("event_at", "status", "location_en", "location_ar", "note_en", "note_ar")}
            | {"event_at_label_en": dt_label_en(parse_dt(e.get("event_at"))),
               "event_at_label_ar": dt_label_ar(parse_dt(e.get("event_at")))}
            for e in events
        ],
        "server_time": now.isoformat(timespec="seconds"),
    }
    if s.get("status") == "Out for Delivery":
        out["eta"] = compute_live_eta(s.get("route_stop"), s.get("route_total"), now)
        drv = refs["drivers"].get(s.get("driver_id")) or {}
        out["driver"] = {"name_en": drv.get("name_en"), "name_ar": drv.get("name_ar"),
                         "vehicle_plate": drv.get("vehicle_plate")}
        centre = refs["centres"].get(s.get("current_centre_id")) or refs["centres"].get(drv.get("centre_id")) or {}
        clat = float(centre.get("lat") or dlat)
        clng = float(centre.get("lng") or dlng)
        f = driver_progress_fraction(s.get("route_stop"), now)
        out["driver_lat"] = round(clat + (dlat - clat) * f, 6)
        out["driver_lng"] = round(clng + (dlng - clng) * f, 6)
        out["progress"] = f
        out["centre_lat"], out["centre_lng"] = clat, clng
    return out


# ============================================================
# Documents: branded PDFs (reportlab) → Storage → signed URL
# ============================================================

NAVY = "#0B1F3A"
YELLOW = "#F5B700"
_FONTS_READY: dict = {}


def _init_fonts() -> dict:
    """Register Arabic-capable fonts once. Amiri (bundled, OFL) renders both
    scripts; falls back to system DejaVuSans; last resort Helvetica with an
    EN-only Arabic document (reported in the response)."""
    if _FONTS_READY:
        return _FONTS_READY
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    info = {"regular": "Helvetica", "bold": "Helvetica-Bold", "arabic": None, "arabic_bold": None, "shaping": False}
    candidates = [(os.path.join(FONTS_DIR, "Amiri-Regular.ttf"), os.path.join(FONTS_DIR, "Amiri-Bold.ttf")),
                  ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")]
    for reg, bold in candidates:
        if os.path.exists(reg):
            try:
                pdfmetrics.registerFont(TTFont("BarqAR", reg))
                pdfmetrics.registerFont(TTFont("BarqAR-Bold", bold if os.path.exists(bold) else reg))
                info["arabic"], info["arabic_bold"] = "BarqAR", "BarqAR-Bold"
                break
            except Exception as e:
                print(f"[pdf] font {reg} failed: {e}")
    try:
        import arabic_reshaper  # noqa: F401
        from bidi.algorithm import get_display  # noqa: F401
        info["shaping"] = bool(info["arabic"])
    except Exception as e:
        print(f"[pdf] Arabic shaping unavailable: {e}")
    _FONTS_READY.update(info)
    return _FONTS_READY


_AR_RE = re.compile(r"[؀-ۿ]")


def _shape(text: str) -> str:
    """Logical Arabic → visual glyph order for reportlab (which has no bidi)."""
    if not text or not _AR_RE.search(text):
        return text or ""
    import arabic_reshaper
    from bidi.algorithm import get_display
    return get_display(arabic_reshaper.reshape(text))


class BarqPdf:
    """Minimal branded page builder. RTL-aware: in `ar` every row is mirrored
    (labels on the right) and text is shaped. Everything is drawn with the
    canvas API — no platypus — so layout is predictable on one page."""

    W, H = 595.27, 841.89  # A4 points
    M = 40

    def __init__(self, lang: str, title_en: str, title_ar: str, ref: str):
        from reportlab.pdfgen import canvas
        self.fonts = _init_fonts()
        self.ar = lang == "ar" and self.fonts["shaping"]
        self.lang_requested = lang
        self.buf = io.BytesIO()
        self.c = canvas.Canvas(self.buf, pagesize=(self.W, self.H))
        self.c.setTitle(f"Barq Express — {title_en} — {ref}")
        self.c.setAuthor("Barq Express (demo)")
        self.title_en, self.title_ar, self.ref = title_en, title_ar, ref
        self.page = 0
        self._new_page()

    # -- fonts / text ------------------------------------------------------
    def _font(self, bold=False, arabic=None):
        use_ar = self.ar if arabic is None else arabic
        if use_ar and self.fonts["arabic"]:
            return self.fonts["arabic_bold"] if bold else self.fonts["arabic"]
        return self.fonts["bold"] if bold else self.fonts["regular"]

    def t(self, en: str, ar: str) -> str:
        return ar if self.ar else en

    def _txt(self, s) -> str:
        s = "" if s is None else str(s)
        return _shape(s) if self.ar or _AR_RE.search(s) else s

    def _draw(self, x, y, s, size=10, bold=False, color=NAVY, align="left"):
        s = "" if s is None else str(s)
        needs_ar = bool(_AR_RE.search(s))
        font = self._font(bold, arabic=(self.ar or (needs_ar and bool(self.fonts["arabic"]))))
        self.c.setFont(font, size)
        self.c.setFillColor(color)
        out = self._txt(s)
        if align == "right":
            self.c.drawRightString(x, y, out)
        elif align == "center":
            self.c.drawCentredString(x, y, out)
        else:
            self.c.drawString(x, y, out)

    def _width(self, s, size=10, bold=False) -> float:
        from reportlab.pdfbase.pdfmetrics import stringWidth
        s = "" if s is None else str(s)
        needs_ar = bool(_AR_RE.search(s))
        return stringWidth(self._txt(s), self._font(bold, arabic=(self.ar or (needs_ar and bool(self.fonts["arabic"])))), size)

    def _wrap(self, s, width, size=10, bold=False) -> list:
        """Word-wrap in LOGICAL order, shape per line afterwards."""
        words = str(s or "").split()
        lines, cur = [], ""
        for w in words:
            cand = f"{cur} {w}".strip()
            if self._width(cand, size, bold) <= width or not cur:
                cur = cand
            else:
                lines.append(cur)
                cur = w
        if cur:
            lines.append(cur)
        return lines or [""]

    # -- layout primitives ---------------------------------------------------
    def _x(self, x_ltr: float) -> float:
        return self.W - x_ltr if self.ar else x_ltr

    def _align(self, a="left"):
        if not self.ar:
            return a
        return {"left": "right", "right": "left"}.get(a, a)

    def _new_page(self):
        if self.page:
            self._footer()
            self.c.showPage()
        self.page += 1
        c = self.c
        c.setFillColor(NAVY)
        c.rect(0, self.H - 78, self.W, 78, stroke=0, fill=1)
        c.setFillColor(YELLOW)
        c.rect(0, self.H - 82, self.W, 4, stroke=0, fill=1)
        # Lightning mark
        p = c.beginPath()
        bx, by = (self.W - self.M - 22, self.H - 60) if self.ar else (self.M, self.H - 60)
        p.moveTo(bx + 12, by + 40); p.lineTo(bx + 2, by + 18); p.lineTo(bx + 10, by + 18)
        p.lineTo(bx + 6, by); p.lineTo(bx + 20, by + 24); p.lineTo(bx + 12, by + 24); p.close()
        c.setFillColor(YELLOW)
        c.drawPath(p, stroke=0, fill=1)
        self._draw(self._x(self.M + 30), self.H - 42, "Barq Express", 18, True, "#FFFFFF", self._align("left"))
        # Separator and Arabic name drawn separately: bidi would otherwise move
        # the "|" to the far side of the Arabic run.
        bw = self._width("Barq Express", 18, True)
        self._draw(self._x(self.M + 30 + bw + 6), self.H - 42, "|", 16, False, "#C9D2E0", self._align("left"))
        self._draw(self._x(self.M + 30 + bw + 16), self.H - 42, "برق إكسبرس", 16, True, YELLOW, self._align("left"))
        self._draw(self._x(self.M + 30), self.H - 62, self.t("Delivery & logistics", "خدمات التوصيل والشحن"), 9, False,
                   "#C9D2E0", self._align("left"))
        self._draw(self._x(self.W - self.M), self.H - 40, self.t(self.title_en, self.title_ar), 13, True, "#FFFFFF",
                   self._align("right"))
        self._draw(self._x(self.W - self.M), self.H - 58, self.ref, 10, False, YELLOW, self._align("right"))
        self.y = self.H - 110

    def _footer(self):
        c = self.c
        c.setStrokeColor(YELLOW)
        c.setLineWidth(1.5)
        c.line(self.M, 46, self.W - self.M, 46)
        foot = self.t("Barq Express Logistics Co. · CR 1010000000 (demo) · This is a demonstration document",
                      "شركة برق إكسبرس للخدمات اللوجستية · س.ت 1010000000 (تجريبي) · هذه وثيقة تجريبية")
        self._draw(self.W / 2, 32, foot, 8, False, "#5A6B85", "center")
        self._draw(self.W / 2, 20, self.t(f"Page {self.page}", f"صفحة {self.page}"), 7, False, "#8A98AE", "center")

    def need(self, h: float):
        if self.y - h < 70:
            self._new_page()

    def section(self, en: str, ar: str):
        self.need(34)
        self.y -= 8
        self.c.setFillColor("#EEF2F8")
        self.c.rect(self.M, self.y - 6, self.W - 2 * self.M, 20, stroke=0, fill=1)
        self.c.setFillColor(YELLOW)
        self.c.rect(self.W - self.M - 4 if self.ar else self.M, self.y - 6, 4, 20, stroke=0, fill=1)
        self._draw(self._x(self.M + 12), self.y, self.t(en, ar), 11, True, NAVY, self._align("left"))
        self.y -= 24

    def kv(self, en: str, ar: str, value, bold_value=False):
        label = self.t(en, ar)
        val = "—" if value in (None, "") else str(value)
        lines = self._wrap(val, self.W - 2 * self.M - 170, 10, bold_value)
        llines = self._wrap(label, 158, 9.5)
        n = max(len(lines), len(llines))
        self.need(14 * n + 2)
        for i, ln in enumerate(llines):
            self._draw(self._x(self.M + 4), self.y - 14 * i, ln, 9.5, False, "#5A6B85", self._align("left"))
        for i, ln in enumerate(lines):
            self._draw(self._x(self.M + 170), self.y - 14 * i, ln, 10, bold_value, NAVY, self._align("left"))
        self.y -= 14 * n + 2

    def para(self, text: str, size=10, color=NAVY):
        for ln in self._wrap(text, self.W - 2 * self.M - 8, size):
            self.need(size + 5)
            self._draw(self._x(self.M + 4), self.y, ln, size, False, color, self._align("left"))
            self.y -= size + 5

    def table(self, headers: list, rows: list, widths: list, total_row: Optional[list] = None):
        """headers/rows are lists of already-localised strings; the last column
        is right-aligned (amounts)."""
        x0 = self.M
        self.need(22)
        self.c.setFillColor(NAVY)
        self.c.rect(x0, self.y - 6, sum(widths), 20, stroke=0, fill=1)
        self._row(headers, widths, "#FFFFFF", True)
        self.y -= 20
        for i, r in enumerate(rows + ([total_row] if total_row else [])):
            is_total = total_row is not None and i == len(rows)
            self.need(18)
            if is_total:
                self.c.setFillColor(YELLOW)
                self.c.rect(x0, self.y - 6, sum(widths), 18, stroke=0, fill=1)
            elif i % 2:
                self.c.setFillColor("#F5F7FB")
                self.c.rect(x0, self.y - 6, sum(widths), 18, stroke=0, fill=1)
            self._row(r, widths, NAVY, is_total)
            self.y -= 18
        self.y -= 6

    def _row(self, cells, widths, color, bold):
        x = self.M
        for j, (cell, w) in enumerate(zip(cells, widths)):
            last = j == len(cells) - 1
            if last:
                self._draw(self._x(x + w - 6), self.y, cell, 9.5, bold, color, self._align("right"))
            else:
                self._draw(self._x(x + 6), self.y, cell, 9.5, bold, color, self._align("left"))
            x += w

    def image(self, data: bytes, max_w: float, max_h: float, caption: Optional[str] = None):
        from reportlab.lib.utils import ImageReader
        try:
            img = ImageReader(io.BytesIO(data))
            iw, ih = img.getSize()
        except Exception as e:
            self.para(self.t(f"(image could not be read: {e})", "(تعذر قراءة الصورة)"), 8, "#8A98AE")
            return
        scale = min(max_w / iw, max_h / ih, 1.0)
        w, h = iw * scale, ih * scale
        self.need(h + (16 if caption else 6))
        x = (self.W - self.M - w) if self.ar else self.M
        self.c.drawImage(img, x, self.y - h + 8, w, h, preserveAspectRatio=True, mask="auto")
        self.c.setStrokeColor("#C9D2E0")
        self.c.rect(x, self.y - h + 8, w, h, stroke=1, fill=0)
        self.y -= h + 4
        if caption:
            self._draw(self._x(self.M), self.y, caption, 8, False, "#5A6B85", self._align("left"))
            self.y -= 12

    def qr(self, payload: str, size: float = 110, caption: Optional[str] = None):
        import qrcode
        from reportlab.lib.utils import ImageReader
        img = qrcode.make(payload)
        b = io.BytesIO()
        img.save(b, format="PNG")
        b.seek(0)
        self.need(size + 20)
        x = self.M if self.ar else self.W - self.M - size
        self.c.drawImage(ImageReader(b), x, self.y - size + 8, size, size)
        if caption:
            self._draw(x + size / 2, self.y - size - 2, caption, 7, False, "#5A6B85", "center")

    def barcode(self, value: str, height: float = 50):
        from reportlab.graphics.barcode import code128
        bc = code128.Code128(value, barHeight=height, barWidth=1.4, humanReadable=True)
        self.need(height + 30)
        self.c.setFillColor("#000000")
        self.c.setStrokeColor("#000000")
        x = (self.W - bc.width) / 2
        bc.drawOn(self.c, x, self.y - height)
        self.y -= height + 26

    def stamp(self, en: str, ar: str, color: str = "#1E8E3E"):
        self.need(30)
        text = self.t(en, ar)
        w = self._width(text, 12, True) + 24
        x = (self.W - self.M - w) if not self.ar else self.M
        self.c.setStrokeColor(color)
        self.c.setLineWidth(2)
        self.c.roundRect(x, self.y - 8, w, 24, 6, stroke=1, fill=0)
        self._draw(x + w / 2, self.y, text, 12, True, color, "center")

    def finish(self) -> bytes:
        self._footer()
        self.c.save()
        return self.buf.getvalue()


def zatca_tlv_base64(seller: str, vat_no: str, timestamp_iso: str, total: str, vat: str) -> str:
    """ZATCA phase-1 QR payload: TLV (tag byte, length byte, UTF-8 value) for
    tags 1 seller name, 2 VAT number, 3 ISO timestamp, 4 total incl. VAT,
    5 VAT amount — concatenated and base64-encoded."""
    out = bytearray()
    for tag, val in enumerate([seller, vat_no, timestamp_iso, total, vat], start=1):
        b = str(val).encode("utf-8")
        if len(b) > 255:
            raise ValueError("TLV value too long")
        out += bytes([tag, len(b)]) + b
    return base64.b64encode(bytes(out)).decode("ascii")


def decode_tlv_base64(s: str) -> dict:
    raw = base64.b64decode(s)
    out, i = {}, 0
    while i < len(raw):
        tag, ln = raw[i], raw[i + 1]
        out[tag] = raw[i + 2:i + 2 + ln].decode("utf-8")
        i += 2 + ln
    return out


DOC_TYPES = {
    "pod": ("Proof of Delivery", "إثبات التسليم"),
    "evidence_pack": ("Delivery Evidence Pack", "ملف إثبات التسليم"),
    "customs_bill": ("Customs Bill", "فاتورة الجمارك"),
    "tax_invoice": ("Tax Invoice", "فاتورة ضريبية"),
    "return_label": ("Return Label", "ملصق الإرجاع"),
    "payment_receipt": ("Payment Receipt", "إيصال دفع"),
}


def _sar(v, ar=False) -> str:
    return f"{money(v):,.2f} ريال" if ar else f"SAR {money(v):,.2f}"


async def _shipment_block(pdf: BarqPdf, s: dict, refs: dict, cust: dict):
    store = refs["stores"].get(s.get("store_id")) or {}
    pdf.section("Shipment", "الشحنة")
    pdf.kv("Tracking number", "رقم التتبع", s["tracking_number"], True)
    pdf.kv("Store", "المتجر", pdf.t(store.get("name_en"), store.get("name_ar")))
    pdf.kv("Order reference", "رقم الطلب", s.get("order_ref"))
    pdf.kv("Contents", "المحتويات", pdf.t(s.get("description_en"), s.get("description_ar") or s.get("description_en")))
    pdf.kv("Customer", "العميل", pdf.t(cust.get("full_name_en"), cust.get("full_name_ar")))
    pdf.kv("Delivery address", "عنوان التوصيل", pdf.t(s.get("delivery_address_en"), s.get("delivery_address_ar") or s.get("delivery_address_en")))
    if s.get("delivery_short_address"):
        pdf.kv("Short address", "العنوان المختصر", s.get("delivery_short_address"))


async def _delivery_block(pdf: BarqPdf, s: dict, with_photo: bool = True):
    pod = s.get("pod") or {}
    dt = parse_dt(s.get("delivered_at"))
    pdf.section("Delivery record", "سجل التسليم")
    pdf.kv("Delivered at", "وقت التسليم", pdf.t(dt_label_en(dt), dt_label_ar(dt)), True)
    pdf.kv("Received / signed by", "المستلم / الموقّع", pod.get("signed_by"))
    method_ar = {"Signature": "توقيع", "Code": "رمز التسليم", "Security": "حارس العمارة"}
    pdf.kv("Method", "طريقة التسليم", pdf.t(pod.get("method"), method_ar.get(pod.get("method"), pod.get("method"))))
    if pod.get("gps_lat") is not None:
        pdf.kv("GPS at delivery", "الموقع عند التسليم", f"{float(pod['gps_lat']):.5f}, {float(pod['gps_lng']):.5f}")
    if pod.get("distance_from_address_m") is not None:
        pdf.kv("Distance from address", "المسافة عن العنوان",
               pdf.t(f"{pod['distance_from_address_m']} m", f"{pod['distance_from_address_m']} متر"))
    if s.get("pod_locked"):
        pdf.kv("Record status", "حالة السجل", pdf.t("Locked — under investigation", "مقفل — قيد التحقيق"), True)
    if with_photo and pod.get("photo_path"):
        data = await storage_download(pod["photo_path"])
        if data:
            pdf.section("Delivery photo", "صورة التسليم")
            pdf.image(data, 300, 220, pdf.t("Photo taken by the driver at delivery", "صورة التقطها المندوب عند التسليم"))
        else:
            pdf.para(pdf.t("Delivery photo on file (not embedded).", "صورة التسليم محفوظة في السجل."), 8, "#8A98AE")


async def render_document(doc_type: str, ref: str, lang: str, owner: str) -> tuple:
    """Build the PDF. Returns (bytes, canonical_ref, customer_id, tracking)."""
    refs = await get_refs()
    title_en, title_ar = DOC_TYPES[doc_type]
    now = _now()
    ref_u = ref.strip().upper().replace(" ", "")

    async def load_shipment(tn):
        s = await sb_get_one("shipments", {"tracking_number": f"eq.{tn}"}, owner=owner)
        if not s:
            raise not_found("shipment_not_found", f"No shipment {tn} found")
        c = await sb_get_one("customers", {"customer_id": f"eq.{s['customer_id']}"}, owner=owner) or {}
        return s, c

    if doc_type == "pod":
        s, cust = await load_shipment(ref_u)
        if not is_delivered(s):
            raise conflict("not_allowed_in_status", "Proof of delivery exists only for delivered parcels.",
                           status=s.get("status"))
        pdf = BarqPdf(lang, title_en, title_ar, s["tracking_number"])
        pdf.stamp("DELIVERED", "تم التسليم")
        pdf.y -= 26
        await _shipment_block(pdf, s, refs, cust)
        await _delivery_block(pdf, s)
        pdf.section("Certification", "إقرار")
        pdf.para(pdf.t("Barq Express certifies that the shipment above was recorded as delivered as shown, "
                       f"based on the driver's device record. Issued {dt_label_en(now)}.",
                       "تشهد برق إكسبرس بأن الشحنة أعلاه سُجلت كمسلّمة وفق ما هو موضح، استناداً إلى سجل جهاز المندوب. "
                       f"صدرت في {dt_label_ar(now)}."), 9)
        return pdf.finish(), s["tracking_number"], s["customer_id"], s["tracking_number"]

    if doc_type == "evidence_pack":
        s, cust = await load_shipment(ref_u)
        if not is_delivered(s):
            raise conflict("not_allowed_in_status", "An evidence pack is for delivered parcels.", status=s.get("status"))
        cases = await sb_get("cases", {"tracking_number": f"eq.{s['tracking_number']}", "order": "opened_at.desc"}, owner=owner)
        pdf = BarqPdf(lang, title_en, title_ar, s["tracking_number"])
        await _shipment_block(pdf, s, refs, cust)
        await _delivery_block(pdf, s)
        dd = delivered_date(s)
        window = int(rule(refs["rules"], "damage_claim_window_days"))
        pdf.section("For the store", "للمتجر")
        pdf.para(pdf.t(
            f"This pack is provided so the customer can raise the matter under the store's own guarantee. "
            f"Barq's damage-claim window is {window} days from delivery"
            + (f"; this parcel was delivered on {date_label_en(dd, True)} ({(now.date() - dd).days} days ago)." if dd else ".")
            + " Barq will answer any question the store sends about this delivery.",
            f"هذا الملف مقدم ليتمكن العميل من متابعة الموضوع عبر ضمان المتجر. مدة مطالبات التلف لدى برق {window} أيام من التسليم"
            + (f"؛ وقد سُلّمت هذه الشحنة في {date_label_ar(dd, True)} (قبل {(now.date() - dd).days} يوماً)." if dd else ".")
            + " وسترد برق على أي استفسار من المتجر بخصوص هذا التسليم."), 9)
        for c in cases:
            pdf.section(f"Case {c['case_id']} — customer statement", f"الحالة {c['case_id']} — إفادة العميل")
            pdf.kv("Opened", "تاريخ الفتح", pdf.t(dt_label_en(parse_dt(c.get("opened_at"))), dt_label_ar(parse_dt(c.get("opened_at")))))
            pdf.kv("Status", "الحالة", c.get("status"))
            pdf.para(c.get("description_ar") if pdf.ar and c.get("description_ar") else (c.get("description_en") or ""), 9)
            for a in (c.get("attachments") or [])[:4]:
                if a.get("stored") and str(a.get("content_type") or "").startswith("image/"):
                    data = await storage_download(a["path"])
                    if data:
                        pdf.image(data, 240, 170, pdf.t("Customer photo", "صورة من العميل"))
        if not cases:
            pdf.para(pdf.t("No Barq case is open for this parcel.", "لا توجد حالة مفتوحة لدى برق لهذه الشحنة."), 9, "#5A6B85")
        return pdf.finish(), s["tracking_number"], s["customer_id"], s["tracking_number"]

    if doc_type == "customs_bill":
        s, cust = await load_shipment(ref_u)
        customs = s.get("customs") or {}
        if not customs:
            raise conflict("not_allowed_in_status", "This parcel has no customs bill.", status=s.get("status"))
        pdf = BarqPdf(lang, title_en, title_ar, s["tracking_number"])
        await _shipment_block(pdf, s, refs, cust)
        pdf.kv("Origin", "بلد المنشأ", s.get("origin_country"))
        pdf.section("Customs charges", "الرسوم الجمركية")
        ar = pdf.ar
        rows = [
            [pdf.t("Goods value (declared)", "قيمة البضاعة (المصرّح بها)"), _sar(customs.get("goods_value_sar"), ar)],
            [pdf.t("International shipping", "الشحن الدولي"), _sar(customs.get("shipping_sar"), ar)],
            [pdf.t("Customs duty (under SAR 1,000 threshold)" if money(customs.get("duty_sar")) == 0 else "Customs duty",
                   "الرسوم الجمركية (أقل من حد 1,000 ريال)" if money(customs.get("duty_sar")) == 0 else "الرسوم الجمركية"),
             _sar(customs.get("duty_sar"), ar)],
            [pdf.t("VAT 15% (on goods + shipping)", "ضريبة القيمة المضافة 15% (على البضاعة والشحن)"), _sar(customs.get("vat_sar"), ar)],
            [pdf.t("Customs clearance fee", "رسوم التخليص الجمركي"), _sar(customs.get("clearance_fee_sar"), ar)],
        ]
        pdf.table([pdf.t("Item", "البند"), pdf.t("Amount", "المبلغ")], rows, [355, 160],
                  [pdf.t("Total due", "الإجمالي المستحق"), _sar(customs.get("total_due_sar"), ar)])
        pdf.section("Requirements", "المتطلبات")
        st_ar = {"Missing": "ناقص", "Received": "تم الاستلام", "Paid": "مدفوع"}
        for r in customs.get("requirements") or []:
            pdf.kv(r.get("label_en") or r.get("code"), r.get("label_ar") or r.get("code"),
                   pdf.t(r.get("status"), st_ar.get(r.get("status"), r.get("status"))), r.get("status") == "Missing")
        cs_ar = {"Awaiting Customer": "بانتظار العميل", "Submitted": "قيد المراجعة", "Cleared": "تم التخليص"}
        pdf.kv("Customs status", "حالة التخليص",
               pdf.t(customs.get("status"), cs_ar.get(customs.get("status"), customs.get("status"))), True)
        return pdf.finish(), s["tracking_number"], s["customer_id"], s["tracking_number"]

    if doc_type == "tax_invoice":
        inv = await sb_get_one("tax_invoices", {"tax_invoice_id": f"eq.{ref_u}"}, owner=owner) if ref_u.startswith("TAX-") \
            else await sb_get_one("tax_invoices", {"tracking_number": f"eq.{ref_u}"}, owner=owner)
        if not inv:
            raise not_found("document_not_found", f"No tax invoice {ref_u}")
        s, cust = await load_shipment(inv["tracking_number"])
        issued = parse_dt(inv.get("issued_at")) or now
        pdf = BarqPdf(lang, title_en, title_ar, inv["tax_invoice_id"])
        pdf.section("Seller", "البائع")
        pdf.kv("Name", "الاسم", pdf.t(SELLER_NAME_EN, SELLER_NAME_AR), True)
        pdf.kv("VAT number", "الرقم الضريبي", SELLER_VAT)
        pdf.kv("Invoice date", "تاريخ الفاتورة", pdf.t(dt_label_en(issued), dt_label_ar(issued)))
        pdf.section("Buyer", "المشتري")
        pdf.kv("Company", "الشركة", pdf.t(inv.get("company_name_en"), inv.get("company_name_ar") or inv.get("company_name_en")), True)
        pdf.kv("VAT number", "الرقم الضريبي", inv.get("vat_number"))
        pdf.kv("Tracking number", "رقم التتبع", inv["tracking_number"])
        pdf.section("Lines", "البنود")
        ar = pdf.ar
        pdf.table([pdf.t("Description", "الوصف"), pdf.t("Amount", "المبلغ")],
                  [[pdf.t(f"Delivery service — {inv['tracking_number']}", f"خدمة التوصيل — {inv['tracking_number']}"),
                    _sar(inv.get("subtotal_sar"), ar)],
                   [pdf.t("VAT 15%", "ضريبة القيمة المضافة 15%"), _sar(inv.get("vat_sar"), ar)]],
                  [355, 160], [pdf.t("Total (VAT inclusive)", "الإجمالي شامل الضريبة"), _sar(inv.get("total_sar"), ar)])
        ts = issued.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        tlv = zatca_tlv_base64(SELLER_NAME_EN, SELLER_VAT, ts, f"{money(inv.get('total_sar')):.2f}",
                               f"{money(inv.get('vat_sar')):.2f}")
        pdf.y -= 6
        pdf.qr(tlv, 110, pdf.t("ZATCA QR (demo)", "رمز الاستجابة — زاتكا (تجريبي)"))
        pdf.para(pdf.t("Simplified tax invoice", "فاتورة ضريبية مبسطة"), 9, "#5A6B85")
        return pdf.finish(), inv["tax_invoice_id"], inv["customer_id"], inv["tracking_number"]

    if doc_type == "return_label":
        ret = await sb_get_one("returns", {"return_id": f"eq.{ref_u}"}, owner=owner) if ref_u.startswith("RET-") \
            else await sb_get_one("returns", {"tracking_number": f"eq.{ref_u}", "order": "created_at.desc"}, owner=owner)
        if not ret:
            raise not_found("document_not_found", f"No return {ref_u}")
        s, cust = await load_shipment(ret["tracking_number"])
        store = refs["stores"].get(s.get("store_id")) or {}
        pdf = BarqPdf(lang, title_en, title_ar, ret["return_id"])
        pdf.barcode(ret["return_id"], 56)
        pdf.section("Return", "الإرجاع")
        pdf.kv("Return ID", "رقم الإرجاع", ret["return_id"], True)
        pdf.kv("Original tracking", "رقم التتبع الأصلي", ret["tracking_number"])
        reason_ar = {"Wrong Item": "منتج خاطئ", "Wrong Size": "مقاس خاطئ", "Damaged": "تالف",
                     "Changed Mind": "تغيير الرأي", "Other": "أخرى"}
        pdf.kv("Reason", "السبب", pdf.t(ret.get("reason_code"), reason_ar.get(ret.get("reason_code"), ret.get("reason_code"))))
        if ret.get("reason_detail"):
            pdf.kv("Detail", "التفاصيل", ret.get("reason_detail"))
        pd_ = parse_date(ret.get("pickup_date"))
        wl = window_labels(ret.get("pickup_window"), refs["windows"])
        pdf.kv("Pickup slot", "موعد الاستلام",
               pdf.t(f"{date_label_en(pd_)}, {wl['window_label_en']}", f"{date_label_ar(pd_)}، {wl['window_label_ar']}"), True)
        pdf.section("From", "من")
        pdf.kv("Customer", "العميل", pdf.t(cust.get("full_name_en"), cust.get("full_name_ar")))
        pdf.kv("Address", "العنوان", pdf.t(s.get("delivery_address_en") or cust.get("address_en"),
                                          s.get("delivery_address_ar") or cust.get("address_ar")))
        pdf.section("To", "إلى")
        pdf.kv("Merchant", "المتجر", pdf.t(store.get("name_en"), store.get("name_ar")), True)
        pdf.kv("Via", "عبر", pdf.t("Barq Express returns — Riyadh Central Sorting Centre", "مرتجعات برق إكسبرس — مركز فرز الرياض المركزي"))
        pdf.para(pdf.t("Print this label or show it on your phone. Keep the item in its original packaging.",
                       "اطبع هذا الملصق أو اعرضه على جوالك. احتفظ بالمنتج في غلافه الأصلي."), 9, "#5A6B85")
        return pdf.finish(), ret["return_id"], ret["customer_id"], ret["tracking_number"]

    if doc_type == "payment_receipt":
        p = await sb_get_one("payment_requests", {"payment_id": f"eq.{ref_u}"}, owner=owner)
        if not p:
            raise not_found("document_not_found", f"No payment {ref_u}")
        if p.get("status") != "Paid":
            raise conflict("not_paid", "A receipt is available once the payment is made.", status=p.get("status"))
        s, cust = await load_shipment(p["tracking_number"])
        paid = parse_dt(p.get("paid_at"))
        pdf = BarqPdf(lang, title_en, title_ar, p["payment_id"])
        pdf.stamp("PAID", "مدفوع")
        pdf.y -= 26
        pdf.section("Payment", "الدفع")
        pdf.kv("Receipt number", "رقم الإيصال", p["payment_id"], True)
        pdf.kv("Paid at", "وقت الدفع", pdf.t(dt_label_en(paid), dt_label_ar(paid)))
        pdf.kv("Method", "طريقة الدفع", p.get("method"))
        pdf.kv("Purpose", "الغرض", pdf.t({"COD": "Cash-on-delivery prepaid", "Customs": "Customs charges"}.get(p.get("purpose")),
                                         {"COD": "دفع مسبق بدل الدفع عند الاستلام", "Customs": "رسوم جمركية"}.get(p.get("purpose"))))
        pdf.kv("Tracking number", "رقم التتبع", p["tracking_number"])
        pdf.kv("Customer", "العميل", pdf.t(cust.get("full_name_en"), cust.get("full_name_ar")))
        ar = pdf.ar
        lines = p.get("line_items") or []
        pdf.table([pdf.t("Item", "البند"), pdf.t("Amount", "المبلغ")],
                  [[pdf.t(li.get("label_en"), li.get("label_ar") or li.get("label_en")), _sar(li.get("amount_sar"), ar)]
                   for li in lines], [355, 160], [pdf.t("Total paid", "الإجمالي المدفوع"), _sar(p.get("amount_sar"), ar)])
        return pdf.finish(), p["payment_id"], p["customer_id"], p["tracking_number"]

    raise bad_request("invalid_document_type", f"type must be one of {sorted(DOC_TYPES)}")


@app.get("/document")
async def get_document(
    type: str = Query(..., description="pod | evidence_pack | customs_bill | tax_invoice | return_label | payment_receipt"),
    ref: str = Query(..., description="tracking number, TAX-/RET-/PAY- id"),
    lang: str = Query("en"),
    caller_phone: Optional[str] = Query(None),
):
    """Render a branded PDF, upload it (upsert) to
    documents/<owner_id>/<type>/<ref>-<lang>.pdf and return a 1-hour signed
    URL. media_type 5 = document for the Nebelus send_whatsapp_media tool."""
    owner = await resolve_owner(caller_phone)
    doc_type = (type or "").strip().lower()
    if doc_type not in DOC_TYPES:
        raise bad_request("invalid_document_type", f"type must be one of {sorted(DOC_TYPES)}")
    lang_n = "ar" if (lang or "").strip().lower().startswith("ar") else "en"
    if not ref or not ref.strip():
        raise bad_request("validation_error", "ref is required")
    pdf_bytes, canon, cid, tn = await render_document(doc_type, ref, lang_n, owner)
    path = f"{owner}/{doc_type}/{canon}-{lang_n}.pdf"
    ok = await storage_upload(path, pdf_bytes, "application/pdf", upsert=True)
    if not ok:
        raise HTTPException(status_code=502, detail={"code": "storage_error", "message": "Could not store the document"})
    url = await storage_sign(path, 3600, use_cache=False)
    if not url:
        raise HTTPException(status_code=502, detail={"code": "storage_error", "message": "Could not sign the document URL"})
    fonts = _init_fonts()
    file_name = f"Barq-{DOC_TYPES[doc_type][0].replace(' ', '-')}-{canon}-{lang_n.upper()}.pdf"
    await log_agent_action(cid, tn, "Document Sent",
                           f"{DOC_TYPES[doc_type][0]} ({lang_n.upper()}) generated for {canon} and sent on WhatsApp",
                           {"type": doc_type, "ref": canon, "lang": lang_n, "path": path, "bytes": len(pdf_bytes)},
                           owner=owner)
    return {"ok": True, "download_url": url, "file_name": file_name, "content_type": "application/pdf",
            "media_type": 5, "type": doc_type, "ref": canon, "lang": lang_n,
            "arabic_rendered": lang_n == "ar" and fonts["shaping"], "expires_in_seconds": 3600}


# ============================================================
# Dev entrypoint
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
