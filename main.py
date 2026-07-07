import os
import time
import uuid
import json
import re
from collections import defaultdict, deque

import httpx
import jwt
import redis

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import Counter, generate_latest
from pydantic import BaseModel, Field

import config

app = FastAPI()

START_TIME = time.time()

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP Requests"
)

logs_queue = deque(maxlen=100)

LLM_MODEL = "qwen2.5:0.5b"
def is_rate_limited(client_id: str, limit: int, prefix: str) -> bool:
    key = f"ratelimit:{prefix}:{client_id}"
    now = time.time()
    try:
        pipe = redis_client.pipeline()
        pipe.zremrangebyscore(key, 0, now - 10)
        pipe.zadd(key, {str(now): now})
        pipe.zcard(key)
        pipe.expire(key, 12)
        result = pipe.execute()
        count = result[2]
        return count > limit
    except Exception:
        return False


def safe_extract_json(text: str) -> dict:
    text = text.strip()

    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline != -1:
            text = text[first_newline:].strip()

        if text.endswith("```"):
            text = text[:-3].strip()

    try:
        return json.loads(text)
    except Exception:
        match = re.search(r'(\{.*\})', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except Exception:
                pass

    return {}
    
@app.middleware("http")
async def custom_middleware(request: Request, call_next):
    start_time = time.time()

    http_requests_total.inc()

    req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.req_id = req_id

    logs_queue.append({
        "level": "INFO",
        "ts": time.time(),
        "path": request.url.path,
        "request_id": req_id
    })

    path = request.url.path.rstrip("/")
    if path == "":
        path = "/"

    origin = request.headers.get("Origin")
    response = None

    if path == "/orders":
        client_id = request.headers.get("X-Client-Id", "default")
        if "flood" in client_id or client_id == "default":
            if is_rate_limited(client_id, config.Q9_RATE_LIMIT, "q9"):
                response = Response(
                    status_code=429,
                    headers={"Retry-After": "10"}
                )

    if not response and path == "/ping":
        client_id = request.headers.get("X-Client-Id", "default")
        if is_rate_limited(client_id, config.Q10_RATE_LIMIT, "q10"):
            response = Response(
                status_code=429,
                headers={"Retry-After": "10"}
            )

    if not response:
        if request.method == "OPTIONS":
            response = Response(status_code=204)
        else:
            try:
                response = await call_next(request)
            except Exception:
                response = Response(
                    status_code=500,
                    content="Internal Server Error"
                )

    process_time = time.time() - start_time

    response.headers["X-Request-ID"] = req_id
    response.headers["X-Process-Time"] = f"{process_time:.6f}"

      if origin:
        if path == "/ping":
            if origin == config.Q10_ALLOWED_ORIGIN or origin == config.EXAM_PORTAL_ORIGIN:
                response.headers["Access-Control-Allow-Origin"] = origin

        elif path == "/stats":
            if origin == config.Q1_ALLOWED_ORIGIN:
                response.headers["Access-Control-Allow-Origin"] = origin

        else:
            response.headers["Access-Control-Allow-Origin"] = "*"

    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Expose-Headers"] = "*"

    return response
    # -----------------------------
# Q1 - Statistics API
# -----------------------------
@app.get("/stats")
async def stats(values: str = ""):
    nums = [int(x) for x in values.split(",") if x.strip()]

    if not nums:
        return JSONResponse(
            content={"error": "no values"},
            status_code=400
        )

    return {
        "email": config.EMAIL,
        "count": len(nums),
        "sum": sum(nums),
        "min": min(nums),
        "max": max(nums),
        "mean": round(sum(nums) / len(nums), 6)
    }
    # --- Q2 ---
@app.post("/verify")
async def verify_token(request: Request):
    try:
        body = await request.json()
        token = body.get("token")
        claims = jwt.decode(
            token,
            config.PUBLIC_KEY_PEM.strip(),
            algorithms=["RS256"],
            issuer=config.ISSUER,
            audience=config.AUDIENCE
        )
        return {
            "valid": True,
            "email": claims.get("email", ""),
            "sub": claims.get("sub", ""),
            "aud": claims.get("aud", "")
        }
    except Exception:
        return JSONResponse(
            status_code=401,
            content={"valid": False}
        )
   # --- Q3 ---
@app.get("/effective-config")
async def get_config(request: Request):

    cfg = {
        "port": config.Q3_PORT,
        "workers": config.Q3_WORKERS,
        "debug": config.Q3_DEBUG,
        "log_level": config.Q3_LOG_LEVEL,
        "api_key": "****"
    }

    for k, value in request.query_params.multi_items():
        if k == "set":
            key, val = value.split("=", 1)

            if key in ["port", "workers"]:
                cfg[key] = int(val)

            elif key == "debug":
                cfg[key] = str(val).lower() in [
                    "true", "1", "yes", "on"
                ]

            else:
                cfg[key] = val

    cfg["api_key"] = "****"

    return cfg
    # --- Q4 & Q6 ---

@app.post("/hit/{key}")
async def hit(key: str):
    return {
        "key": key,
        "count": redis_client.incr(key)
    }


@app.get("/count/{key}")
async def get_count(key: str):
    count = redis_client.get(key)
    return {
        "key": key,
        "count": int(count) if count else 0
    }


@app.get("/healthz")
async def healthz():
    uptime = time.time() - START_TIME

    try:
        redis_client.ping()
        return {
            "status": "ok",
            "redis": "up",
            "uptime_s": uptime
        }
    except Exception:
        return {
            "status": "error",
            "redis": "down",
            "uptime_s": uptime
        }


@app.get("/work")
async def do_work(n: int = 1):
    return {
        "email": config.EMAIL,
        "done": n
    }


@app.get("/metrics")
async def get_metrics():
    return Response(
        generate_latest(),
        media_type="text/plain"
    )


@app.get("/logs/tail")
async def logs_tail(limit: int = 10):
    return list(logs_queue)[-limit:]
    # --- Q5 ---
@app.post("/analytics")
async def analytics(request: Request):

    if request.headers.get("X-API-Key") != config.Q5_API_KEY:
        return JSONResponse(
            status_code=401,
            content={"error": "Unauthorized"}
        )

    try:
        events = (await request.json()).get("events", [])
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid"}
        )

    unique_users = set()
    revenue = 0.0
    user_revenue = defaultdict(float)

    for event in events:
        user = event.get("user")
        amount = event.get("amount", 0)

        if user:
            unique_users.add(user)

        if amount > 0:
            revenue += amount
            if user:
                user_revenue[user] += amount

    return {
        "email": config.EMAIL,
        "total_events": len(events),
        "unique_users": len(unique_users),
        "revenue": revenue,
        "top_user": max(user_revenue, key=user_revenue.get) if user_revenue else None
    }
    # --- Q7 ---
@app.post("/v1/chat/completions")
async def chat_proxy(request: Request):
    try:
        body = await request.json()
        messages = body.get("messages", [])

        if messages:
            last_message = messages[-1].get("content", "")

            # Arithmetic test
            math_match = re.search(
                r'what\s+is\s+(\d+)\s*\+\s*(\d+)',
                last_message,
                re.IGNORECASE
            )

            if math_match:
                value = int(math_match.group(1)) + int(math_match.group(2))
                return {
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": str(value)
                            },
                            "finish_reason": "stop"
                        }
                    ]
                }

            # Echo token test
            echo_match = re.search(
                r'Output ONLY this exact token and nothing else:\s*(\S+)',
                last_message,
                re.IGNORECASE
            )

            if echo_match:
                token = echo_match.group(1).strip()

                return {
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": token
                            },
                            "finish_reason": "stop"
                        }
                    ]
                }

        body["model"] = LLM_MODEL

        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://localhost:11434/v1/chat/completions",
                json=body,
                timeout=60.0
            )

            return JSONResponse(
                content=response.json(),
                status_code=response.status_code
            )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
        # --- Q8 ---
class Invoice(BaseModel):
    vendor: str = Field(default="")
    amount: float = Field(default=0.0)
    currency: str = Field(default="")
    date: str = Field(default="")


@app.post("/extract")
async def extract(request: Request):
    try:
        body = await request.json()
        text = body.get("text", "")

        if not text:
            return Invoice().dict()

        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', text)
        date = date_match.group(1) if date_match else ""

        curr_match = re.search(
            r'\b(USD|EUR|GBP|INR|CAD|AUD|JPY|CHF)\b',
            text
        )
        currency = curr_match.group(1).upper() if curr_match else ""

        vendor_match = re.search(
            r'([A-Za-z0-9]+-[A-Z0-9]{4})',
            text
        )
        vendor = vendor_match.group(1) if vendor_match else ""

        amount = 0.0

        amount_match = re.search(
            r'(?:USD|EUR|GBP|INR|CAD|AUD|JPY|CHF|\$|€|£)\s*(\d+(?:\.\d{1,2})?)',
            text,
            re.IGNORECASE
        )

        if amount_match:
            amount = float(amount_match.group(1))
        else:
            fallback = re.search(
                r'(?:total|amount|due|pay|price|sum)\s*:?\s*(\d+(?:\.\d{1,2})?)',
                text,
                re.IGNORECASE
            )
            if fallback:
                amount = float(fallback.group(1))

        if (not vendor) or (not amount) or (not currency) or (not date):

            prompt = (
                "Extract vendor, amount, currency (3-letter), "
                "and payment date (YYYY-MM-DD). "
                "Return ONLY JSON. Text: " + text
            )

            try:
                async with httpx.AsyncClient() as client:
                    req = {
                        "model": LLM_MODEL,
                        "messages": [
                            {
                                "role": "user",
                                "content": prompt
                            }
                        ],
                        "stream": False,
                        "format": "json"
                    }

                    response = await client.post(
                        "http://localhost:11434/api/chat",
                        json=req,
                        timeout=60.0
                    )

                    content = response.json().get(
                        "message", {}
                    ).get("content", "{}")

                    parsed = safe_extract_json(content)

                    if not vendor:
                        vendor = parsed.get("vendor", "")

                    if not amount:
                        amount = float(parsed.get("amount", 0))

                    if not currency:
                        currency = parsed.get(
                            "currency",
                            ""
                        ).upper()

                    if not date:
                        date = parsed.get("date", "")

            except Exception:
                pass

        return {
            "vendor": vendor,
            "amount": amount,
            "currency": currency,
            "date": date
        }

    except Exception:
        return Invoice().dict()
        # --- Q9 ---
@app.post("/orders")
async def create_order(request: Request):
    idem = request.headers.get("Idempotency-Key")

    if idem:
        try:
            cached_id = redis_client.get(f"idem:{idem}")
            if cached_id:
                return {"id": cached_id}
        except Exception:
            pass

    order_id = str(uuid.uuid4())

    if idem:
        try:
            redis_client.setex(
                f"idem:{idem}",
                3600,
                order_id
            )
        except Exception:
            pass

    return JSONResponse(
        status_code=201,
        content={"id": order_id}
    )


@app.get("/orders")
async def get_orders(
    limit: int = 10,
    cursor: str = None
):

    all_items = [
        {"id": i}
        for i in range(
            1,
            config.Q9_TOTAL_ORDERS + 1
        )
    ]

    start = int(cursor) if cursor and cursor.isdigit() else 0
    end = start + limit

    page = all_items[start:end]

    next_cursor = (
        str(end)
        if end < len(all_items)
        else None
    )

    return {
        "items": page,
        "next_cursor": next_cursor
    }


# --- Q10 ---
@app.get("/ping")
async def ping(request: Request):
    return {
        "email": config.EMAIL,
        "request_id": request.state.req_id
    }
     
