#!/opt/stock-web-v3/venv/bin/python3
"""Migrate Stockbit token from Redis to core_db PostgreSQL"""
import asyncio
import json
import base64
import hashlib
import sys

import redis.asyncio as redis_async
import asyncpg
from datetime import datetime, timezone

REDIS_URL = "redis://localhost:6379/0"
DB_URL = "postgresql://core_app:core_app_secure_2026@localhost/core_db"

async def migrate():
    r = redis_async.from_url(REDIS_URL, decode_responses=True)
    conn = await asyncpg.connect(DB_URL)

    token = await r.get("stockbit_token:system:primary")
    meta_raw = await r.get("stockbit_token:system:meta")

    if not token or not meta_raw:
        print("❌ No token found in Redis")
        return 1

    meta = json.loads(meta_raw)
    print(f"Token found: {meta.get('user')} / {meta.get('email')}")
    print(f"Hash: {meta.get('token_hash')}")
    print(f"Exp: {meta.get('exp')}")
    print(f"Ext: {meta.get('extension_version')}")

    # Decode JWT payload
    parts = token.split(".")
    payload_b64 = parts[1]
    padding = 4 - len(payload_b64) % 4
    if padding != 4:
        payload_b64 += "=" * padding
    jwt_payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    
    token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
    token_preview = f"{token[:20]}...{token[-20:]}"

    exp_ts = meta.get("exp")
    iat_ts = meta.get("iat")
    expires_at = datetime.fromtimestamp(exp_ts, tz=timezone.utc) if exp_ts else None
    issued_at = datetime.fromtimestamp(iat_ts, tz=timezone.utc) if iat_ts else None
    
    # 1. Insert provider
    provider_id = await conn.fetchval("""
        INSERT INTO token_providers (provider_type, provider_name, device_fingerprint, user_agent, source_ip, is_active)
        VALUES ('chrome_extension', $1, $2, $3, NULL, TRUE)
        ON CONFLICT (provider_type, device_fingerprint) DO UPDATE SET
            provider_name = EXCLUDED.provider_name,
            user_agent = EXCLUDED.user_agent,
            updated_at = NOW()
        RETURNING id
    """, meta.get("extension_version", "chrome_extension"), meta.get("token_hash"), meta.get("user_agent", ""))

    print(f"Provider ID: {provider_id}")

    # 2. Invalidate old primary tokens
    await conn.execute("""
        UPDATE stockbit_tokens SET is_primary = FALSE, is_valid = FALSE
        WHERE is_primary = TRUE
    """)
    print("Old primary tokens invalidated")

    # 3. Insert new token
    token_id = await conn.fetchval("""
        INSERT INTO stockbit_tokens (
            provider_id, token, token_hash, token_preview, jwt_payload,
            user_name, email, full_name, expires_at, issued_at,
            is_valid, is_primary
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE, TRUE)
        RETURNING id
    """, provider_id, token, token_hash, token_preview, json.dumps(jwt_payload),
    meta.get("user"), meta.get("email"), meta.get("full_name"), expires_at, issued_at)

    print(f"Token inserted ID: {token_id}")

    # 4. Log sync event
    await conn.execute("""
        INSERT INTO token_sync_log (provider_id, token_id, action, new_token_hash, status, message)
        VALUES ($1, $2, 'receive', $3, 'success', 'Migrated from Redis via manual script')
    """, provider_id, token_id, token_hash)

    print("Sync log recorded")

    # 5. Verify
    row = await conn.fetchrow("SELECT id, token_hash, is_primary, is_valid, remaining_hours FROM stockbit_tokens WHERE id = $1", token_id)
    print(f"Verified: hash={row['token_hash']}, primary={row['is_primary']}, valid={row['is_valid']}, remaining={row['remaining_hours']:.1f}h")

    await conn.close()
    await r.close()
    print("\n✅ MIGRATION COMPLETE")
    return 0

if __name__ == "__main__":
    exit(asyncio.run(migrate()))
