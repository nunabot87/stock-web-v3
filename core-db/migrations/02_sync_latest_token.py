#!/opt/stock-web-v3/venv/bin/python3
"""Sync latest Stockbit token from Redis to core_db PostgreSQL"""
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

async def sync_new_token():
    r = redis_async.from_url(REDIS_URL, decode_responses=True)
    conn = await asyncpg.connect(DB_URL)

    token = await r.get("stockbit_token:system:primary")
    meta_raw = await r.get("stockbit_token:system:meta")

    if not token or not meta_raw:
        print("❌ No token found in Redis")
        return 1

    meta = json.loads(meta_raw)
    print(f"🔴 TOKEN BARU TERDETEKSI")
    print(f"   User: {meta.get('user')}")
    print(f"   Email: {meta.get('email')}")
    print(f"   Hash: {meta.get('token_hash')}")
    print(f"   Exp (Unix): {meta.get('exp')}")
    
    exp_dt = datetime.fromtimestamp(meta.get('exp'), tz=timezone.utc)
    now = datetime.now(timezone.utc)
    remaining = (meta.get('exp') - now.timestamp()) / 3600.0
    print(f"   Exp (Human): {exp_dt.isoformat()}")
    print(f"   Remaining: {remaining:.1f} hours")
    print(f"   Ext Version: {meta.get('extension_version')}")
    print(f"   Last Updated: {datetime.fromtimestamp(meta.get('last_updated'), tz=timezone.utc).isoformat()}")

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
    
    provider_id = await conn.fetchval("""
        INSERT INTO token_providers (provider_type, provider_name, device_fingerprint, user_agent, source_ip, is_active)
        VALUES ('chrome_extension', $1, $2, $3, NULL, TRUE)
        ON CONFLICT (provider_type, device_fingerprint) DO UPDATE SET
            provider_name = EXCLUDED.provider_name,
            user_agent = EXCLUDED.user_agent,
            updated_at = NOW()
        RETURNING id
    """, meta.get("extension_version", "chrome_extension"), meta.get("token_hash"), meta.get("user_agent", ""))

    print(f"\n📍 Provider ID: {provider_id}")

    await conn.execute("""
        UPDATE stockbit_tokens SET is_primary = FALSE
        WHERE is_primary = TRUE
    """)
    print("🔄 Old primary tokens demoted")

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

    print(f"✅ New token inserted ID: {token_id}")

    await conn.execute("""
        INSERT INTO token_sync_log (provider_id, token_id, action, new_token_hash, status, message)
        VALUES ($1, $2, 'receive', $3, 'success', 'Synced from Chrome Extension via Redis')
    """, provider_id, token_id, token_hash)

    print("📝 Sync log recorded")

    row = await conn.fetchrow("""
        SELECT id, token_hash, is_primary, is_valid, remaining_hours, expires_at 
        FROM stockbit_tokens WHERE id = $1
    """, token_id)
    print(f"\n📊 VERIFIED:")
    print(f"   ID: {row['id']}")
    print(f"   Hash: {row['token_hash']}")
    print(f"   Primary: {row['is_primary']}")
    print(f"   Valid: {row['is_valid']}")
    print(f"   Remaining: {row['remaining_hours']:.1f} hours")
    print(f"   Expires: {row['expires_at'].isoformat()}")

    count = await conn.fetchval("SELECT COUNT(*) FROM stockbit_tokens")
    print(f"\n📦 Total tokens in core_db: {count}")

    await conn.close()
    await r.close()
    print("\n🎯 SYNC COMPLETE")
    return 0

if __name__ == "__main__":
    exit(asyncio.run(sync_new_token()))
