"""
Object storage for raw + extracted content.

Postgres holds pointers, never payloads. Large text in Postgres bloats TOAST,
slows VACUUM, and makes every backup enormous -- for data that is written
once and read rarely.

Keys are derived from the URL hash, so the same URL always maps to the same
key. Re-crawls overwrite in place; no orphan accumulation from revisits.
"""
from __future__ import annotations

import hashlib

import zstandard as zstd

RAW_PREFIX = "raw"
TEXT_PREFIX = "text"
_COMPRESSOR = zstd.ZstdCompressor(level=6)
_DECOMPRESSOR = zstd.ZstdDecompressor()


def _key(prefix: str, host: str, url_id: int, ext: str) -> str:
    # Shard by hash prefix: flat buckets with millions of keys in one
    # pseudo-directory degrade badly on most object stores.
    shard = hashlib.sha256(str(url_id).encode()).hexdigest()[:2]
    return f"{prefix}/{shard}/{host}/{url_id}.{ext}.zst"


class BlobStore:
    """Thin async wrapper over an S3-compatible client (MinIO in dev)."""

    def __init__(self, client, bucket: str):
        self.client = client
        self.bucket = bucket

    async def put_raw(self, host: str, url_id: int, body: bytes) -> str:
        key = _key(RAW_PREFIX, host, url_id, "html")
        await self._put(key, _COMPRESSOR.compress(body), "text/html")
        return key

    async def put_text(self, host: str, url_id: int, text: str) -> str:
        key = _key(TEXT_PREFIX, host, url_id, "txt")
        await self._put(key, _COMPRESSOR.compress(text.encode()), "text/plain")
        return key

    async def get(self, key: str) -> bytes:
        obj = await self.client.get_object(Bucket=self.bucket, Key=key)
        return _DECOMPRESSOR.decompress(await obj["Body"].read())

    async def _put(self, key: str, data: bytes, content_type: str) -> None:
        await self.client.put_object(
            Bucket=self.bucket, Key=key, Body=data,
            ContentType=content_type, ContentEncoding="zstd",
        )
