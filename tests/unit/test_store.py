"""
BlobStore tests with a fake S3 client (no real MinIO needed) -- verifies
key derivation/sharding and, critically, that the real zstd
compress/decompress round trip is correct (not mocked -- only the S3
client is faked).
"""
import zstandard as zstd

from crawler.store import BlobStore, _key


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    async def read(self) -> bytes:
        return self._data


class _FakeS3Client:
    def __init__(self):
        self.objects: dict[str, dict] = {}

    async def put_object(self, Bucket, Key, Body, ContentType, ContentEncoding):
        self.objects[Key] = {"body": Body, "content_type": ContentType,
                             "encoding": ContentEncoding, "bucket": Bucket}

    async def get_object(self, Bucket, Key):
        return {"Body": _FakeBody(self.objects[Key]["body"])}


def test_key_is_deterministic():
    assert _key("raw", "example.com", 42, "html") == _key("raw", "example.com", 42, "html")


def test_key_shards_by_url_id_hash():
    import hashlib
    key = _key("raw", "example.com", 42, "html")
    expected_shard = hashlib.sha256(b"42").hexdigest()[:2]
    assert key == f"raw/{expected_shard}/example.com/42.html.zst"


def test_key_different_url_ids_different_keys():
    assert _key("raw", "example.com", 1, "html") != _key("raw", "example.com", 2, "html")


async def test_put_raw_then_get_round_trips_through_real_zstd():
    client = _FakeS3Client()
    store = BlobStore(client, "test-bucket")
    original = b"<html><body>hello world</body></html>"

    key = await store.put_raw("example.com", 1, original)
    stored = client.objects[key]
    assert stored["content_type"] == "text/html"
    assert stored["encoding"] == "zstd"
    assert stored["bucket"] == "test-bucket"
    # actually compressed, not stored raw
    assert stored["body"] != original
    assert zstd.ZstdDecompressor().decompress(stored["body"]) == original

    fetched = await store.get(key)
    assert fetched == original


async def test_put_text_then_get_round_trips():
    client = _FakeS3Client()
    store = BlobStore(client, "test-bucket")
    original = "extracted plain text content"

    key = await store.put_text("example.com", 1, original)
    fetched = await store.get(key)
    assert fetched.decode("utf-8") == original


async def test_raw_and_text_keys_for_same_url_id_do_not_collide():
    client = _FakeS3Client()
    store = BlobStore(client, "test-bucket")

    raw_key = await store.put_raw("example.com", 1, b"raw")
    text_key = await store.put_text("example.com", 1, "text")

    assert raw_key != text_key
    assert await store.get(raw_key) == b"raw"
    assert (await store.get(text_key)).decode() == "text"
