import hashlib
from config import QDRANT_URL, QDRANT_COLLECTION, QDRANT_ENABLED

_qdrant_client = None


def _get_qdrant_client():
    global _qdrant_client
    if not QDRANT_ENABLED:
        return None
    if _qdrant_client is not None:
        return _qdrant_client
    try:
        from qdrant_client import QdrantClient
        _qdrant_client = QdrantClient(url=QDRANT_URL, timeout=10)
        print(f"[Qdrant] connected to {QDRANT_URL}")
        return _qdrant_client
    except ImportError:
        print("[Qdrant] qdrant_client not installed — embeddings disabled")
        return None
    except Exception as e:
        print(f"[Qdrant] connection failed: {e}")
        return None


def _qdrant_ensure_collection():
    client = _get_qdrant_client()
    if not client:
        return False
    try:
        from qdrant_client.models import Distance, VectorParams
        collections = client.get_collections().collections
        names = [c.name for c in collections]
        if QDRANT_COLLECTION not in names:
            client.create_collection(
                collection_name=QDRANT_COLLECTION,
                vectors_config=VectorParams(size=768, distance=Distance.COSINE),
            )
            print(f"[Qdrant] created collection '{QDRANT_COLLECTION}' (768-dim cosine)")
        return True
    except Exception as e:
        print(f"[Qdrant] ensure_collection error: {e}")
        return False


def _qdrant_upsert(gen_id: str, vector: list, payload: dict):
    client = _get_qdrant_client()
    if not client or not vector:
        return False
    try:
        from qdrant_client.models import PointStruct
        point_id = int(hashlib.md5(gen_id.encode()).hexdigest()[:16], 16)
        client.upsert(
            collection_name=QDRANT_COLLECTION,
            points=[
                PointStruct(id=point_id, vector=vector, payload=payload)
            ],
        )
        print(f"[Qdrant] upsert gen_id={gen_id[:16]} dim={len(vector)}")
        return True
    except Exception as e:
        print(f"[Qdrant] upsert error: {e}")
        return False
