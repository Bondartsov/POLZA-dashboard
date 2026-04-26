"""
Fix Qdrant indexing: enable max_indexing_threads in HNSW config.
When max_indexing_threads=0, vectors are stored but NOT indexed for search.
"""
import sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent))

from embeddings.qdrant import _get_qdrant_client
from qdrant_client.models import HnswConfigDiff
from config import QDRANT_COLLECTION

client = _get_qdrant_client()
if not client:
    print("ERROR: Qdrant not connected")
    sys.exit(1)

try:
    # Update HNSW config: enable indexing with 4 threads
    client.update_collection(
        collection_name=QDRANT_COLLECTION,
        hnsw_config=HnswConfigDiff(max_indexing_threads=4)
    )
    print(f"[Qdrant] Updated {QDRANT_COLLECTION}: max_indexing_threads=4")
    print("[Qdrant] Indexing should now rebuild vectors. Check dashboard in 30s.")
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
