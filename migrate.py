import chromadb
from llama_index.core import (
    VectorStoreIndex,
    StorageContext,
    load_index_from_storage,
)
from llama_index.vector_stores.chroma import ChromaVectorStore

OLD_PERSIST_DIR = "./storage"
CHROMA_PERSIST_DIR = "./chroma_db"
CHROMA_COLLECTION_NAME = "wikipedia_articles"

storage_context_old = StorageContext.from_defaults(persist_dir=OLD_PERSIST_DIR)
old_index = load_index_from_storage(storage_context_old)

db = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
chroma_collection = db.get_or_create_collection(CHROMA_COLLECTION_NAME)

vector_store_new = ChromaVectorStore(chroma_collection=chroma_collection)
storage_context_new = StorageContext.from_defaults(vector_store=vector_store_new)

new_index = VectorStoreIndex(
    nodes=list(old_index.docstore.docs.values()),  # Get all nodes from the old index
    storage_context=storage_context_new,
    show_progress=True,
)
