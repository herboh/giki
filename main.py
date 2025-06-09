import os
import sys
import asyncio
from llama_index.core import (
    VectorStoreIndex,
    SimpleDirectoryReader,
    StorageContext,
    load_index_from_storage,
    Settings,
)
from llama_index.llms.ollama import Ollama
from llama_index.readers.json import JSONReader
from llama_index.embeddings.ollama import OllamaEmbedding

# Set up the LLM. Point it to your LAN Ollama instance.
Settings.llm = Ollama(
    model="gemma3:4b",
    base_url="http://192.168.1.2:11434",
    request_timeout=120,
    context_window=3072,
)
Settings.embed_model = OllamaEmbedding(
    model_name="mxbai-embed-large:335m",
    base_url="http://192.168.1.2:11434",
    embed_batch_size=32,
)
Settings.chunk_size = 512
Settings.chunk_overlap = 20

# Build RAG Index

DATA_DIR = "./data"
PERSIST_DIR = "./data/index"

if not os.path.exists(PERSIST_DIR):
    print("No existing index found. Loading input to embed...")
    reader = SimpleDirectoryReader(
        input_dir=DATA_DIR,
        required_exts=[".jsonl"],
        file_extractor={".jsonl": JSONReader(is_jsonl=True)},
    )
    articles = reader.load_data()
    print(f"Loaded {len(articles)} articles.")

    index = VectorStoreIndex.from_documents(articles, show_progress=True)
    # index.vector_store.persist(persist_path=PERSIST_DIR)  # unsure which is best
    index.storage_context.persist(persist_dir=PERSIST_DIR)  # unsure which is best
else:
    print(f"Loading existing index from {PERSIST_DIR}...")
    storage_context = StorageContext.from_defaults(persist_dir=PERSIST_DIR)
    index = load_index_from_storage(storage_context)
    # index = VectorStoreIndex.from_vector_store(vector_store)
query_engine = index.as_query_engine()


async def search_giki(
    query: str,
) -> str:
    response = await query_engine.aquery(query)
    return str(response)


async def main():
    response = await search_giki(
        "I want to know more about Geysers. Specifically about Major Geyser fields in the south pacific. What is the largest geyser ever known? When did it erupt? and recently what have scientists discoved about the crust in this area."
    )
    print(response)


if __name__ == "__main__":
    asyncio.run(main())
