from llama_index.core import VectorStoreIndex, SimpleDirectoryReader

documents = SimpleDirectoryReader("data").load_data()
index = VectorStoreIndex.from_documents(documents)
query_engine = index.as_query_engine()
response = query_engine.query(
    "Tell me about Barack Obama's education, specifically his college and research jobs. What did he do in mid-1981?"
)
print(response)
