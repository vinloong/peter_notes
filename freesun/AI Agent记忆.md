AI Agent记忆

```sh
docker run -p 6333:6333 -p 6334:6334 \
    -v $(pwd)/qdrant_storage:/qdrant/storage:z \
   registry.ngaiot.com/tools/qdrant:20250807
```

