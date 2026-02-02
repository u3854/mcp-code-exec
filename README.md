(wip)

Sanboxed python code execution tool for LLMs via MCP.

# Run

## Docker
Http transport:
```
docker compose up --build
```
## uv
Http transport:
```
uv run fastmcp run server.py:mcp --transport http --port 8000 --host 0.0.0.0
```
Stdio transport:
```
uv run server.py
```