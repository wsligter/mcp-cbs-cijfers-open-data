# mcp-cbs-cijfers-open-data

This project provides a [Model Context
Protocol](https://modelcontextprotocol.io/) (MCP) server for the [CBS Open Data
API](https://www.cbs.nl/nl-nl/onze-diensten/open-data). It
allows AI tools to interact with the CBS Open Data through MCP
tools.

## Features

- Implements a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP)
  server
- Provides tools for interacting with the CBS Open Data:
  - List available datasets
  - Query dataset metadata
  - Retrieve statistical data

## Tools

This MCP server provides the following tools for AI assistants to interact with the CBS Open Data API:

### `list_datasets`

Lists available datasets from the CBS Open Data API.

**Parameters:**

- `page` (optional): Page number for paginated results

**Example response:**

```jsonc
{
  "datasets": [
    {
      "id": "83583NED",
      "title": "Consumentenprijzen; prijsindex 2015=100",
      "updated": "2025-03-10T12:00:00Z"
    }
    // More datasets...
  ],
  "total": 120,
  "page": 1,
  "pageSize": 10
}
```

### `get_dataset_metadata`

Retrieves metadata for a specific dataset.

**Parameters:**

- `id` (required): Dataset identifier

**Example response:**

```jsonc
{
  "id": "83583NED",
  "title": "Consumentenprijzen; prijsindex 2015=100",
  "description": "Consumentenprijsindex (CPI), prijsindex 2015=100, vanaf 1963.",
  "updated": "2025-03-10T12:00:00Z",
  "dimensions": [
    {
      "id": "Perioden",
      "title": "Periods",
      "values": ["2020MM01", "2020MM02", "..."]
    }
    // More dimensions...
  ],
  "topics": ["Prices", "Economy"]
}
```

### `query_data`

Queries statistical data from a dataset.

**Parameters:**

- `id` (required): Dataset identifier
- `dimensions` (optional): Filter by specific dimension values
- `select` (optional): Select specific variables to return
- `format` (optional): Response format (json, csv)

**Example response:**

```jsonc
{
  "data": [
    {
      "period": "2025MM01",
      "value": 123.4,
      "category": "All households"
    }
    // More data points...
  ]
}
```

## Requirements

- [Go 1.24](https://go.dev/dl/)

## Installation

Configuration for common MCP hosts (Claude Desktop, Cursor):

```jsonc
{
  "mcpServers": {
    "cbs-cijfers-open-data": {
      "command": "go",
      "args": ["run", "github.com/dstotijn/mcp-cbs-cijfers-open-data@latest"]
    }
  }
}
```

Alternatively, you can manually install the program (given you have Go installed
):

```sh
go install github.com/dstotijn/mcp-cbs-cijfers-open-data@latest
```

## Usage

```
$ mcp-cbs-cijfers-open-data --help

Usage of mcp-cbs-cijfers-open-data:
  -http string
        HTTP listen address for JSON-RPC over HTTP (default ":8080")
  -sse
        Enable SSE transport
  -stdio
        Enable stdio transport (default true)
```

Typically, your MCP host will run the program and start the MCP server, and you
don't need to manually do this. But if you want to run the MCP server manually,
for instance because you want to serve over HTTP (using SSE):

Given you have your `PATH` environment configured to include the path named by
`$GOBIN` (or `$GOPATH/bin` `$HOME/go/bin` if `$GOBIN` is not set), you can then
run:

```sh
mcp-cbs-cijfers-open-data --stdio=false --sse
```

Which will output startup logs with the SSE transport URL:

```
CBS API MCP server started, using transports: [sse]
SSE transport endpoint: http://localhost:8080
```

## License

[Apache-2.0 license](/LICENSE)

---

© 2025 David Stotijn
