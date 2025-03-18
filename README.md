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
  - List available datasets and catalogs
  - Query dataset metadata and dimensions
  - Retrieve statistical data with filtering options
  - Explore dimension values

## Tools

This MCP server provides the following tools for AI assistants to interact with the CBS Open Data API:

### `get_catalogs`

Retrieves all available CBS data catalogs.

**Parameters:**

- None

### `query_datasets`

Lists available datasets from the CBS Open Data API with advanced filtering, sorting, and pagination options.

**Parameters:**

- `catalog` (required): Catalog identifier (use "CBS")
- `select` (optional): OData $select parameter to choose specific fields
- `filter` (optional): OData $filter parameter for filtering results
- `orderby` (optional): OData $orderby parameter for sorting results
- `top` (optional): OData $top parameter for limiting results
- `skip` (optional): OData $skip parameter for pagination
- `count` (optional): OData $count parameter to include count in response
- `search` (optional): OData $search parameter for free-text search
- `expand` (optional): OData $expand parameter to include related entities

### `get_dimensions`

Retrieves all dimensions for a specific dataset.

**Parameters:**

- `catalog` (required): Catalog identifier
- `dataset` (required): Dataset identifier

### `get_dimension_values`

Retrieves all values for a specific dimension with filtering and sorting options.

**Parameters:**

- `catalog` (required): Catalog identifier
- `dataset` (required): Dataset identifier
- `dimension` (required): Dimension identifier
- `select` (optional): OData $select parameter to choose specific fields
- `filter` (optional): Additional OData $filter parameter for filtering results
- `orderby` (optional): OData $orderby parameter for sorting results
- `top` (optional): OData $top parameter for limiting results

### `get_observations`

Retrieves observations from a specific dataset with optional filters.

**Parameters:**

- `catalog` (required): Catalog identifier
- `dataset` (required): Dataset identifier
- `filters` (optional): Filters as key-value pairs
- `limit` (optional): Maximum number of observations to return

### `query_observations`

Queries observations with advanced OData query options to filter, sort, and shape results.

**Parameters:**

- `catalog` (required): Catalog identifier
- `dataset` (required): Dataset identifier
- `select` (optional): OData $select parameter to choose specific fields
- `filter` (optional): OData $filter parameter for filtering results
- `orderby` (optional): OData $orderby parameter for sorting results
- `top` (optional): OData $top parameter for limiting results
- `skip` (optional): OData $skip parameter for pagination
- `count` (optional): OData $count parameter to include count in response
- `search` (optional): OData $search parameter for free-text search
- `expand` (optional): OData $expand parameter to include related entities

### `get_metadata`

Retrieves the OData service metadata document for the CBS API.

**Parameters:**

- None

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

---

© 2025 David Stotijn — This project is licensed under the [Apache License 2.0](/LICENSE)
