// Copyright 2025 David Stotijn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dstotijn/go-mcp"
)

// Command-line flags.
var (
	httpAddr string
	useStdio bool
	useSSE   bool
)

const (
	baseURL = "https://datasets.cbs.nl/odata/v1"
)

// Client represents a CBS API client.
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// Catalog represents a CBS data catalog.
type Catalog struct {
	Identifier   string `json:"identifier"`
	Index        int    `json:"index"`
	Title        string `json:"title"`
	Description  string `json:"description,omitempty"`
	Publisher    string `json:"publisher,omitempty"`
	Language     string `json:"language,omitempty"`
	License      string `json:"license,omitempty"`
	Homepage     string `json:"homepage,omitempty"`
	Authority    string `json:"authority,omitempty"`
	ContactPoint string `json:"contactPoint,omitempty"`
}

// CatalogResponse represents the response from the Catalogs endpoint.
type CatalogResponse struct {
	Value []Catalog `json:"value"`
}

// Dataset represents a CBS dataset.
type Dataset struct {
	ID                   string    `json:"id,omitempty"`
	Identifier           string    `json:"identifier"`
	Title                string    `json:"title"`
	Description          string    `json:"-"`
	Modified             time.Time `json:"modified"`
	ReleaseDate          time.Time `json:"releaseDate"`
	ModificationDate     time.Time `json:"modificationDate"`
	Language             string    `json:"language"`
	Catalog              string    `json:"catalog"`
	Version              string    `json:"version"`
	Status               string    `json:"status"`
	ObservationsModified time.Time `json:"observationsModified"`
	ObservationCount     int64     `json:"observationCount"`
	DatasetType          string    `json:"datasetType"`
}

// DatasetResponse represents the response from the Datasets endpoint.
type DatasetResponse struct {
	Value []Dataset `json:"value"`
}

// Dimension represents a dataset dimension.
type Dimension struct {
	Identifier     string `json:"identifier"`
	Title          string `json:"title"`
	Description    string `json:"description,omitempty"`
	Kind           string `json:"kind"`
	ContainsGroups bool   `json:"containsGroups"`
	ContainsCodes  bool   `json:"containsCodes"`
}

// DimensionResponse represents the response from the Dimensions endpoint.
type DimensionResponse struct {
	Value []Dimension `json:"value"`
}

// Observation represents a data observation.
type Observation struct {
	ID     int64             `json:"ID"`
	Values map[string]string `json:"-"` // Dynamic fields based on dimensions.
}

func main() {
	log.SetFlags(0)

	flag.StringVar(&httpAddr, "http", ":8080", "HTTP listen address for JSON-RPC over HTTP")
	flag.BoolVar(&useStdio, "stdio", true, "Enable stdio transport")
	flag.BoolVar(&useSSE, "sse", false, "Enable SSE transport")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	transports := []string{}
	opts := []mcp.ServerOption{}

	if useStdio {
		transports = append(transports, "stdio")
		opts = append(opts, mcp.WithStdioTransport())
	}

	var sseURL url.URL

	if useSSE {
		transports = append(transports, "sse")

		host := "localhost"

		hostPart, port, err := net.SplitHostPort(httpAddr)
		if err != nil {
			log.Fatalf("Failed to split host and port: %v", err)
		}

		if hostPart != "" {
			host = hostPart
		}

		sseURL = url.URL{
			Scheme: "http",
			Host:   host + ":" + port,
		}

		opts = append(opts, mcp.WithSSETransport(sseURL))
	}

	mcpServer := mcp.NewServer(mcp.ServerConfig{}, opts...)

	mcpServer.Start(ctx)

	// Register CBS API tools
	mcpServer.RegisterTools(
		createGetCatalogsTools(),
		createGetDimensionsTools(),
		createGetObservationsTools(),
		createQueryDatasetsTools(),
		createQueryObservationsTools(),
		createGetMetadataTools(),
		createGetDimensionValuesTools(),
	)

	httpServer := &http.Server{
		Addr:        httpAddr,
		Handler:     mcpServer,
		BaseContext: func(l net.Listener) context.Context { return ctx },
	}

	if useSSE {
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("HTTP server error: %v", err)
			}
		}()
	}

	log.Printf("CBS API MCP server started, using transports: %v", transports)
	if useSSE {
		log.Printf("SSE transport endpoint: %v", sseURL.String())
	}

	// Wait for interrupt signal.
	<-ctx.Done()
	// Restore signal, allowing "force quit".
	stop()

	timeout := 5 * time.Second
	cancelContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("Shutting down server (waiting %s). Press Ctrl+C to force quit.", timeout)

	var wg sync.WaitGroup

	if useSSE {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpServer.Shutdown(cancelContext); err != nil && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("HTTP server shutdown error: %v", err)
			}
		}()
	}

	wg.Wait()
}

// createGetCatalogsTools creates a tool to retrieve CBS data catalogs.
func createGetCatalogsTools() mcp.Tool {
	type GetCatalogsParams struct{}

	return mcp.CreateTool(mcp.ToolDef[GetCatalogsParams]{
		Name:        "get_catalogs",
		Description: "Retrieve all available CBS data catalogs",
		HandleFunc: func(ctx context.Context, params GetCatalogsParams) *mcp.CallToolResult {
			client := NewClient()
			catalogs, err := client.GetCatalogs()
			if err != nil {
				return newToolCallErrorResult("Failed to retrieve catalogs: %v", err)
			}

			catalogsJSON, err := json.Marshal(catalogs)
			if err != nil {
				return newToolCallErrorResult("Failed to format catalogs: %v", err)
			}

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					mcp.TextContent{
						Text: fmt.Sprintf("Found %d catalogs:\n\n```json\n%s\n```", len(catalogs), string(catalogsJSON)),
					},
				},
			}
		},
	})
}

// createGetDimensionsTools creates a tool to retrieve dimensions for a dataset.
func createGetDimensionsTools() mcp.Tool {
	type GetDimensionsParams struct {
		// The catalog identifier
		Catalog string `json:"catalog"`
		// The dataset identifier
		Dataset string `json:"dataset"`
	}

	return mcp.CreateTool(mcp.ToolDef[GetDimensionsParams]{
		Name:        "get_dimensions",
		Description: "Retrieve all dimensions for a specific CBS dataset",
		HandleFunc: func(ctx context.Context, params GetDimensionsParams) *mcp.CallToolResult {
			if params.Catalog == "" {
				return newToolCallErrorResult("Catalog identifier is required")
			}
			if params.Dataset == "" {
				return newToolCallErrorResult("Dataset identifier is required")
			}

			client := NewClient()
			dimensions, err := client.GetDimensions(params.Catalog, params.Dataset)
			if err != nil {
				return newToolCallErrorResult("Failed to retrieve dimensions: %v", err)
			}

			dimensionsJSON, err := json.Marshal(dimensions)
			if err != nil {
				return newToolCallErrorResult("Failed to format dimensions: %v", err)
			}

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					mcp.TextContent{
						Text: fmt.Sprintf("Found %d dimensions for dataset '%s' in catalog '%s':\n\n```json\n%s\n```",
							len(dimensions), params.Dataset, params.Catalog, string(dimensionsJSON)),
					},
				},
			}
		},
	})
}

// createGetObservationsTools creates a tool to retrieve observations from a dataset.
func createGetObservationsTools() mcp.Tool {
	type GetObservationsParams struct {
		// The catalog identifier
		Catalog string `json:"catalog"`
		// The dataset identifier
		Dataset string `json:"dataset"`
		// Optional filters as key-value pairs
		Filters map[string]string `json:"filters,omitempty"`
		// Maximum number of observations to return
		Limit int `json:"limit,omitempty"`
	}

	return mcp.CreateTool(mcp.ToolDef[GetObservationsParams]{
		Name:        "get_observations",
		Description: "Retrieve observations from a specific CBS dataset with optional filters",
		HandleFunc: func(ctx context.Context, params GetObservationsParams) *mcp.CallToolResult {
			if params.Catalog == "" {
				return newToolCallErrorResult("Catalog identifier is required")
			}
			if params.Dataset == "" {
				return newToolCallErrorResult("Dataset identifier is required")
			}

			client := NewClient()
			observations, err := client.GetObservations(params.Catalog, params.Dataset, params.Filters)
			if err != nil {
				return newToolCallErrorResult("Failed to retrieve observations: %v", err)
			}

			// Apply limit if specified
			limit := len(observations)
			if params.Limit > 0 && params.Limit < limit {
				limit = params.Limit
			}

			// Truncate observations to the limit
			limitedObservations := observations
			if limit < len(observations) {
				limitedObservations = observations[:limit]
			}

			observationsJSON, err := json.Marshal(limitedObservations)
			if err != nil {
				return newToolCallErrorResult("Failed to format observations: %v", err)
			}

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					mcp.TextContent{
						Text: fmt.Sprintf("Retrieved %d observations (showing %d) from dataset '%s' in catalog '%s':\n\n```json\n%s\n```",
							len(observations), limit, params.Dataset, params.Catalog, string(observationsJSON)),
					},
				},
			}
		},
	})
}

func newToolCallErrorResult(format string, args ...any) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.TextContent{
				Text: fmt.Sprintf(format, args...),
			},
		},
		IsError: true,
	}
}

// NewClient creates a new CBS API client.
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: baseURL,
	}
}

// GetCatalogs retrieves all available catalogs.
func (c *Client) GetCatalogs() ([]Catalog, error) {
	url := c.baseURL + "/Catalogs"

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get catalogs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var catalogResp CatalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&catalogResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal catalogs: %w", err)
	}

	return catalogResp.Value, nil
}

// GetDimensions retrieves all dimensions for a dataset.
func (c *Client) GetDimensions(catalog, identifier string) ([]Dimension, error) {
	url := fmt.Sprintf("%s/%s/%s/Dimensions", c.baseURL, catalog, identifier)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get dimensions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var dimensionResp DimensionResponse
	if err := json.NewDecoder(resp.Body).Decode(&dimensionResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal dimensions: %w", err)
	}

	return dimensionResp.Value, nil
}

// GetObservations retrieves observations for a dataset with optional filters.
func (c *Client) GetObservations(catalog, identifier string, filters map[string]string) ([]map[string]any, error) {
	baseURL := fmt.Sprintf("%s/%s/%s/Observations", c.baseURL, catalog, identifier)

	// Build URL with query parameters.
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add filters as query parameters.
	if len(filters) > 0 {
		query := "$filter="
		first := true
		for key, value := range filters {
			if !first {
				query += " and "
			}
			query += fmt.Sprintf("%s eq '%s'", key, value)
			first = false
		}

		q := u.Query()
		q.Set("$filter", query)
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get observations: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal observations: %w", err)
	}

	// Extract the observations from the response
	observations, ok := result["value"].([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	// Convert to a slice of maps
	var observationMaps []map[string]any
	for _, obs := range observations {
		if obsMap, ok := obs.(map[string]any); ok {
			observationMaps = append(observationMaps, obsMap)
		}
	}

	return observationMaps, nil
}

// GetDatasetsWithQuery retrieves datasets with advanced OData query options.
func (c *Client) GetDatasetsWithQuery(catalog string, queryOptions map[string]string) ([]Dataset, int, error) {
	baseURL := fmt.Sprintf("%s/%s/Datasets", c.baseURL, catalog)

	// Build URL with query parameters.
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add OData query parameters.
	q := u.Query()
	for key, value := range queryOptions {
		if strings.HasPrefix(key, "$") {
			q.Set(key, value)
		} else {
			q.Set("$"+key, value)
		}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get datasets: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var datasetResp DatasetResponse
	if err := json.NewDecoder(resp.Body).Decode(&datasetResp); err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal datasets: %w", err)
	}

	// Get total count from OData-Count header if available.
	totalCount := len(datasetResp.Value)
	if countHeader := resp.Header.Get("OData-Count"); countHeader != "" {
		if count, err := strconv.Atoi(countHeader); err == nil {
			totalCount = count
		}
	}

	return datasetResp.Value, totalCount, nil
}

// GetObservationsWithQuery retrieves observations with advanced OData query options.
func (c *Client) GetObservationsWithQuery(catalog, identifier string, queryOptions map[string]string) ([]map[string]any, error) {
	baseURL := fmt.Sprintf("%s/%s/%s/Observations", c.baseURL, catalog, identifier)

	// Build URL with query parameters.
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add OData query parameters.
	q := u.Query()
	for key, value := range queryOptions {
		if strings.HasPrefix(key, "$") {
			q.Set(key, value)
		} else {
			q.Set("$"+key, value)
		}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get observations: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal observations: %w", err)
	}

	// Extract the observations from the response
	observations, ok := result["value"].([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	// Convert to a slice of maps
	var observationMaps []map[string]any
	for _, obs := range observations {
		if obsMap, ok := obs.(map[string]any); ok {
			observationMaps = append(observationMaps, obsMap)
		}
	}

	return observationMaps, nil
}

// GetMetadata retrieves the metadata document for the OData service.
func (c *Client) GetMetadata() (string, error) {
	url := c.baseURL + "/$metadata"

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/xml")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read metadata: %w", err)
	}

	return string(data), nil
}

// GetDimensionValues retrieves all values for a specific dimension.
func (c *Client) GetDimensionValues(catalog, dataset, dimension string, queryOptions map[string]string) ([]map[string]any, error) {
	baseURL := fmt.Sprintf("%s/%s/%s/DimensionValues", c.baseURL, catalog, dataset)

	// Build URL with query parameters.
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add dimension filter
	filterQuery := fmt.Sprintf("Dimension eq '%s'", dimension)

	// Add OData query parameters
	q := u.Query()
	q.Set("$filter", filterQuery)
	for key, value := range queryOptions {
		if key != "filter" && key != "$filter" { // Skip filter as we already set it
			if strings.HasPrefix(key, "$") {
				q.Set(key, value)
			} else {
				q.Set("$"+key, value)
			}
		}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get dimension values: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal dimension values: %w", err)
	}

	// Extract the values from the response.
	values, ok := result["value"].([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	// Convert to a slice of maps.
	var valuesMaps []map[string]any
	for _, val := range values {
		if valMap, ok := val.(map[string]any); ok {
			valuesMaps = append(valuesMaps, valMap)
		}
	}

	return valuesMaps, nil
}

// ExecuteQuery executes an arbitrary OData query against a specific endpoint.
func (c *Client) ExecuteQuery(path string, queryOptions map[string]string) (map[string]any, error) {
	baseURL := c.baseURL + path

	// Build URL with query parameters
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add OData query parameters
	q := u.Query()
	for key, value := range queryOptions {
		if strings.HasPrefix(key, "$") {
			q.Set(key, value)
		} else {
			q.Set("$"+key, value)
		}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query results: %w", err)
	}

	return result, nil
}

// createQueryDatasetsTools creates a tool to query datasets with advanced OData options.
func createQueryDatasetsTools() mcp.Tool {
	type QueryDatasetsParams struct {
		// The catalog identifier. For now, just use "CBS"
		Catalog string `json:"catalog"`
		// OData $select parameter to choose specific fields
		Select string `json:"select,omitempty"`
		// OData $filter parameter for filtering results
		Filter string `json:"filter,omitempty"`
		// OData $orderby parameter for sorting results
		OrderBy string `json:"orderby,omitempty"`
		// OData $top parameter for limiting results
		Top int `json:"top,omitempty"`
		// OData $skip parameter for pagination
		Skip int `json:"skip,omitempty"`
		// OData $count parameter to include count in response
		Count bool `json:"count,omitempty"`
		// OData $search parameter for free-text search
		Search string `json:"search,omitempty"`
		// OData $expand parameter to include related entities
		Expand string `json:"expand,omitempty"`
	}

	return mcp.CreateTool(mcp.ToolDef[QueryDatasetsParams]{
		Name: "query_datasets",
		Description: `
Query datasets with advanced OData query options. Make sure to use pagination query params
to avoid exceeding the token limits. For now, just use 'CBS' as the catalog identifier. Make sure
to always include a "filter" parameter to exclude items where "Status" is "Gediscontinueerd".`,
		HandleFunc: func(ctx context.Context, params QueryDatasetsParams) *mcp.CallToolResult {
			if params.Catalog == "" {
				return newToolCallErrorResult("Catalog identifier is required")
			}

			// Build query options map from parameters
			queryOptions := make(map[string]string)

			if params.Select != "" {
				queryOptions["$select"] = params.Select
			}

			if params.Filter != "" {
				queryOptions["$filter"] = params.Filter
			}

			if params.OrderBy != "" {
				queryOptions["$orderby"] = params.OrderBy
			}

			if params.Top > 0 {
				queryOptions["$top"] = strconv.Itoa(params.Top)
			}

			if params.Skip > 0 {
				queryOptions["$skip"] = strconv.Itoa(params.Skip)
			}

			if params.Count {
				queryOptions["$count"] = "true"
			}

			if params.Search != "" {
				queryOptions["$search"] = params.Search
			}

			if params.Expand != "" {
				queryOptions["$expand"] = params.Expand
			}

			client := NewClient()
			datasets, totalCount, err := client.GetDatasetsWithQuery(params.Catalog, queryOptions)
			if err != nil {
				return newToolCallErrorResult("Failed to query datasets: %v", err)
			}

			// Format datasets as a concise list instead of raw JSON
			var datasetList strings.Builder
			for i, dataset := range datasets {
				datasetList.WriteString(fmt.Sprintf("%d. %s (ID: %s)\n", i+1, dataset.Title, dataset.Identifier))
				datasetList.WriteString(fmt.Sprintf("- Modified: %s\n", dataset.Modified.Format("2006-01-02")))
				datasetList.WriteString(fmt.Sprintf("- Status: %s\n", dataset.Status))
				datasetList.WriteString(fmt.Sprintf("- Type: %s\n", dataset.DatasetType))
				datasetList.WriteString(fmt.Sprintf("- Observations: %d\n", dataset.ObservationCount))
				datasetList.WriteString("\n")
			}

			// Calculate pagination information if relevant
			paginationInfo := ""
			if params.Top > 0 {
				paginationInfo = fmt.Sprintf("Retrieved %d datasets (total count: %d)", len(datasets), totalCount)

				// Add navigation hints if paging
				if params.Skip > 0 || len(datasets) == params.Top {
					paginationInfo += "\n\n"

					if params.Skip > 0 {
						paginationInfo += fmt.Sprintf("Previous page: skip=%d, top=%d\n",
							int(math.Max(0, float64(params.Skip-params.Top))), params.Top)
					}

					if len(datasets) == params.Top {
						paginationInfo += fmt.Sprintf("Next page: skip=%d, top=%d",
							params.Skip+params.Top, params.Top)
					}
				}
			}

			// Provide query info
			queryInfo := "Applied query options:\n"
			for k, v := range queryOptions {
				queryInfo += fmt.Sprintf("- %s: %s\n", k, v)
			}

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					mcp.TextContent{
						Text: fmt.Sprintf("%s\n\n%s\n\nDatasets from catalog '%s':\n\n%s",
							paginationInfo, queryInfo, params.Catalog, datasetList.String()),
					},
				},
			}
		},
	})
}

// createQueryObservationsTools creates a tool to query observations with advanced OData options.
func createQueryObservationsTools() mcp.Tool {
	type QueryObservationsParams struct {
		// The catalog identifier
		Catalog string `json:"catalog"`
		// The dataset identifier
		Dataset string `json:"dataset"`
		// OData $select parameter to choose specific fields
		Select string `json:"select,omitempty"`
		// OData $filter parameter for filtering results
		Filter string `json:"filter,omitempty"`
		// OData $orderby parameter for sorting results
		OrderBy string `json:"orderby,omitempty"`
		// OData $top parameter for limiting results
		Top int `json:"top,omitempty"`
		// OData $skip parameter for pagination
		Skip int `json:"skip,omitempty"`
		// OData $count parameter to include count in response
		Count bool `json:"count,omitempty"`
		// OData $search parameter for free-text search
		Search string `json:"search,omitempty"`
		// OData $expand parameter to include related entities
		Expand string `json:"expand,omitempty"`
	}

	return mcp.CreateTool(mcp.ToolDef[QueryObservationsParams]{
		Name:        "query_observations",
		Description: "Query observations with advanced OData query options to filter, sort, and shape results",
		HandleFunc: func(ctx context.Context, params QueryObservationsParams) *mcp.CallToolResult {
			if params.Catalog == "" {
				return newToolCallErrorResult("Catalog identifier is required")
			}
			if params.Dataset == "" {
				return newToolCallErrorResult("Dataset identifier is required")
			}

			// Build query options map from parameters
			queryOptions := make(map[string]string)

			if params.Select != "" {
				queryOptions["$select"] = params.Select
			}

			if params.Filter != "" {
				queryOptions["$filter"] = params.Filter
			}

			if params.OrderBy != "" {
				queryOptions["$orderby"] = params.OrderBy
			}

			if params.Top > 0 {
				queryOptions["$top"] = strconv.Itoa(params.Top)
			}

			if params.Skip > 0 {
				queryOptions["$skip"] = strconv.Itoa(params.Skip)
			}

			if params.Count {
				queryOptions["$count"] = "true"
			}

			if params.Search != "" {
				queryOptions["$search"] = params.Search
			}

			if params.Expand != "" {
				queryOptions["$expand"] = params.Expand
			}

			client := NewClient()
			observations, err := client.GetObservationsWithQuery(params.Catalog, params.Dataset, queryOptions)
			if err != nil {
				return newToolCallErrorResult("Failed to query observations: %v", err)
			}

			// Limit the output size for very large result sets
			limit := len(observations)
			if params.Top > 0 && params.Top < limit {
				limit = params.Top
			} else if limit > 100 {
				limit = 100 // Default limit to prevent huge responses
			}

			limitedObservations := observations
			if limit < len(observations) {
				limitedObservations = observations[:limit]
			}

			observationsJSON, err := json.Marshal(limitedObservations)
			if err != nil {
				return newToolCallErrorResult("Failed to format observations: %v", err)
			}

			// Provide query info
			queryInfo := "Applied query options:\n"
			for k, v := range queryOptions {
				queryInfo += fmt.Sprintf("- %s: %s\n", k, v)
			}

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					mcp.TextContent{
						Text: fmt.Sprintf("Retrieved %d observations (showing %d) from dataset '%s' in catalog '%s'\n\n%s\n\n```json\n%s\n```",
							len(observations), limit, params.Dataset, params.Catalog, queryInfo, string(observationsJSON)),
					},
				},
			}
		},
	})
}

// createGetMetadataTools creates a tool to retrieve the OData service metadata.
func createGetMetadataTools() mcp.Tool {
	type GetMetadataParams struct {
		// No parameters needed for this tool
	}

	return mcp.CreateTool(mcp.ToolDef[GetMetadataParams]{
		Name:        "get_metadata",
		Description: "Retrieve the OData service metadata document for the CBS API",
		HandleFunc: func(ctx context.Context, params GetMetadataParams) *mcp.CallToolResult {
			client := NewClient()
			metadata, err := client.GetMetadata()
			if err != nil {
				return newToolCallErrorResult("Failed to retrieve metadata: %v", err)
			}

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					mcp.TextContent{
						Text: fmt.Sprintf("OData service metadata:\n\n```xml\n%s\n```", metadata),
					},
				},
			}
		},
	})
}

// createGetDimensionValuesTools creates a tool to retrieve all values for a dimension.
func createGetDimensionValuesTools() mcp.Tool {
	type GetDimensionValuesParams struct {
		// The catalog identifier
		Catalog string `json:"catalog"`
		// The dataset identifier
		Dataset string `json:"dataset"`
		// The dimension identifier
		Dimension string `json:"dimension"`
		// OData $select parameter to choose specific fields
		Select string `json:"select,omitempty"`
		// Additional OData $filter parameter for filtering results
		Filter string `json:"filter,omitempty"`
		// OData $orderby parameter for sorting results
		OrderBy string `json:"orderby,omitempty"`
		// OData $top parameter for limiting results
		Top int `json:"top,omitempty"`
	}

	return mcp.CreateTool(mcp.ToolDef[GetDimensionValuesParams]{
		Name:        "get_dimension_values",
		Description: "Retrieve all values for a specific dimension with filtering and sorting options",
		HandleFunc: func(ctx context.Context, params GetDimensionValuesParams) *mcp.CallToolResult {
			if params.Catalog == "" {
				return newToolCallErrorResult("Catalog identifier is required")
			}
			if params.Dataset == "" {
				return newToolCallErrorResult("Dataset identifier is required")
			}
			if params.Dimension == "" {
				return newToolCallErrorResult("Dimension identifier is required")
			}

			// Build query options map from parameters
			queryOptions := make(map[string]string)

			if params.Select != "" {
				queryOptions["$select"] = params.Select
			}

			if params.Filter != "" {
				queryOptions["$filter"] = params.Filter
			}

			if params.OrderBy != "" {
				queryOptions["$orderby"] = params.OrderBy
			}

			if params.Top > 0 {
				queryOptions["$top"] = strconv.Itoa(params.Top)
			}

			client := NewClient()
			values, err := client.GetDimensionValues(params.Catalog, params.Dataset, params.Dimension, queryOptions)
			if err != nil {
				return newToolCallErrorResult("Failed to retrieve dimension values: %v", err)
			}

			valuesJSON, err := json.Marshal(values)
			if err != nil {
				return newToolCallErrorResult("Failed to format dimension values: %v", err)
			}

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					mcp.TextContent{
						Text: fmt.Sprintf("Retrieved %d values for dimension '%s' in dataset '%s' (catalog '%s'):\n\n```json\n%s\n```",
							len(values), params.Dimension, params.Dataset, params.Catalog, string(valuesJSON)),
					},
				},
			}
		},
	})
}
