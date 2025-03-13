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
	"log"
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

// Client represents a CBS API client
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// Catalog represents a CBS data catalog
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

// CatalogResponse represents the response from the Catalogs endpoint
type CatalogResponse struct {
	Value []Catalog `json:"value"`
}

// Dataset represents a CBS dataset
type Dataset struct {
	ID                   string    `json:"id,omitempty"`
	Identifier           string    `json:"identifier"`
	Title                string    `json:"title"`
	Description          string    `json:"description,omitempty"`
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

// DatasetResponse represents the response from the Datasets endpoint
type DatasetResponse struct {
	Value []Dataset `json:"value"`
}

// Dimension represents a dataset dimension
type Dimension struct {
	Identifier     string `json:"identifier"`
	Title          string `json:"title"`
	Description    string `json:"description,omitempty"`
	Kind           string `json:"kind"`
	ContainsGroups bool   `json:"containsGroups"`
	ContainsCodes  bool   `json:"containsCodes"`
}

// DimensionResponse represents the response from the Dimensions endpoint
type DimensionResponse struct {
	Value []Dimension `json:"value"`
}

// Observation represents a data observation
type Observation struct {
	ID     int64             `json:"ID"`
	Values map[string]string `json:"-"` // Dynamic fields based on dimensions
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
		createGetDatasetsTools(),
		createGetDimensionsTools(),
		createGetObservationsTools(),
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

// createGetCatalogsTools creates a tool to retrieve CBS data catalogs
func createGetCatalogsTools() mcp.Tool {
	type GetCatalogsParams struct {
		// No parameters needed for this tool
	}

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

// createGetDatasetsTools creates a tool to retrieve datasets from a catalog
func createGetDatasetsTools() mcp.Tool {
	type GetDatasetsParams struct {
		// The catalog identifier to retrieve datasets from
		Catalog string `json:"catalog"`
		// The page number (1-based) for pagination
		Page int `json:"page,omitempty"`
		// The number of datasets per page
		PageSize int `json:"page_size,omitempty"`
	}

	return mcp.CreateTool(mcp.ToolDef[GetDatasetsParams]{
		Name:        "get_datasets",
		Description: "Retrieve datasets from a specific CBS catalog with pagination support",
		HandleFunc: func(ctx context.Context, params GetDatasetsParams) *mcp.CallToolResult {
			if params.Catalog == "" {
				return newToolCallErrorResult("Catalog identifier is required")
			}

			// Set default pagination values if not provided
			page := params.Page
			if page <= 0 {
				page = 1
			}

			pageSize := params.PageSize
			if pageSize <= 0 {
				pageSize = 20 // Default page size
			}

			client := NewClient()
			datasets, totalCount, err := client.GetDatasets(params.Catalog, page, pageSize)
			if err != nil {
				return newToolCallErrorResult("Failed to retrieve datasets: %v", err)
			}

			datasetsJSON, err := json.Marshal(datasets)
			if err != nil {
				return newToolCallErrorResult("Failed to format datasets: %v", err)
			}

			// Calculate pagination information
			totalPages := (totalCount + pageSize - 1) / pageSize
			hasNextPage := page < totalPages
			hasPrevPage := page > 1

			paginationInfo := fmt.Sprintf(
				"Page %d of %d (showing %d of %d total datasets)",
				page, totalPages, len(datasets), totalCount,
			)

			// Add navigation hints
			var navHints []string
			if hasPrevPage {
				navHints = append(navHints, fmt.Sprintf("For previous page, use page=%d", page-1))
			}
			if hasNextPage {
				navHints = append(navHints, fmt.Sprintf("For next page, use page=%d", page+1))
			}

			navInfo := ""
			if len(navHints) > 0 {
				navInfo = "\n\n" + strings.Join(navHints, "\n")
			}

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					mcp.TextContent{
						Text: fmt.Sprintf("%s\n\nFound datasets in catalog '%s':\n\n```json\n%s\n```%s",
							paginationInfo, params.Catalog, string(datasetsJSON), navInfo),
					},
				},
			}
		},
	})
}

// createGetDimensionsTools creates a tool to retrieve dimensions for a dataset
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

// createGetObservationsTools creates a tool to retrieve observations from a dataset
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

// NewClient creates a new CBS API client
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: baseURL,
	}
}

// GetCatalogs retrieves all available catalogs
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
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var catalogResp CatalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&catalogResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal catalogs: %w", err)
	}

	return catalogResp.Value, nil
}

// GetDatasets retrieves all datasets in a catalog
func (c *Client) GetDatasets(catalog string, page, pageSize int) ([]Dataset, int, error) {
	baseURL := fmt.Sprintf("%s/%s/Datasets", c.baseURL, catalog)

	// Build URL with query parameters
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add pagination parameters
	q := u.Query()
	if pageSize > 0 {
		q.Set("$top", fmt.Sprintf("%d", pageSize))
		if page > 0 {
			q.Set("$skip", fmt.Sprintf("%d", (page-1)*pageSize))
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
		return nil, 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var datasetResp DatasetResponse
	if err := json.NewDecoder(resp.Body).Decode(&datasetResp); err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal datasets: %w", err)
	}

	// Get total count from OData-Count header if available
	totalCount := len(datasetResp.Value)
	if countHeader := resp.Header.Get("OData-Count"); countHeader != "" {
		if count, err := strconv.Atoi(countHeader); err == nil {
			totalCount = count
		}
	}

	return datasetResp.Value, totalCount, nil
}

// GetDimensions retrieves all dimensions for a dataset
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
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var dimensionResp DimensionResponse
	if err := json.NewDecoder(resp.Body).Decode(&dimensionResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal dimensions: %w", err)
	}

	return dimensionResp.Value, nil
}

// GetObservations retrieves observations for a dataset with optional filters
func (c *Client) GetObservations(catalog, identifier string, filters map[string]string) ([]map[string]interface{}, error) {
	baseURL := fmt.Sprintf("%s/%s/%s/Observations", c.baseURL, catalog, identifier)

	// Build URL with query parameters
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add filters as query parameters
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
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal observations: %w", err)
	}

	// Extract the observations from the response
	observations, ok := result["value"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	// Convert to a slice of maps
	var observationMaps []map[string]interface{}
	for _, obs := range observations {
		if obsMap, ok := obs.(map[string]interface{}); ok {
			observationMaps = append(observationMaps, obsMap)
		}
	}

	return observationMaps, nil
}
