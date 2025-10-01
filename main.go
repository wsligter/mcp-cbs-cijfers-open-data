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
	"crypto/subtle"
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

		// Prefer a public base URL if provided (Azure Container Apps).
		if pub := os.Getenv("PUBLIC_BASE_URL"); pub != "" {
			u, err := url.Parse(pub)
			if err != nil {
				log.Fatalf("Bad PUBLIC_BASE_URL: %v", err)
			}
			sseURL = *u
		} else {
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
		Addr:              httpAddr,
		Handler:           requireAPIKey(mcpServer),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(l net.Listener) context.Context { return ctx },
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

	<-ctx.Done()
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

// simple apikey guard
func requireAPIKey(next http.Handler) http.Handler {
	expected := os.Getenv("API_KEY")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expected == "" {
			http.Error(w, "server misconfigured", http.StatusInternalServerError)
			return
		}
		got := r.Header.Get("X-API-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
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
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    baseURL,
	}
}

// === Tool implementations ===

// GetCatalogs retrieves all available catalogs.
func (c *Client) GetCatalogs() ([]Catalog, error) {
	url := c.baseURL + "/Catalogs"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
	}
	var catalogResp CatalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&catalogResp); err != nil {
		return nil, err
	}
	return catalogResp.Value, nil
}

// GetDimensions retrieves all dimensions for a dataset.
func (c *Client) GetDimensions(catalog, identifier string) ([]Dimension, error) {
	url := fmt.Sprintf("%s/%s/%s/Dimensions", c.baseURL, catalog, identifier)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
	}
	var dimensionResp DimensionResponse
	if err := json.NewDecoder(resp.Body).Decode(&dimensionResp); err != nil {
		return nil, err
	}
	return dimensionResp.Value, nil
}

// GetObservations retrieves observations for a dataset with optional filters.
func (c *Client) GetObservations(catalog, identifier string, filters map[string]string) ([]map[string]any, error) {
	obsURL := fmt.Sprintf("%s/%s/%s/Observations", c.baseURL, catalog, identifier)
	u, err := url.Parse(obsURL)
	if err != nil {
		return nil, err
	}
	if len(filters) > 0 {
		var parts []string
		for k, v := range filters {
			parts = append(parts, fmt.Sprintf("%s eq '%s'", k, v))
		}
		q := u.Query()
		q.Set("$filter", strings.Join(parts, " and "))
		u.RawQuery = q.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
	}
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	values, ok := result["value"].([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}
	var observationMaps []map[string]any
	for _, obs := range values {
		if obsMap, ok := obs.(map[string]any); ok {
			observationMaps = append(observationMaps, obsMap)
		}
	}
	return observationMaps, nil
}

// GetDatasetsWithQuery retrieves datasets with advanced OData query options.
func (c *Client) GetDatasetsWithQuery(catalog string, queryOptions map[string]string) ([]Dataset, int, error) {
	baseURL := fmt.Sprintf("%s/%s/Datasets", c.baseURL, catalog)
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, 0, err
	}
	q := u.Query()
	for k, v := range queryOptions {
		if strings.HasPrefix(k, "$") {
			q.Set(k, v)
		} else {
			q.Set("$"+k, v)
		}
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
	}
	var payload struct {
		Value []Dataset `json:"value"`
		Count *int      `json:"@odata.count,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, 0, err
	}
	totalCount := len(payload.Value)
	if payload.Count != nil {
		totalCount = *payload.Count
	}
	return payload.Value, totalCount, nil
}

// (other tool functions remain unchanged, using NewClient and helpers above)

// TODO: Add createGetCatalogsTools, createGetDimensionsTools, createGetObservationsTools, 
// createQueryDatasetsTools, createQueryObservationsTools, createGetMetadataTools, 
// createGetDimensionValuesTools as in your current code.
