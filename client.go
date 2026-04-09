package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// PennsieveClient is a minimal HTTP client for the Pennsieve API endpoints
// needed by the upload target: manifest management.
type PennsieveClient struct {
	apiHost2       string // e.g. https://api2.pennsieve.io
	executionRunID string
	callbackToken  string
	httpClient     *http.Client
}

func NewPennsieveClient(apiHost2, executionRunID, callbackToken string) *PennsieveClient {
	return &PennsieveClient{
		apiHost2:       apiHost2,
		executionRunID: executionRunID,
		callbackToken:  callbackToken,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ManifestFileDTO represents a file to register in a manifest.
type ManifestFileDTO struct {
	UploadID   string `json:"upload_id"`
	S3Key      string `json:"s3_key"`
	TargetPath string `json:"target_path"`
	TargetName string `json:"target_name"`
}

// ManifestRequest is the body for POST /manifest.
type ManifestRequest struct {
	ID        string            `json:"id"`
	DatasetID string            `json:"dataset_id"`
	Files     []ManifestFileDTO `json:"files"`
}

// ManifestResponse is the response from POST /manifest.
type ManifestResponse struct {
	ManifestNodeID string   `json:"manifest_node_id"`
	NrFilesUpdated int      `json:"nr_files_updated"`
	FailedFiles    []string `json:"failed_files"`
}

// CreateManifest creates a new upload manifest for the given dataset.
// Returns the manifest node ID.
func (c *PennsieveClient) CreateManifest(datasetID string) (string, error) {
	body := ManifestRequest{
		DatasetID: datasetID,
	}
	var result ManifestResponse
	if err := c.postManifest(datasetID, body, &result); err != nil {
		return "", fmt.Errorf("creating manifest: %w", err)
	}
	return result.ManifestNodeID, nil
}

// SyncManifest registers files in an existing manifest.
func (c *PennsieveClient) SyncManifest(manifestNodeID, datasetID string, files []ManifestFileDTO) error {
	body := ManifestRequest{
		ID:        manifestNodeID,
		DatasetID: datasetID,
		Files:     files,
	}
	var result ManifestResponse
	if err := c.postManifest(datasetID, body, &result); err != nil {
		return fmt.Errorf("syncing manifest: %w", err)
	}
	if len(result.FailedFiles) > 0 {
		return fmt.Errorf("manifest sync had %d failed files: %v", len(result.FailedFiles), result.FailedFiles)
	}
	return nil
}

func (c *PennsieveClient) postManifest(datasetID string, body interface{}, result interface{}) error {
	reqURL := fmt.Sprintf("%s/upload/manifest?dataset_id=%s", c.apiHost2, url.QueryEscape(datasetID))
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshaling manifest request: %w", err)
	}

	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("creating manifest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Callback workflow-service:%s:%s", c.executionRunID, c.callbackToken))

	return c.doJSON(req, result)
}

func (c *PennsieveClient) doJSON(req *http.Request, result interface{}) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}