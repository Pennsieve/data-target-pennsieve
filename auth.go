package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// StorageCredentials matches the response from POST /manifest/storage-credentials.
// These are temporary STS credentials scoped to upload directly into the
// manifest's destination storage bucket under KeyPrefix
// (O{orgId}/D{datasetId}/{manifestId}) — no user-supplied upload bucket.
type StorageCredentials struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`
	Expiration      string `json:"expiration"`
	Bucket          string `json:"bucket"`
	KeyPrefix       string `json:"keyPrefix"`
	Region          string `json:"region"`
}

// GetStorageCredentials requests STS credentials scoped to the manifest's
// destination storage bucket + O{org}/D{ds}/{manifest}/* prefix, so files are
// uploaded directly to storage (no legacy upload-bucket staging hop and no
// user-supplied bucket). Returns creds, bucket, keyPrefix and region.
func GetStorageCredentials(apiHost2, datasetID, manifestNodeID, executionRunID, callbackToken string) (aws.Credentials, string, string, string, error) {
	reqURL := fmt.Sprintf("%s/upload/manifest/storage-credentials?dataset_id=%s", apiHost2, url.QueryEscape(datasetID))

	body := fmt.Sprintf(`{"manifestNodeId":%q}`, manifestNodeID)
	req, err := http.NewRequest("POST", reqURL, strings.NewReader(body))
	if err != nil {
		return aws.Credentials{}, "", "", "", fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Callback workflow-service:%s:%s", executionRunID, callbackToken))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return aws.Credentials{}, "", "", "", fmt.Errorf("storage-credentials request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return aws.Credentials{}, "", "", "", fmt.Errorf("storage-credentials returned status %d", resp.StatusCode)
	}

	var result StorageCredentials
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return aws.Credentials{}, "", "", "", fmt.Errorf("decoding response: %w", err)
	}

	expiration, _ := time.Parse(time.RFC3339, result.Expiration)

	creds := aws.Credentials{
		AccessKeyID:     result.AccessKeyID,
		SecretAccessKey: result.SecretAccessKey,
		SessionToken:    result.SessionToken,
		CanExpire:       true,
		Expires:         expiration,
	}

	return creds, result.Bucket, result.KeyPrefix, result.Region, nil
}