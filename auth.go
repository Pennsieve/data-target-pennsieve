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

// UploadCredentialsResponse matches the response from POST /manifest/upload-credentials.
type UploadCredentialsResponse struct {
	AccessKeyID    string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken   string `json:"sessionToken"`
	Expiration     string `json:"expiration"`
	Bucket         string `json:"bucket"`
	Region         string `json:"region"`
}

// GetUploadCredentials calls the upload service to get temporary S3 credentials
// scoped to the given manifest prefix.
func GetUploadCredentials(apiHost2, datasetID, manifestNodeID, executionRunID, callbackToken string) (aws.Credentials, string, string, error) {
	reqURL := fmt.Sprintf("%s/manifest/upload-credentials?dataset_id=%s", apiHost2, url.QueryEscape(datasetID))

	body := fmt.Sprintf(`{"manifestNodeId":%q}`, manifestNodeID)
	req, err := http.NewRequest("POST", reqURL, strings.NewReader(body))
	if err != nil {
		return aws.Credentials{}, "", "", fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Callback workflow-service:%s:%s", executionRunID, callbackToken))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return aws.Credentials{}, "", "", fmt.Errorf("upload-credentials request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return aws.Credentials{}, "", "", fmt.Errorf("upload-credentials returned status %d", resp.StatusCode)
	}

	var result UploadCredentialsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return aws.Credentials{}, "", "", fmt.Errorf("decoding response: %w", err)
	}

	expiration, _ := time.Parse(time.RFC3339, result.Expiration)

	creds := aws.Credentials{
		AccessKeyID:    result.AccessKeyID,
		SecretAccessKey: result.SecretAccessKey,
		SessionToken:   result.SessionToken,
		CanExpire:      true,
		Expires:        expiration,
	}

	return creds, result.Bucket, result.Region, nil
}