package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"
)

// Config holds the environment configuration passed by the orchestrator.
type Config struct {
	// Standard env vars (same as processors)
	InputDir       string
	APIHost2       string
	DeploymentMode string
	ExecutionRunID string
	CallbackToken  string

	// Target-specific env vars
	DatasetID      string
	OrganizationID string
	TargetFolder   string
	TargetType     string
	UploadBucket   string
}

// LambdaEvent mirrors the per-invocation payload fields sent by the
// Step Functions Lambda invoke state.
type LambdaEvent struct {
	// Common fields
	InputDir       string `json:"inputDir"`
	ExecutionRunID string `json:"executionRunId"`
	IntegrationID  string `json:"integrationId"`
	ComputeNodeID  string `json:"computeNodeId"`
	CallbackToken  string `json:"callbackToken"`
	DatasetID      string `json:"datasetId"`
	OrganizationID string `json:"organizationId"`
	TargetType     string `json:"targetType"`

	// Target-type-specific params (SCREAMING_SNAKE_CASE keys → env vars)
	Params map[string]string `json:"params"`
}

// LambdaResponse is returned to Step Functions after the handler completes.
type LambdaResponse struct {
	Status         string `json:"status"`
	ExecutionRunID string `json:"executionRunId"`
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		InputDir:       os.Getenv("INPUT_DIR"),
		APIHost2:       os.Getenv("PENNSIEVE_API_HOST2"),
		DeploymentMode: os.Getenv("DEPLOYMENT_MODE"),
		ExecutionRunID: os.Getenv("EXECUTION_RUN_ID"),
		CallbackToken:  os.Getenv("CALLBACK_TOKEN"),
		DatasetID:      os.Getenv("DATASET_ID"),
		OrganizationID: os.Getenv("ORGANIZATION_ID"),
		TargetFolder:   os.Getenv("TARGET_FOLDER"),
		TargetType:     os.Getenv("TARGET_TYPE"),
		UploadBucket:   os.Getenv("UPLOAD_BUCKET"),
	}

	if cfg.InputDir == "" {
		return nil, fmt.Errorf("INPUT_DIR is required")
	}
	if cfg.CallbackToken == "" {
		return nil, fmt.Errorf("CALLBACK_TOKEN is required")
	}
	if cfg.DatasetID == "" {
		return nil, fmt.Errorf("DATASET_ID is required")
	}
	if cfg.ExecutionRunID == "" {
		return nil, fmt.Errorf("EXECUTION_RUN_ID is required")
	}

	return cfg, nil
}

// discoverFiles walks INPUT_DIR and returns all regular file paths.
// It follows symlinks so that processor output written as symlinked
// directories is included.
func discoverFiles(inputDir string) ([]string, error) {
	var files []string
	var walk func(dir string) error
	walk = func(dir string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			path := filepath.Join(dir, e.Name())
			// Resolve symlinks by using os.Stat (follows symlinks)
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if info.IsDir() {
				if err := walk(path); err != nil {
					return err
				}
			} else if info.Mode().IsRegular() {
				files = append(files, path)
			}
		}
		return nil
	}
	return files, walk(inputDir)
}

// run contains the core target logic shared between ECS and Lambda modes.
func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	log.Printf("Starting pennsieve-upload target")
	log.Printf("  executionRunId: %s", cfg.ExecutionRunID)
	log.Printf("  inputDir:       %s", cfg.InputDir)
	log.Printf("  datasetId:      %s", cfg.DatasetID)
	log.Printf("  organizationId: %s", cfg.OrganizationID)
	log.Printf("  targetFolder:   %s", cfg.TargetFolder)
	log.Printf("  deploymentMode: %s", cfg.DeploymentMode)
	log.Printf("  apiHost2:       %s", cfg.APIHost2)

	// Discover files from EFS input directory
	files, err := discoverFiles(cfg.InputDir)
	if err != nil {
		return fmt.Errorf("failed to discover files in %s: %w", cfg.InputDir, err)
	}

	if len(files) == 0 {
		log.Printf("No files found in %s, nothing to upload", cfg.InputDir)
		return nil
	}

	log.Printf("Discovered %d files to upload:", len(files))
	for _, f := range files {
		info, _ := os.Stat(f)
		size := int64(0)
		if info != nil {
			size = info.Size()
		}
		rel, _ := filepath.Rel(cfg.InputDir, f)
		log.Printf("  %s (%d bytes)", rel, size)
	}

	// Step 1: Create Pennsieve client (uses callback auth)
	client := NewPennsieveClient(cfg.APIHost2, cfg.ExecutionRunID, cfg.CallbackToken)

	// Step 2: Create manifest
	log.Printf("Creating upload manifest for dataset %s...", cfg.DatasetID)
	manifestNodeID, err := client.CreateManifest(cfg.DatasetID)
	if err != nil {
		return fmt.Errorf("failed to create manifest: %w", err)
	}
	log.Printf("Created manifest: %s", manifestNodeID)

	// Step 3: Build file list with UUIDs and sync to manifest
	filesToUpload := make([]FileToUpload, len(files))
	manifestFiles := make([]ManifestFileDTO, len(files))
	for i, f := range files {
		uploadID := uuid.New().String()
		rel, _ := filepath.Rel(cfg.InputDir, f)

		// targetPath = target folder + relative directory
		targetPath := cfg.TargetFolder
		dir := filepath.Dir(rel)
		if dir != "." && dir != "" {
			targetPath = filepath.Join(targetPath, dir)
		}

		filesToUpload[i] = FileToUpload{
			Path:     f,
			UploadID: uploadID,
		}
		manifestFiles[i] = ManifestFileDTO{
			UploadID:   uploadID,
			S3Key:      fmt.Sprintf("%s/%s", manifestNodeID, uploadID),
			TargetPath: targetPath,
			TargetName: filepath.Base(f),
		}
	}

	log.Printf("Syncing %d files to manifest...", len(manifestFiles))
	if err := client.SyncManifest(manifestNodeID, cfg.DatasetID, manifestFiles); err != nil {
		return fmt.Errorf("failed to sync manifest: %w", err)
	}
	log.Printf("Manifest synced successfully")

	// Step 4: Get scoped AWS credentials for S3 upload via upload-credentials endpoint
	log.Printf("Getting upload credentials...")
	awsCreds, bucket, region, err := GetUploadCredentials(cfg.APIHost2, cfg.DatasetID, manifestNodeID, cfg.ExecutionRunID, cfg.CallbackToken)
	if err != nil {
		return fmt.Errorf("failed to get upload credentials: %w", err)
	}
	// Use bucket from credentials response, fall back to env var
	if bucket == "" {
		bucket = cfg.UploadBucket
	}
	log.Printf("Obtained temporary upload credentials (expires: %s)", awsCreds.Expires.Format("15:04:05"))

	// Step 5: Upload all files to S3
	log.Printf("Starting S3 upload to bucket %s...", bucket)
	if err := UploadFiles(context.Background(), awsCreds, bucket, manifestNodeID, filesToUpload, cfg.OrganizationID, cfg.DatasetID, region); err != nil {
		return fmt.Errorf("S3 upload failed: %w", err)
	}

	// Step 6: Summary
	log.Printf("Upload complete: %d files uploaded to manifest %s", len(files), manifestNodeID)
	return nil
}

// lambdaHandler bridges the Lambda invocation payload to environment variables,
// then runs the same logic as ECS mode.
func lambdaHandler(ctx context.Context, event LambdaEvent) (LambdaResponse, error) {
	log.Printf("Lambda handler invoked")

	// Bridge per-invocation payload fields to env vars so the core logic
	// works identically to ECS mode.
	os.Setenv("INPUT_DIR", event.InputDir)
	os.Setenv("EXECUTION_RUN_ID", event.ExecutionRunID)
	os.Setenv("CALLBACK_TOKEN", event.CallbackToken)
	os.Setenv("DATASET_ID", event.DatasetID)
	os.Setenv("ORGANIZATION_ID", event.OrganizationID)
	os.Setenv("TARGET_TYPE", event.TargetType)

	// Bridge target-type-specific params to env vars
	for k, v := range event.Params {
		os.Setenv(k, v)
	}

	if err := run(); err != nil {
		return LambdaResponse{Status: "error", ExecutionRunID: event.ExecutionRunID}, err
	}

	return LambdaResponse{Status: "success", ExecutionRunID: event.ExecutionRunID}, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Dual-mode detection: AWS_LAMBDA_RUNTIME_API is set by the Lambda
	// service and absent on ECS/local.
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		log.Printf("Detected Lambda runtime, starting RIC handler")
		lambda.Start(lambdaHandler)
	} else {
		log.Printf("Running in ECS/local mode")
		if err := run(); err != nil {
			log.Fatalf("%v", err)
		}
	}
}