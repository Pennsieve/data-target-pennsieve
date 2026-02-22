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
	SessionToken   string
	RefreshToken   string
	APIHost        string
	APIHost2       string
	DeploymentMode string
	ExecutionRunID string

	// Target-specific env vars
	DatasetID      string
	OrganizationID string
	TargetFolder   string
	TargetType     string
	UploadBucket   string
}

// LambdaEvent mirrors the per-invocation payload fields sent by the
// Step Functions Lambda invoke state. Static env vars (PENNSIEVE_API_HOST,
// PENNSIEVE_API_HOST2, DEPLOYMENT_MODE, REGION, ENVIRONMENT) are already
// set on the Lambda function configuration at creation time.
type LambdaEvent struct {
	InputDir       string `json:"inputDir"`
	ExecutionRunID string `json:"executionRunId"`
	IntegrationID  string `json:"integrationId"`
	ComputeNodeID  string `json:"computeNodeId"`
	SessionToken   string `json:"sessionToken"`
	RefreshToken   string `json:"refreshToken"`
	DatasetID      string `json:"datasetId"`
	OrganizationID string `json:"organizationId"`
	TargetFolder   string `json:"targetFolder"`
	TargetType     string `json:"targetType"`
	UploadBucket   string `json:"uploadBucket"`
}

// LambdaResponse is returned to Step Functions after the handler completes.
type LambdaResponse struct {
	Status         string `json:"status"`
	ExecutionRunID string `json:"executionRunId"`
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		InputDir:       os.Getenv("INPUT_DIR"),
		SessionToken:   os.Getenv("SESSION_TOKEN"),
		RefreshToken:   os.Getenv("REFRESH_TOKEN"),
		APIHost:        os.Getenv("PENNSIEVE_API_HOST"),
		APIHost2:       os.Getenv("PENNSIEVE_API_HOST2"),
		DeploymentMode: os.Getenv("DEPLOYMENT_MODE"),
		ExecutionRunID: os.Getenv("EXECUTION_RUN_ID"),
		DatasetID:      os.Getenv("DATASET_ID"),
		OrganizationID: os.Getenv("ORGANIZATION_ID"),
		TargetFolder:   os.Getenv("TARGET_FOLDER"),
		TargetType:     os.Getenv("TARGET_TYPE"),
		UploadBucket:   os.Getenv("UPLOAD_BUCKET"),
	}

	if cfg.InputDir == "" {
		return nil, fmt.Errorf("INPUT_DIR is required")
	}
	if cfg.SessionToken == "" {
		return nil, fmt.Errorf("SESSION_TOKEN is required")
	}
	if cfg.DatasetID == "" {
		return nil, fmt.Errorf("DATASET_ID is required")
	}
	if cfg.UploadBucket == "" {
		cfg.UploadBucket = "pennsieve-prod-uploads-v2-use1"
	}

	return cfg, nil
}

// discoverFiles walks INPUT_DIR and returns all file paths.
func discoverFiles(inputDir string) ([]string, error) {
	var files []string
	err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// run contains the core target logic shared between ECS and Lambda modes.
func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	ctx := context.Background()

	log.Printf("Starting pennsieve-upload target")
	log.Printf("  executionRunId: %s", cfg.ExecutionRunID)
	log.Printf("  inputDir:       %s", cfg.InputDir)
	log.Printf("  datasetId:      %s", cfg.DatasetID)
	log.Printf("  organizationId: %s", cfg.OrganizationID)
	log.Printf("  targetFolder:   %s", cfg.TargetFolder)
	log.Printf("  deploymentMode: %s", cfg.DeploymentMode)
	log.Printf("  apiHost:        %s", cfg.APIHost)
	log.Printf("  uploadBucket:   %s", cfg.UploadBucket)

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

	// Step 1: Create Pennsieve client
	client := NewPennsieveClient(cfg.APIHost, cfg.APIHost2, cfg.SessionToken)

	// Step 2: Get Cognito config
	log.Printf("Fetching Cognito configuration...")
	cognitoConfig, err := client.GetCognitoConfig()
	if err != nil {
		return fmt.Errorf("failed to get cognito config: %w", err)
	}
	log.Printf("Cognito config: region=%s, identityPool=%s", cognitoConfig.Region, cognitoConfig.IdentityPool.ID)

	// Step 3: Create manifest
	log.Printf("Creating upload manifest for dataset %s...", cfg.DatasetID)
	manifestNodeID, err := client.CreateManifest(cfg.DatasetID)
	if err != nil {
		return fmt.Errorf("failed to create manifest: %w", err)
	}
	log.Printf("Created manifest: %s", manifestNodeID)

	// Step 4: Build file list with UUIDs and sync to manifest
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

	// Step 5: Get AWS credentials via Cognito
	log.Printf("Refreshing Cognito credentials...")
	awsCreds, err := RefreshAndGetAWSCredentials(ctx, cognitoConfig, cfg.RefreshToken)
	if err != nil {
		return fmt.Errorf("failed to get AWS credentials: %w", err)
	}
	log.Printf("Obtained temporary AWS credentials (expires: %s)", awsCreds.Expires.Format("15:04:05"))

	// Step 6: Upload all files to S3
	log.Printf("Starting S3 upload to bucket %s...", cfg.UploadBucket)
	if err := UploadFiles(ctx, awsCreds, cfg.UploadBucket, manifestNodeID, filesToUpload, cfg.OrganizationID, cfg.DatasetID); err != nil {
		return fmt.Errorf("S3 upload failed: %w", err)
	}

	// Step 7: Summary
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
	os.Setenv("SESSION_TOKEN", event.SessionToken)
	os.Setenv("REFRESH_TOKEN", event.RefreshToken)
	os.Setenv("EXECUTION_RUN_ID", event.ExecutionRunID)
	os.Setenv("DATASET_ID", event.DatasetID)
	os.Setenv("ORGANIZATION_ID", event.OrganizationID)
	os.Setenv("TARGET_FOLDER", event.TargetFolder)
	os.Setenv("TARGET_TYPE", event.TargetType)
	if event.UploadBucket != "" {
		os.Setenv("UPLOAD_BUCKET", event.UploadBucket)
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
