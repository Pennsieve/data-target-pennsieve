# data-target-pennsieve

A Go service that uploads processed data files to the [Pennsieve](https://pennsieve.io) platform. It runs as either an AWS Lambda function or an ECS task, automatically detecting the runtime environment.

## How It Works

1. Discovers files in the configured input directory
2. Creates an upload manifest via the Pennsieve API
3. Registers each file in the manifest (preserving directory structure)
4. Authenticates via AWS Cognito to obtain temporary S3 credentials
5. Uploads files to S3 using multipart uploads with SHA256 checksums

## Prerequisites

- Go 1.23.6+
- Docker (for container builds)
- AWS credentials configured for deployment

## Building

```bash
# Build locally
make build

# Build Docker image
make docker-build

# Build and push Docker image
make docker-push
```

## Configuration

### Required Environment Variables

| Variable | Description |
|---|---|
| `INPUT_DIR` | Directory containing files to upload |
| `CALLBACK_TOKEN` | Workflow callback token (used for `Callback workflow-service:{runId}:{token}` auth) |
| `DATASET_ID` | Target dataset ID |
| `EXECUTION_RUN_ID` | Unique execution identifier |

### Optional Environment Variables

| Variable | Description | Default |
|---|---|---|
| `TARGET_FOLDER` | Destination folder in dataset | |
| `TARGET_TYPE` | Upload target type | |
| `ORGANIZATION_ID` | Organization ID for S3 tagging | |
| `PENNSIEVE_API_HOST2` | API host for manifest operations | |
| `DEPLOYMENT_MODE` | Environment indicator | |

> **No upload bucket needed.** Files are uploaded **directly to the destination
> storage bucket**: the processor calls `POST /upload/manifest/storage-credentials`
> to get short-lived STS credentials plus the bucket and key prefix
> (`O{org}/D{ds}/{manifest}`), uploads to `{keyPrefix}/{uploadId}`, then calls
> `POST /upload/manifest/files/finalize` so the server verifies the objects and
> creates the package rows. There is no user-supplied `UPLOAD_BUCKET`.

## Running

### As an ECS task / locally

Set the required environment variables, then:

```bash
./data-target-pennsieve
```

### As a Lambda function

The service detects the `AWS_LAMBDA_RUNTIME_API` environment variable and automatically switches to Lambda mode. It accepts a JSON payload:

```json
{
  "inputDir": "/mnt/efs/input",
  "executionRunId": "run-123",
  "sessionToken": "...",
  "datasetId": "N:dataset:...",
  "refreshToken": "..."
}
```

And returns:

```json
{
  "status": "success",
  "executionRunId": "run-123"
}
```