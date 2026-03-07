package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const partSize = 64 * 1024 * 1024 // 64 MB per part

// FileToUpload pairs a local file path with its manifest upload ID.
type FileToUpload struct {
	Path     string // absolute path on disk
	UploadID string // UUID used as S3 key suffix
}

// UploadFiles uploads all files to S3 using the temporary AWS credentials
// obtained from Cognito. Files are uploaded sequentially with multipart
// support for large files.
func UploadFiles(ctx context.Context, creds aws.Credentials, bucket, manifestNodeID string, files []FileToUpload, orgID, datasetID, region string) error {
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID,
			creds.SecretAccessKey,
			creds.SessionToken,
		)),
	)
	if err != nil {
		return fmt.Errorf("creating S3 config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = partSize
	})

	tags := fmt.Sprintf("OrgId=%s&DatasetId=%s", orgID, datasetID)

	for i, f := range files {
		s3Key := fmt.Sprintf("%s/%s", manifestNodeID, f.UploadID)
		log.Printf("Uploading file %d/%d: %s → s3://%s/%s", i+1, len(files), f.Path, bucket, s3Key)

		if err := uploadFile(ctx, uploader, bucket, s3Key, tags, f.Path); err != nil {
			return fmt.Errorf("uploading %s: %w", f.Path, err)
		}
	}

	return nil
}

func uploadFile(ctx context.Context, uploader *manager.Uploader, bucket, key, tags, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              file,
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
		Tagging:           aws.String(tags),
	})
	if err != nil {
		return fmt.Errorf("S3 upload: %w", err)
	}

	return nil
}
