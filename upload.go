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

// FileToUpload pairs a local file path with its manifest upload ID and size.
type FileToUpload struct {
	Path     string // absolute path on disk
	UploadID string // UUID used as the S3 key suffix (under the manifest key prefix)
	Size     int64  // file size in bytes (reported to finalize)
}

// UploadFiles uploads all files directly to the destination storage bucket
// under keyPrefix (O{org}/D{ds}/{manifest}) using the temporary STS credentials
// from /manifest/storage-credentials. Files are uploaded sequentially with
// multipart support. Returns the per-file finalize entries (uploadId, size,
// base64 SHA256) the caller passes to /manifest/files/finalize.
func UploadFiles(ctx context.Context, creds aws.Credentials, bucket, keyPrefix string, files []FileToUpload, orgID, datasetID, region string) ([]FinalizeFile, error) {
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
		return nil, fmt.Errorf("creating S3 config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = partSize
	})

	tags := fmt.Sprintf("OrgId=%s&DatasetId=%s", orgID, datasetID)

	finalize := make([]FinalizeFile, 0, len(files))
	for i, f := range files {
		s3Key := fmt.Sprintf("%s/%s", keyPrefix, f.UploadID)
		log.Printf("Uploading file %d/%d: %s → s3://%s/%s", i+1, len(files), f.Path, bucket, s3Key)

		out, err := uploadFile(ctx, uploader, bucket, s3Key, tags, f.Path)
		if err != nil {
			return nil, fmt.Errorf("uploading %s: %w", f.Path, err)
		}
		finalize = append(finalize, FinalizeFile{
			UploadID: f.UploadID,
			Size:     f.Size,
			SHA256:   aws.ToString(out.ChecksumSHA256),
		})
	}

	return finalize, nil
}

func uploadFile(ctx context.Context, uploader *manager.Uploader, bucket, key, tags, filePath string) (*manager.UploadOutput, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	out, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              file,
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
		Tagging:           aws.String(tags),
	})
	if err != nil {
		return nil, fmt.Errorf("S3 upload: %w", err)
	}

	return out, nil
}
