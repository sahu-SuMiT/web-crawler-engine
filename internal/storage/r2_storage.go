package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// R2Storage manages uploading WARC archives to Cloudflare R2 (S3-compatible Object Storage).
type R2Storage struct {
	client     *s3.Client
	bucketName string
	enabled    bool
}

// NewR2Storage initializes an S3 client pointing to Cloudflare R2 endpoint.
// If credentials or accountID are empty, it checks environment variables.
// If any parameter is missing, R2 storage operates in disabled/dry-run mode cleanly.
func NewR2Storage(ctx context.Context, accountID, accessKeyID, secretAccessKey, bucketName string) (*R2Storage, error) {
	if accountID == "" {
		accountID = os.Getenv("R2_ACCOUNT_ID")
	}
	if accessKeyID == "" {
		accessKeyID = os.Getenv("R2_ACCESS_KEY_ID")
	}
	if secretAccessKey == "" {
		secretAccessKey = os.Getenv("R2_SECRET_ACCESS_KEY")
	}
	if bucketName == "" {
		bucketName = os.Getenv("R2_BUCKET_NAME")
	}
	if bucketName == "" {
		bucketName = "sota-crawler-archive"
	}

	if accountID == "" || accessKeyID == "" || secretAccessKey == "" {
		return &R2Storage{enabled: false, bucketName: bucketName}, nil
	}

	r2Endpoint := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
		config.WithRegion("auto"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load R2 AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(r2Endpoint)
	})

	return &R2Storage{
		client:     client,
		bucketName: bucketName,
		enabled:    true,
	}, nil
}

// IsEnabled returns true if Cloudflare R2 credentials are configured and active.
func (r *R2Storage) IsEnabled() bool {
	return r.enabled
}

// UploadWARC uploads a local .warc.gz archive file to Cloudflare R2 bucket.
func (r *R2Storage) UploadWARC(ctx context.Context, localFilePath string) (string, error) {
	if !r.enabled {
		return "", nil // Local mode (R2 upload skipped)
	}

	file, err := os.Open(localFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open WARC file for upload: %w", err)
	}
	defer file.Close()

	key := "warc/" + filepath.Base(localFilePath)

	_, err = r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(r.bucketName),
		Key:         aws.String(key),
		Body:        file,
		ContentType: aws.String("application/gzip"),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload object to Cloudflare R2: %w", err)
	}

	remoteURI := fmt.Sprintf("r2://%s/%s", r.bucketName, key)
	return remoteURI, nil
}
