package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/chawanghyeon/eventglass/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const checksumMetadataKey = "eventglass-sha256"

type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	PathStyle       bool
}

type S3Store struct {
	client          *s3.Client
	bucket          string
	prefix          string
	putRequests     atomic.Uint64
	putBytes        atomic.Uint64
	headRequests    atomic.Uint64
	fullGetRequests atomic.Uint64
	fullGetBytes    atomic.Uint64
	rangeRequests   atomic.Uint64
	rangeBytes      atomic.Uint64
}

type OperationCounts struct {
	PutRequests     uint64
	PutBytes        uint64
	HeadRequests    uint64
	FullGetRequests uint64
	FullGetBytes    uint64
	RangeRequests   uint64
	RangeBytes      uint64
}

func (s *S3Store) OperationCounts() OperationCounts {
	return OperationCounts{
		PutRequests: s.putRequests.Load(), PutBytes: s.putBytes.Load(), HeadRequests: s.headRequests.Load(),
		FullGetRequests: s.fullGetRequests.Load(), FullGetBytes: s.fullGetBytes.Load(),
		RangeRequests: s.rangeRequests.Load(), RangeBytes: s.rangeBytes.Load(),
	}
}

type ObjectInfo struct {
	Key    string
	Size   int64
	SHA256 string
	ETag   string
}

type MultipartUpload struct {
	Key      string
	UploadID string
}

type UploadedPart struct {
	Number int32
	ETag   string
}

func NewS3Store(ctx context.Context, settings S3Config) (*S3Store, error) {
	if settings.Region == "" || settings.Bucket == "" {
		return nil, errors.New("S3 region and bucket are required")
	}
	loadOptions := []func(*config.LoadOptions) error{config.WithRegion(settings.Region)}
	if settings.AccessKeyID != "" || settings.SecretAccessKey != "" {
		if settings.AccessKeyID == "" || settings.SecretAccessKey == "" {
			return nil, errors.New("both S3 access key and secret are required")
		}
		loadOptions = append(loadOptions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(settings.AccessKeyID, settings.SecretAccessKey, settings.SessionToken)))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.UsePathStyle = settings.PathStyle
		if settings.Endpoint != "" {
			options.BaseEndpoint = aws.String(strings.TrimRight(settings.Endpoint, "/"))
		}
	})
	return &S3Store{client: client, bucket: settings.Bucket, prefix: strings.Trim(settings.Prefix, "/")}, nil
}

func (s *S3Store) objectKey(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "..") || strings.ContainsRune(key, '\\') {
		return "", errors.New("object key must be a non-empty server-generated relative path")
	}
	if s.prefix == "" {
		return key, nil
	}
	return s.prefix + "/" + key, nil
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) (ObjectInfo, error) {
	digest := sha256.Sum256(data)
	return s.PutStream(ctx, key, bytes.NewReader(data), int64(len(data)), hex.EncodeToString(digest[:]))
}

// PutStream uploads a private immutable spool without retaining a full object
// in Go memory. The workflow must register an intent before calling this.
func (s *S3Store) PutStream(ctx context.Context, key string, body io.ReadSeeker, size int64, checksum string) (ObjectInfo, error) {
	if size < 0 || size == int64(^uint64(0)>>1) {
		return ObjectInfo{}, errors.New("invalid object size")
	}
	if _, err := decodeHash(checksum); err != nil {
		return ObjectInfo{}, err
	}
	objectKey, err := s.objectKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return ObjectInfo{}, err
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(body, size+1))
	if err != nil {
		return ObjectInfo{}, err
	}
	if n != size || hex.EncodeToString(hash.Sum(nil)) != checksum {
		return ObjectInfo{}, errors.New("spool size or checksum mismatch")
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return ObjectInfo{}, err
	}
	s.putRequests.Add(1)
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(objectKey),
		Body:          body,
		ContentLength: aws.Int64(size),
		Metadata:      map[string]string{checksumMetadataKey: checksum},
	})
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("put S3 object: %w", err)
	}
	s.putBytes.Add(uint64(size))
	info, err := s.Head(ctx, key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if info.Size != size || info.SHA256 != checksum {
		return ObjectInfo{}, errors.New("uploaded S3 object metadata does not match content")
	}
	if err := s.VerifyObject(ctx, key, size, checksum); err != nil {
		return ObjectInfo{}, err
	}
	return info, nil
}

// VerifyObject establishes stored bytes with a bounded full readback. Head
// metadata is uploader-controlled and is not accepted as content evidence.
func (s *S3Store) VerifyObject(ctx context.Context, key string, size int64, checksum string) error {
	if size < 0 {
		return errors.New("invalid verification size")
	}
	if _, err := decodeHash(checksum); err != nil {
		return err
	}
	objectKey, err := s.objectKey(key)
	if err != nil {
		return err
	}
	s.fullGetRequests.Add(1)
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey)})
	if err != nil {
		return fmt.Errorf("get S3 object for verification: %w", err)
	}
	defer output.Body.Close()
	if output.ContentLength != nil && aws.ToInt64(output.ContentLength) != size {
		return errors.New("stored S3 object size mismatch")
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(output.Body, size+1))
	if n > 0 {
		s.fullGetBytes.Add(uint64(n))
	}
	if err != nil {
		return fmt.Errorf("read S3 object for verification: %w", err)
	}
	if n != size || hex.EncodeToString(hash.Sum(nil)) != checksum {
		return errors.New("stored S3 object checksum mismatch")
	}
	return nil
}

// DownloadToFile materializes one supervisor-owned private input and verifies
// the actual object bytes. Callers must provide a new path in a private task
// directory and an operation-specific limit (journal24MiB, query64MiB,
// bundle128MiB). The shared format ceiling cannot be raised by a caller.
// Partial data is removed on every failure.
func (s *S3Store) DownloadToFile(ctx context.Context, key, path string, size int64, checksum string, maxBytes int64) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(path) || maxBytes <= 0 || maxBytes > model.MaxBundleFileBytes || size <= 0 || size > maxBytes {
		return errors.New("invalid verified download target")
	}
	if _, err := decodeHash(checksum); err != nil {
		return err
	}
	objectKey, err := s.objectKey(key)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
		if retErr != nil {
			_ = os.Remove(path)
		}
	}()
	s.fullGetRequests.Add(1)
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey)})
	if err != nil {
		return fmt.Errorf("get verified object: %w", err)
	}
	defer output.Body.Close()
	if output.ContentLength != nil && aws.ToInt64(output.ContentLength) != size {
		return errors.New("downloaded object size mismatch")
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(output.Body, size+1))
	if written > 0 {
		s.fullGetBytes.Add(uint64(written))
	}
	if err != nil || written != size || hex.EncodeToString(hash.Sum(nil)) != checksum {
		return errors.Join(errors.New("downloaded object checksum mismatch"), err)
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

func (s *S3Store) Head(ctx context.Context, key string) (ObjectInfo, error) {
	objectKey, err := s.objectKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	s.headRequests.Add(1)
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey)})
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("head S3 object: %w", err)
	}
	return ObjectInfo{Key: key, Size: aws.ToInt64(output.ContentLength), SHA256: output.Metadata[checksumMetadataKey], ETag: aws.ToString(output.ETag)}, nil
}

// EnsureImmutableObject creates a durable authority object only when absent.
// An existing object must already be byte-identical; conflicting bytes are
// never overwritten. PostgreSQL owns coordination, while this check protects
// setup/recovery identities from accidental S3 replacement.
func (s *S3Store) EnsureImmutableObject(ctx context.Context, key string, data []byte, checksum string) error {
	digest := sha256.Sum256(data)
	if len(data) == 0 || hex.EncodeToString(digest[:]) != checksum {
		return errors.New("invalid immutable object identity")
	}
	info, err := s.Head(ctx, key)
	if err == nil {
		if info.Size != int64(len(data)) || info.SHA256 != checksum {
			return errors.New("immutable object conflicts with stored identity")
		}
		return s.VerifyObject(ctx, key, int64(len(data)), checksum)
	}
	var responseError *smithyhttp.ResponseError
	if !errors.As(err, &responseError) || responseError.HTTPStatusCode() != 404 {
		return err
	}
	uploaded, err := s.Put(ctx, key, data)
	if err != nil {
		return err
	}
	if uploaded.Size != int64(len(data)) || uploaded.SHA256 != checksum {
		return errors.New("immutable object upload metadata mismatch")
	}
	return nil
}

func (s *S3Store) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 {
		return nil, errors.New("range offset must be non-negative and length positive")
	}
	objectKey, err := s.objectKey(key)
	if err != nil {
		return nil, err
	}
	rangeHeader := "bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+length-1, 10)
	s.rangeRequests.Add(1)
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey), Range: aws.String(rangeHeader)})
	if err != nil {
		return nil, fmt.Errorf("range GET S3 object: %w", err)
	}
	defer output.Body.Close()
	data, err := io.ReadAll(io.LimitReader(output.Body, length+1))
	if err != nil {
		return nil, fmt.Errorf("read S3 range: %w", err)
	}
	if int64(len(data)) != length {
		return nil, io.ErrUnexpectedEOF
	}
	s.rangeBytes.Add(uint64(len(data)))
	return data, nil
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	objectPrefix, err := s.objectKey(prefix)
	if err != nil {
		return nil, err
	}
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(objectPrefix)})
	var objects []ObjectInfo
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list S3 objects: %w", err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if s.prefix != "" {
				key = strings.TrimPrefix(key, s.prefix+"/")
			}
			objects = append(objects, ObjectInfo{Key: key, Size: aws.ToInt64(object.Size), ETag: aws.ToString(object.ETag)})
		}
	}
	return objects, nil
}

func (s *S3Store) Delete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	objects := make([]types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		objectKey, err := s.objectKey(key)
		if err != nil {
			return err
		}
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(objectKey)})
	}
	output, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(s.bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(false)}})
	if err != nil {
		return fmt.Errorf("delete S3 objects: %w", err)
	}
	if len(output.Errors) > 0 {
		return fmt.Errorf("delete S3 object %q: %s", aws.ToString(output.Errors[0].Key), aws.ToString(output.Errors[0].Code))
	}
	return nil
}

func (s *S3Store) BeginMultipart(ctx context.Context, key string, sha256Hex string) (MultipartUpload, error) {
	if _, err := decodeHash(sha256Hex); err != nil {
		return MultipartUpload{}, err
	}
	objectKey, err := s.objectKey(key)
	if err != nil {
		return MultipartUpload{}, err
	}
	output, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey), Metadata: map[string]string{checksumMetadataKey: sha256Hex}})
	if err != nil {
		return MultipartUpload{}, fmt.Errorf("create multipart upload: %w", err)
	}
	return MultipartUpload{Key: key, UploadID: aws.ToString(output.UploadId)}, nil
}

func (s *S3Store) UploadPart(ctx context.Context, upload MultipartUpload, number int32, data []byte) (UploadedPart, error) {
	if number <= 0 || upload.UploadID == "" {
		return UploadedPart{}, errors.New("invalid multipart upload or part number")
	}
	objectKey, err := s.objectKey(upload.Key)
	if err != nil {
		return UploadedPart{}, err
	}
	output, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey), UploadId: aws.String(upload.UploadID), PartNumber: aws.Int32(number), Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data)))})
	if err != nil {
		return UploadedPart{}, fmt.Errorf("upload multipart part: %w", err)
	}
	return UploadedPart{Number: number, ETag: aws.ToString(output.ETag)}, nil
}

func (s *S3Store) CompleteMultipart(ctx context.Context, upload MultipartUpload, parts []UploadedPart) (ObjectInfo, error) {
	objectKey, err := s.objectKey(upload.Key)
	if err != nil {
		return ObjectInfo{}, err
	}
	completed := make([]types.CompletedPart, 0, len(parts))
	for _, part := range parts {
		completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(part.Number), ETag: aws.String(part.ETag)})
	}
	if _, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey), UploadId: aws.String(upload.UploadID), MultipartUpload: &types.CompletedMultipartUpload{Parts: completed}}); err != nil {
		return ObjectInfo{}, fmt.Errorf("complete multipart upload: %w", err)
	}
	return s.Head(ctx, upload.Key)
}

func (s *S3Store) AbortMultipart(ctx context.Context, upload MultipartUpload) error {
	objectKey, err := s.objectKey(upload.Key)
	if err != nil {
		return err
	}
	_, err = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey), UploadId: aws.String(upload.UploadID)})
	if err != nil {
		return fmt.Errorf("abort multipart upload: %w", err)
	}
	return nil
}
