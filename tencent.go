package go_cover_storage

import (
	"bytes"
	"context"
	"errors"
	"github.com/tencentyun/cos-go-sdk-v5"
	"net/http"
	"net/url"
	"sort"
)

// 腾讯云存储 cos
type tencent struct {
	appId, secretId, secretKey string
}

func (t *tencent) getCosNewClient(bucketName, region string) (*cos.Client, error) {
	u, _ := url.Parse("https://" + bucketName + "-" + t.appId + ".cos." + region + ".myqcloud.com")
	b := &cos.BaseURL{BucketURL: u}
	// 1.永久密钥
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  t.secretId,
			SecretKey: t.secretKey,
		},
	})
	if client == nil {
		return nil, errors.New("cannot initialize cos client")
	}
	return client, nil
}

func (t *tencent) Init(options map[string]interface{}) (StoreClient, error) {
	appId, err := checkCommonStringKey("appId", options, ErrEmptyAppId, ErrStringAppId)
	if err != nil {
		return nil, err
	}
	accessKey, secretKey, err := getAccessKeySecretKey(options)
	if err != nil {
		return nil, err
	}
	t.appId = appId
	t.secretId = accessKey
	t.secretKey = secretKey
	return &tencent{
		appId:     appId,
		secretId:  accessKey,
		secretKey: secretKey,
	}, nil
}

func (t *tencent) MultipartUploadInit(bucketName, region, objectKey string) (string, error) {
	client, err := t.getCosNewClient(bucketName, region)
	if err != nil {
		return "", err
	}
	v, _, err := client.Object.InitiateMultipartUpload(context.Background(), objectKey, nil)
	if err != nil {
		return "", err
	}
	return v.UploadID, nil
}

func (t *tencent) MultipartUploadPart(bucketName, region, objectKey, uploadId string, partNumber uint, body []byte) (H, error) {
	client, err := t.getCosNewClient(bucketName, region)
	if err != nil {
		return nil, err
	}
	resp, err := client.Object.UploadPart(
		context.Background(), objectKey, uploadId, int(partNumber), bytes.NewReader(body), nil,
	)
	if err != nil {
		return nil, err
	}
	return H{
		"PartNumber": int(partNumber),
		"ETag":       resp.Header.Get("ETag"),
	}, nil
}

func (t *tencent) MultipartUploadComplete(bucketName, region, objectKey, uploadId string, parts map[uint]string) (H, error) {
	client, err := t.getCosNewClient(bucketName, region)
	if err != nil {
		return nil, err
	}
	optParts := make([]cos.Object, 0)
	for partNumber, eTag := range parts {
		optParts = append(optParts, cos.Object{
			ETag:       eTag,
			PartNumber: int(partNumber),
		})
	}
	sort.Slice(optParts, func(i, j int) bool {
		return optParts[i].PartNumber < optParts[j].PartNumber
	})

	opt := &cos.CompleteMultipartUploadOptions{
		Parts: optParts,
	}
	result, _, err := client.Object.CompleteMultipartUpload(
		context.Background(), objectKey, uploadId, opt,
	)
	if err != nil {
		return nil, err
	}
	return H{
		"Location": result.Location,
		"Bucket":   result.Bucket,
		"ETag":     result.ETag,
		"Key":      result.Key,
	}, nil
}
