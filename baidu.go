package go_cover_storage

import (
	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"sort"
)

// 百度云存储 bce
type baidu struct {
	accessKey, secretKey string
}

func (b *baidu) getBosNewClient(region string) (*bos.Client, error) {
	endpoint := "http://" + region + ".bcebos.com"
	return bos.NewClient(b.accessKey, b.secretKey, endpoint)
}

func (b *baidu) Init(options map[string]interface{}) (StoreClient, error) {
	accessKey, secretKey, err := getAccessKeySecretKey(options)
	if err != nil {
		return nil, err
	}
	return &baidu{
		accessKey: accessKey,
		secretKey: secretKey,
	}, nil
}

func (b *baidu) MultipartUploadInit(bucketName, region, objectKey string) (string, error) {
	bosClient, err := b.getBosNewClient(region)
	if err != nil {
		return "", err
	}
	result, err := bosClient.BasicInitiateMultipartUpload(bucketName, objectKey)
	if err != nil {
		return "", err
	}
	return result.UploadId, nil
}

func (b *baidu) MultipartUploadPart(bucketName, region, objectKey, uploadId string, partNumber uint, body []byte) (H, error) {
	bosClient, err := b.getBosNewClient(region)
	if err != nil {
		return nil, err
	}
	partBody, err := bce.NewBodyFromBytes(body)
	if err != nil {
		return nil, err
	}
	etag, err := bosClient.BasicUploadPart(bucketName, objectKey, uploadId, int(partNumber), partBody)
	if err != nil {
		return nil, err
	}
	return H{
		"PartNumber": int(partNumber),
		"ETag":       etag,
	}, nil
}

func (b *baidu) MultipartUploadComplete(bucketName, region, objectKey, uploadId string, parts map[uint]string) (H, error) {
	bosClient, err := b.getBosNewClient(region)
	if err != nil {
		return nil, err
	}
	partEtags := make([]api.UploadInfoType, 0)
	for partNumber, eTag := range parts {
		partEtags = append(partEtags, api.UploadInfoType{
			PartNumber: int(partNumber),
			ETag:       eTag,
		})
	}
	sort.Slice(partEtags, func(i, j int) bool {
		return partEtags[i].PartNumber < partEtags[j].PartNumber
	})

	completeArgs := api.CompleteMultipartUploadArgs{Parts: partEtags}
	result, err := bosClient.CompleteMultipartUploadFromStruct(
		bucketName, objectKey, uploadId, &completeArgs)
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
