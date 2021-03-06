package go_cover_storage

import (
	"bytes"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"sort"
	"strconv"
)

// 阿里云存储 oss
type aliyun struct {
	accessKeyId, accessKeySecret string
}

func (a *aliyun) getOssClientBucket(bucketName, region string) (*oss.Bucket, error) {
	endpoint := "oss-" + region + ".aliyuncs.com"
	client, err := oss.New(endpoint, a.accessKeyId, a.accessKeySecret)
	if err != nil {
		return nil, err
	}
	return client.Bucket(bucketName)
}

func (a *aliyun) DoUploadPart(bucket oss.Bucket, request *oss.UploadPartRequest, options []oss.Option) (*oss.UploadPartResult, error) {
	listener := oss.GetProgressListener(options)
	options = append(options, oss.ContentLength(request.PartSize))
	params := map[string]interface{}{}
	params["partNumber"] = strconv.Itoa(request.PartNumber)
	params["uploadId"] = request.InitResult.UploadID
	resp, err := bucket.Do("PUT", request.InitResult.Key, params, options, request.Reader, listener)
	if err != nil {
		return &oss.UploadPartResult{}, err
	}
	defer resp.Body.Close()

	part := oss.UploadPart{
		ETag:       resp.Headers.Get(oss.HTTPHeaderEtag),
		PartNumber: request.PartNumber,
	}

	if bucket.GetConfig().IsEnableCRC {
		err = oss.CheckCRC(resp, "DoUploadPart")
		if err != nil {
			return &oss.UploadPartResult{Part: part}, err
		}
	}

	return &oss.UploadPartResult{Part: part}, nil
}

func (a *aliyun) Init(options map[string]interface{}) (StoreClient, error) {
	accessKey, secretKey, err := getAccessKeySecretKey(options)
	if err != nil {
		return nil, err
	}
	return &aliyun{
		accessKeyId:     accessKey,
		accessKeySecret: secretKey,
	}, nil
}

func (a *aliyun) MultipartUploadInit(bucketName, region, objectKey string) (string, error) {
	bucket, err := a.getOssClientBucket(bucketName, region)
	if err != nil {
		return "", err
	}
	storageType := oss.ObjectStorageClass(oss.StorageStandard)
	result, err := bucket.InitiateMultipartUpload(objectKey, storageType)
	if err != nil {
		return "", err
	}
	return result.UploadID, nil
}

func (a *aliyun) MultipartUploadPart(bucketName, region, objectKey, uploadId string, partNumber uint, body []byte) (H, error) {
	bucket, err := a.getOssClientBucket(bucketName, region)
	if err != nil {
		return nil, err
	}
	InitResult := oss.InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadID: uploadId,
	}
	fd := bytes.NewReader(body)
	request := &oss.UploadPartRequest{
		InitResult: &InitResult,
		Reader:     fd,
		PartSize:   fd.Size(),
		PartNumber: int(partNumber),
	}
	result, err := a.DoUploadPart(*bucket, request, nil)
	if err != nil {
		return nil, err
	}
	return H{
		"PartNumber": result.Part.PartNumber,
		"ETag":       result.Part.ETag,
	}, nil
}

func (a *aliyun) MultipartUploadComplete(bucketName, region, objectKey, uploadId string, parts map[uint]string) (H, error) {
	bucket, err := a.getOssClientBucket(bucketName, region)
	if err != nil {
		return nil, err
	}
	InitResult := oss.InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadID: uploadId,
	}
	var uploadParts []oss.UploadPart
	for partNumber, eTag := range parts {
		uploadParts = append(uploadParts, oss.UploadPart{
			PartNumber: int(partNumber),
			ETag:       eTag,
		})
	}
	sort.Slice(uploadParts, func(i, j int) bool {
		return uploadParts[i].PartNumber < uploadParts[j].PartNumber
	})
	result, err := bucket.CompleteMultipartUpload(InitResult, uploadParts)
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
