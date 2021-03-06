package go_cover_storage

import (
	"bytes"
	"github.com/north-team/huawei-obs-sdk-go/obs"
	"sort"
)

// 华为云存储 obs
type huawei struct {
	accessKey, secretKey string
}

func (h *huawei) getObsNewClient(region string) (*obs.ObsClient, error) {
	endpoint := "https://obs." + region + ".myhuaweicloud.com"
	return obs.New(h.accessKey, h.secretKey, endpoint)
}

func (h *huawei) Init(options map[string]interface{}) (StoreClient, error) {
	accessKey, secretKey, err := getAccessKeySecretKey(options)
	if err != nil {
		return nil, err
	}
	h.accessKey = accessKey
	h.secretKey = secretKey
	return &huawei{
		accessKey: accessKey,
		secretKey: secretKey,
	}, nil
}

func (h *huawei) MultipartUploadInit(bucketName, region, objectKey string) (string, error) {
	obsClient, err := h.getObsNewClient(region)
	if err != nil {
		return "", err
	}
	input := &obs.InitiateMultipartUploadInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: bucketName,
			Key:    objectKey,
		},
		ContentType: "text/plain",
	}
	output, err := obsClient.InitiateMultipartUpload(input)
	if err != nil {
		return "", err
	}
	obsClient.Close()
	return output.UploadId, nil
}

func (h *huawei) MultipartUploadPart(bucketName, region, objectKey, uploadId string, partNumber uint, body []byte) (H, error) {
	obsClient, err := h.getObsNewClient(region)
	if err != nil {
		return nil, err
	}
	input := &obs.UploadPartInput{
		Bucket:     bucketName,
		Key:        objectKey,
		PartNumber: int(partNumber),
		UploadId:   uploadId,
		Body:       bytes.NewReader(body),
	}
	output, err := obsClient.UploadPart(input)
	if err != nil {
		return nil, err
	}

	return H{
		"PartNumber": output.PartNumber,
		"ETag":       output.ETag,
	}, nil
}

func (h *huawei) MultipartUploadComplete(bucketName, region, objectKey, uploadId string, parts map[uint]string) (H, error) {
	obsClient, err := h.getObsNewClient(region)
	if err != nil {
		return nil, err
	}
	inputParts := make([]obs.Part, 0)
	for partNumber, eTag := range parts {
		inputParts = append(inputParts, obs.Part{
			PartNumber: int(partNumber),
			ETag:       eTag,
		})
	}
	sort.Slice(inputParts, func(i, j int) bool {
		return inputParts[i].PartNumber < inputParts[j].PartNumber
	})
	input := &obs.CompleteMultipartUploadInput{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadId: uploadId,
		Parts:    inputParts,
	}

	result, err := obsClient.CompleteMultipartUpload(input)
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
