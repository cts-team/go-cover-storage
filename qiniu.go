package go_cover_storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"github.com/qiniu/go-sdk/v7/auth/qbox"
	"github.com/qiniu/go-sdk/v7/conf"
	"github.com/qiniu/go-sdk/v7/storage"
	"net/http"
	"sort"
	"strings"
)

// 七牛云存储 kodo
type qiniu struct {
	accessKey, secretKey string
}

type uploadPartInfo struct {
	Etag       string `json:"etag"`
	PartNumber int64  `json:"partNumber"`
	partSize   int
	fileOffset int64
}

type rputV2Extra struct {
	Recorder   storage.Recorder  // 可选。上传进度记录
	Metadata   map[string]string // 可选。用户自定义文件 metadata 信息
	CustomVars map[string]string // 可选。用户自定义参数，以"x:"开头，而且值不能为空，否则忽略
	UpHost     string
	MimeType   string                                              // 可选。
	PartSize   int64                                               // 可选。每次上传的块大小
	TryTimes   int                                                 // 可选。尝试次数
	Progress   []uploadPartInfo                                    // 上传进度
	Notify     func(partNumber int64, ret *storage.UploadPartsRet) // 可选。进度提示（注意多个block是并行传输的）
	NotifyErr  func(partNumber int64, err error)
}

func encodeV2(key string, hasKey bool) string {
	if !hasKey {
		return "~"
	} else {
		return base64.URLEncoding.EncodeToString([]byte(key))
	}
}

func makeHeadersForUploadEx(upToken, contentType string) http.Header {
	headers := http.Header{}
	if contentType != "" {
		headers.Add("Content-Type", contentType)
	}
	headers.Add("Authorization", "UpToken "+upToken)
	return headers
}

func completeParts(resumeUploaderV2 *storage.ResumeUploaderV2, ctx context.Context, upToken, upHost string, ret interface{}, bucket, key string, hasKey bool, uploadId string, extra *rputV2Extra) error {
	type CompletePartBody struct {
		Parts      []uploadPartInfo  `json:"parts"`
		MimeType   string            `json:"mimeType,omitempty"`
		Metadata   map[string]string `json:"metadata,omitempty"`
		CustomVars map[string]string `json:"customVars,omitempty"`
	}
	if extra == nil {
		extra = &rputV2Extra{}
	}
	completePartBody := CompletePartBody{
		Parts:      extra.Progress,
		MimeType:   extra.MimeType,
		Metadata:   extra.Metadata,
		CustomVars: make(map[string]string),
	}
	for k, v := range extra.CustomVars {
		if strings.HasPrefix(k, "x:") && v != "" {
			completePartBody.CustomVars[k] = v
		}
	}

	reqUrl := upHost + "/buckets/" + bucket + "/objects/" + encodeV2(key, hasKey) + "/uploads/" + uploadId

	return resumeUploaderV2.Client.CallWithJson(ctx, ret, "POST", reqUrl, makeHeadersForUploadEx(upToken, conf.CONTENT_TYPE_JSON), &completePartBody)
}

func (q *qiniu) getKodoResumeUploaderV2(bucketName string) (string, string, *storage.ResumeUploaderV2, error) {
	putPolicy := storage.PutPolicy{
		Scope: bucketName,
	}
	mac := qbox.NewMac(q.accessKey, q.secretKey)
	upToken := putPolicy.UploadToken(mac)
	cfg := storage.Config{}
	// 空间对应的机房
	region, err := storage.GetRegion(q.accessKey, bucketName)
	if err != nil {
		return "", "", nil, err
	}
	cfg.Region = region
	// 是否使用https域名
	cfg.UseHTTPS = true
	// 上传是否使用CDN上传加速
	cfg.UseCdnDomains = false
	resumeUploader := storage.NewResumeUploaderV2(&cfg)
	upHost, err := resumeUploader.UpHost(q.accessKey, bucketName)
	if err != nil {
		return "", "", nil, err
	}
	return upToken, upHost, resumeUploader, nil
}

func (q *qiniu) Init(options map[string]interface{}) (StoreClient, error) {
	accessKey, secretKey, err := getAccessKeySecretKey(options)
	if err != nil {
		return nil, err
	}
	return &qiniu{
		accessKey: accessKey,
		secretKey: secretKey,
	}, nil
}

func (q *qiniu) MultipartUploadInit(bucketName, region, objectKey string) (string, error) {
	upToken, upHost, resumeUploaderV2, err := q.getKodoResumeUploaderV2(bucketName)
	if err != nil {
		return "", err
	}
	result := &storage.InitPartsRet{}
	err = resumeUploaderV2.InitParts(context.Background(), upToken, upHost, bucketName, objectKey, true, result)
	if err != nil {
		return "", err
	}
	return result.UploadID, nil
}

func (q *qiniu) MultipartUploadPart(bucketName, region, objectKey, uploadId string, partNumber uint, body []byte) (H, error) {
	upToken, upHost, resumeUploaderV2, err := q.getKodoResumeUploaderV2(bucketName)
	if err != nil {
		return nil, err
	}
	result := &storage.UploadPartsRet{}
	fd := bytes.NewReader(body)
	err = resumeUploaderV2.UploadParts(context.Background(), upToken, upHost, bucketName, objectKey, true, uploadId, int64(partNumber), "", result, fd, fd.Len())
	if err != nil {
		return nil, err
	}
	return H{
		"PartNumber": int(partNumber),
		"ETag":       result.Etag,
	}, nil
}

func (q *qiniu) MultipartUploadComplete(bucketName, region, objectKey, uploadId string, parts map[uint]string) (H, error) {
	upToken, upHost, resumeUploaderV2, err := q.getKodoResumeUploaderV2(bucketName)
	if err != nil {
		return nil, err
	}
	inputParts := make([]uploadPartInfo, 0)
	for partNumber, eTag := range parts {
		inputParts = append(inputParts, uploadPartInfo{
			Etag:       eTag,
			PartNumber: int64(partNumber),
		})
	}
	sort.Slice(inputParts, func(i, j int) bool {
		return inputParts[i].PartNumber < inputParts[j].PartNumber
	})
	result := storage.PutRet{}
	putExtra := rputV2Extra{
		Progress: inputParts,
	}
	err = completeParts(resumeUploaderV2, context.Background(), upToken, upHost, &result, bucketName, objectKey, true, uploadId, &putExtra)
	if err != nil {
		return nil, err
	}
	return H{
		"Key": result.Key,
	}, nil
}
