package go_cover_storage

import (
	"errors"
	"fmt"
	"strings"
)

type H map[string]interface{}

type ClientInterface interface {
	Init(options map[string]interface{}) (StoreClient, error)
}

// 云存储客户端
type StoreClient interface {
	// 初始化分片上传
	MultipartUploadInit(bucketName, region, objectKey string) (string, error)
	// 上传分片
	MultipartUploadPart(bucketName, region, objectKey, uploadId string, partNumber uint, body []byte) (H, error)
	// 完成分片上传
	MultipartUploadComplete(bucketName, region, objectKey, uploadId string, parts map[uint]string) (H, error)
}

var (
	Aliyun  ClientInterface
	Baidu   ClientInterface
	Huawei  ClientInterface
	Local   ClientInterface
	Qiniu   ClientInterface
	Tencent ClientInterface
)

var (
	ErrEmptyAccessKey  = errors.New("accessKey cannot be empty")
	ErrEmptySecretKey  = errors.New("secretKey cannot be empty")
	ErrEmptyAppId      = errors.New("appId cannot be empty")
	ErrStringAccessKey = errors.New("accessKey is not a string")
	ErrStringSecretKey = errors.New("secretKey is not a string")
	ErrStringAppId     = errors.New("appId is not a string")
)

func init() {
	Aliyun = &aliyun{}
	Baidu = &baidu{}
	Huawei = &huawei{}
	Local = &local{}
	Qiniu = &qiniu{}
	Tencent = &tencent{}
}

func CreateClient(clientName string, options map[string]interface{}) (StoreClient, error) {
	var client ClientInterface
	switch clientName {
	case "aliyun":
		client = Aliyun
	case "baidu":
		client = Baidu
	case "huawei":
		client = Huawei
	case "local":
		client = Local
	case "qiniu":
		client = Qiniu
	case "tencent":
		client = Tencent
	}
	if client == nil {
		return nil, fmt.Errorf("client %s not exist", clientName)
	}
	return client.Init(options)
}

func checkCommonStringKey(key string, options map[string]interface{}, errEmpty, errString error) (string, error) {
	data, ok := options[key]
	if !ok {
		return "", errEmpty
	}
	stringData, ok := data.(string)
	if !ok {
		return "", errString
	}
	if stringData = strings.TrimSpace(stringData); stringData == "" {
		return "", errEmpty
	}
	return stringData, nil
}

func getAccessKeySecretKey(options map[string]interface{}) (string, string, error) {
	accessKey, err := checkCommonStringKey("accessKey", options, ErrEmptyAccessKey, ErrStringAccessKey)
	if err != nil {
		return "", "", err
	}
	secretKey, err := checkCommonStringKey("secretKey", options, ErrEmptySecretKey, ErrStringSecretKey)
	if err != nil {
		return "", "", err
	}
	return accessKey, secretKey, nil
}

