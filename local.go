package go_cover_storage

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

// 本地存储 local
type local struct {
	tempDir, storageDir string
}

type uploadPart struct {
	PartNumber int
	ETag       string
}

var (
	ErrEmptyTempDir     = errors.New("tempDir canot be empty")
	ErrEmptyStorageDir  = errors.New("storageDir cannot be empty")
	ErrStringTempDir    = errors.New("tempDir is not a string")
	ErrStringStorageDir = errors.New("storageDir is not a string")
)

func safetyPath(filepath string) string {
	sources := []string{".....", "....", "...", "..", ":", "*", "\"", "<", ">", "|"}
	for _, source := range sources {
		filepath = strings.ReplaceAll(filepath, source, "")
	}
	for _, s := range []string{"\\", "/"} {
		filepath = strings.ReplaceAll(filepath, s, string(os.PathSeparator))
	}
	return filepath
}

func (l *local) generateUploadId(bucketName, objectKey string) string {
	md5str2 := fmt.Sprintf("%x", md5.Sum([]byte(bucketName+objectKey)))
	return strings.ToUpper(md5str2)
}

func (l *local) Init(options map[string]interface{}) (StoreClient, error) {
	tempDir, err := checkCommonStringKey("tempDir", options, ErrEmptyTempDir, ErrStringTempDir)
	if err != nil {
		return nil, err
	}
	storageDir, err := checkCommonStringKey("storageDir", options, ErrEmptyStorageDir, ErrStringStorageDir)
	if err != nil {
		return nil, err
	}
	return &local{
		tempDir:    tempDir,
		storageDir: storageDir,
	}, nil
}

func (l *local) MultipartUploadInit(bucketName, region, objectKey string) (string, error) {
	l.tempDir = safetyPath(l.tempDir)
	l.storageDir = safetyPath(l.storageDir)
	return l.generateUploadId(bucketName, objectKey), nil
}

func (l *local) MultipartUploadPart(bucketName, region, objectKey, uploadId string, partNumber uint, body []byte) (H, error) {
	l.tempDir = safetyPath(l.tempDir)
	l.storageDir = safetyPath(l.storageDir)
	localUploadId := l.generateUploadId(bucketName, objectKey)
	if localUploadId != uploadId {
		return nil, errors.New("uploadId not exists")
	}
	partDir := safetyPath(path.Join(l.tempDir, uploadId))
	err := os.MkdirAll(partDir, os.ModePerm)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	partName := fmt.Sprintf("%x", md5.Sum([]byte(uploadId+strconv.Itoa(int(partNumber)))))
	partName = strings.ToUpper(partName)
	partPath := path.Join(partDir, partName+".part")
	file, err := os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	defer file.Close()
	if err != nil {
		return nil, err
	}
	if _, err = file.Write(body); err != nil {
		return nil, err
	}

	return H{
		"PartNumber": int(partNumber),
		"ETag":       partName,
	}, nil
}

func (l *local) MultipartUploadComplete(bucketName, region, objectKey, uploadId string, parts map[uint]string) (H, error) {
	l.tempDir = safetyPath(l.tempDir)
	l.storageDir = safetyPath(l.storageDir)
	localUploadId := l.generateUploadId(bucketName, objectKey)
	if localUploadId != uploadId {
		return nil, errors.New("uploadId not exists")
	}
	storageFile := path.Join(l.storageDir, bucketName, objectKey)
	storageFile = safetyPath(storageFile)
	storagePath := path.Dir(storageFile)
	err := os.MkdirAll(storagePath, os.ModePerm)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	targetFile, err := os.OpenFile(storageFile, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer targetFile.Close()
	if err != nil {
		return nil, err
	}

	newParts := make([]uploadPart, 0)
	for partNumber, eTag := range parts {
		newParts = append(newParts, uploadPart{
			PartNumber: int(partNumber),
			ETag:       eTag,
		})
	}
	sort.Slice(newParts, func(i, j int) bool {
		return newParts[i].PartNumber < newParts[j].PartNumber
	})

	partDir := safetyPath(path.Join(l.tempDir, uploadId))
	for _, part := range newParts {
		partName := fmt.Sprintf("%x", md5.Sum([]byte(uploadId+strconv.Itoa(part.PartNumber))))
		partName = strings.ToUpper(partName)
		if partName != part.ETag {
			continue
		}
		partPath := path.Join(partDir, partName+".part")
		part, err := os.Open(partPath)
		if err != nil {
			return nil, err
		}
		_, _ = io.Copy(targetFile, part)
		_ = part.Close()
		_ = os.Remove(partPath)
	}
	_ = os.Remove(partDir)

	return H{
		"path": storageFile,
	}, nil
}
