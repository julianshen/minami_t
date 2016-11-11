package minami_t

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
)

type Downloader struct {
	CacheDir string
	locks    map[string]*sync.Mutex
}

func urlToFileName(urlStr string) (string, error) {
	_url, err := url.Parse(urlStr)

	if err != nil {
		return "", err
	}

	return path.Base(_url.Path), nil
}

func (d *Downloader) Download(urlStr string) (string, error) {
	fileName, err := urlToFileName(urlStr)
	fileName = d.CacheDir + fileName

	lock := d.locks[fileName]
	if lock == nil {
		lock = &sync.Mutex{}
		d.locks[fileName] = lock
	}

	lock.Lock()
	defer func() {
		d.locks[fileName] = nil
		lock.Unlock()
	}()

	if _, err := os.Stat(fileName); err == nil {
		log.Println("File " + fileName + " exists in cache")
		return fileName, nil
	}

	log.Println("Downloading " + urlStr)
	resp, err := http.Get(urlStr)

	if err != nil {
		return "", err
	}

	file, err := os.Create(fileName)

	if err != nil {
		return "", err
	}

	io.Copy(file, resp.Body)
	return fileName, nil
}

func NewDownloader(cacheDir string) Downloader {
	os.Mkdir(cacheDir, 0755)
	return Downloader{cacheDir, make(map[string]*sync.Mutex)}
}
