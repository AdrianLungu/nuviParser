// go install && $GOPATH/bin/nuviParser
package main

import (
	"net/http"
	"strings"
	"archive/zip"
	"os"
	"path/filepath"
	"io"
	"flag"
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"golang.org/x/net/html"
	"net/url"
	"strconv"
	"github.com/iris-contrib/errors"
	"sort"
)

var (
	filesUrl = flag.String("files-url", "http://feed.omgili.com/5Rh5AMTrc4Pv/mainstream/posts/", "The URL of the zip files")
	redisAddress = flag.String("redis", ":6379", "Redis server address")
	maxConnections = flag.Int("max-connections", 10, "Max number of Redis connections")

	waitGroup sync.WaitGroup
	redisPool *redis.Pool
)

func main() {

	log.Println(`Starting NuviParser...`)

	// Create the download path where we will download the zip files and unzip them
	log.Println(`Crearting temporary directory...`)
	tempDir, err := ioutil.TempDir("", "nuviTemp")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Println(`Removing temporary directory...`)
		err = os.RemoveAll(tempDir)
		if err != nil {
			panic(err)
		}
	}()

	// Create a Redis pool
	log.Println(`Creating a Redis pool...`)
	redisPool = redis.NewPool(func() (redis.Conn, error) {

		conn, err := redis.Dial("tcp", *redisAddress)
		if err != nil {
			return nil, err
		}

		return conn, nil

	}, *maxConnections)

	// Retrieve the file names without extension from the url
	log.Println(`Retrieving file names without extension from the url...`)
	timestamps, err := getTimestampsFromUrl(*filesUrl)
	if err != nil {
		log.Panic(err)
	}

	// The new int which will be saved in Redis to represent the last file processed
	var newFileTimestamp int

	// The timestamp of the last file that was parsed
	lastParsedFileTimestamp, err := getLastParsedFileTimestamp()
	if err != nil {
		log.Panic(err)
	}

	log.Println(`Last parsed file timestamp:`, lastParsedFileTimestamp)

	// Download, unzip and publish the content of each file
	for _, timestamp := range timestamps {

		if timestamp <= lastParsedFileTimestamp {
			continue
		}

		waitGroup.Add(1)

		if timestamp > newFileTimestamp {
			newFileTimestamp = timestamp
		}

		log.Println(`Processing zip file:`, timestamp)

		go processTimestamp(timestamp, tempDir)

	}

	waitGroup.Wait()

	// Only save the new timestamp if it was assigned a new value from a file, otherwise skip
	if newFileTimestamp > 1 {
		log.Println(`Saving new timestamp to Redis:`, newFileTimestamp)
		conn := redisPool.Get()
		conn.Send(`SET`, `parser:lastParsedTimestamp`, newFileTimestamp)
		conn.Close()
	} else {
		log.Println(`No new files found.`)
	}

}

// Get the last/latest parsed file's timestamp from Redis (stored in parser:lastParsedTimestamp)
func getLastParsedFileTimestamp() (lastParsedFile int, err error) {

	conn := redisPool.Get()
	defer conn.Close()

	lpt, err := conn.Do(`GET`, `parser:lastParsedTimestamp`)
	if err != nil {
		return lastParsedFile, err
	}

	if lpt == nil {
		return 0, nil
	}

	switch v := lpt.(type) {
	case string:
		lastParsedFile, err = strconv.Atoi(v)
		return lastParsedFile, err
	case []uint8:
		b := make([]byte, len(v))
		for i, v := range v {
			b[i] = byte(v)
		}

		lastParsedFile, err = strconv.Atoi(string(b))
		return lastParsedFile, err
	}

	return lastParsedFile, errors.New("Couldn't get last parsed file.")

}

// Download, unzip and and insert the xml files belonging to the specified timestamp's zip file, into Redis.
func processTimestamp(timestamp int, tempDir string) {

	defer waitGroup.Done()

	fileName := strconv.Itoa(timestamp) + ".zip"

	log.Println(`Retrieving connection from Redis pool...`)

	c := redisPool.Get()
	defer c.Close()

	fileUrl, err := url.Parse(*filesUrl)
	if err != nil {
		log.Panic(err)
	}

	fileUrl.Path = path.Join(fileUrl.Path, fileName)
	filePath := path.Join(tempDir, fileName)
	unzipDir := path.Join(tempDir, strings.Split(fileName, ".")[0])

	log.Println(`Downloading file:`, fileUrl.String())

	err = download(fileUrl.String(), filePath)
	if err != nil {
		log.Panic(err)
	}

	log.Println(`Unzipping file:`, filePath)

	err = unzip(filePath, unzipDir)
	if err != nil {
		log.Panic(err)
	}

	log.Println(`Processing xml files from:`, unzipDir)

	err = filepath.Walk(unzipDir, func(path string, info os.FileInfo, err error) error {

		if info.IsDir() {
			return nil
		}

		xmlData, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		c.Send("RPUSH", "NEWS_XML", info.Name(), string(xmlData))

		return nil

	})
	if err != nil {
		log.Panic(err)
	}

	log.Println(`Finished processing all xml files of zip file:`, fileName)

}

// Get all the file names without extension (timestamps) from the url as integers sorted in increasing order
func getTimestampsFromUrl(url string) (timestamps []int, err error) {

	resp, err := http.Get(url)
	if err != nil {
		return timestamps, err
	}

	tokenizer := html.NewTokenizer(resp.Body)

	for {
		tokenType := tokenizer.Next()

		switch {
		case tokenType == html.ErrorToken:
			return timestamps, err
		case tokenType == html.StartTagToken:
			token := tokenizer.Token()

			isAnchor := token.Data == "a"
			if isAnchor {
				for _, v := range token.Attr {
					if strings.Compare(v.Key, `href`) != 0 {
						continue
					}

					extension := string(v.Val[len(v.Val) - 3:]) // Should be ZIP

					if strings.Compare(extension, `zip`) != 0 {
						continue
					}

					timestamp, err := strconv.Atoi(v.Val[:len(v.Val) - 4])
					if err != nil {
						return timestamps, err
					}

					timestamps = append(timestamps, timestamp)
				}
			}
		}
	}

	sort.Ints(timestamps)
	return timestamps, err

}

// Download a file from src to the specified dest
func download(src, dest string) error {

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(src)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil

}

// Unzip a file from src to the specified dest
func unzip(src, dest string) error {

	reader, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			panic(err)
		}
	}()

	err = os.MkdirAll(dest, 0755)
	if err != nil {
		return err
	}

	extractAndWriteFile := func(file *zip.File) error {
		rc, err := file.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, file.Name)

		if file.FileInfo().IsDir() {
			err = os.MkdirAll(path, file.Mode())
			if err != nil {
				return err
			}
		} else {
			f, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, file.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range reader.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil

}
