package client

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type lastUpdated struct {
	Value       string
	LastUpdated time.Time
}

type RcfgClient struct {
	Url        string
	CacheFor   time.Duration
	localCache map[string]*lastUpdated
}

func NewRcfgClient(url string, cacheFor time.Duration) *RcfgClient {
	return &RcfgClient{
		Url:        url,
		CacheFor:   cacheFor,
		localCache: make(map[string]*lastUpdated),
	}
}

func mergeDbAndKey(db string, key string) string {
	return db + "|" + key
}

func (rc *RcfgClient) Add(db string) (string, error) {
	resp, err := http.Get(rc.Url + fmt.Sprintf("/%s/add", db))
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyString := string(body)
	return bodyString, nil
}

func (rc *RcfgClient) Get(db string, key string) (string, error) {
	k := mergeDbAndKey(db, key)
	v, ok := rc.localCache[k]
	if !ok || time.Since(v.LastUpdated) > rc.CacheFor {
		log.Println("Cache miss")
		resp, err := http.Get(rc.Url + fmt.Sprintf("/%s/get?k=%s", db, key))
		if err != nil {
			return "", err
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		bodyString := string(body)
		log.Println("Get result:", bodyString)
		if resp.StatusCode != 200 {
			return "", fmt.Errorf("%s: %s", resp.Status, bodyString)
		}
		rc.localCache[k] = &lastUpdated{Value: bodyString, LastUpdated: time.Now()}
		return bodyString, nil
	} else {
		return v.Value, nil
	}
}

func (rc *RcfgClient) Set(db string, key string, value string) (string, error) {
	resp, err := http.Get(rc.Url + fmt.Sprintf("/%s/set?k=%s&v=%s", db, key, value))
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyString := string(body)
	return bodyString, nil
}

func (rc *RcfgClient) SetWithTTL(db string, key string, value string, ttl string) (string, error) {
	resp, err := http.Get(rc.Url + fmt.Sprintf("/%s/setttl?k=%s&v=%s&ttl=%s", db, key, value, ttl))
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyString := string(body)
	return bodyString, nil
}
