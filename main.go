package client

import (
	"fmt"
	"io/ioutil"
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

func (rc *RcfgClient) Get(db string, key string) (string, error) {
	k := mergeDbAndKey(db, key)
	v, ok := rc.localCache[k]
	if !ok || time.Since(v.LastUpdated) > rc.CacheFor {
		resp, err := http.Get(rc.Url + fmt.Sprintf("/%s?k=%s", db, key))
		if err != nil {
			return "", err
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		bodyString := string(body)
		rc.localCache[k] = &lastUpdated{Value: bodyString, LastUpdated: time.Now()}
		return bodyString, nil
	} else {
		return v.Value, nil
	}
}
