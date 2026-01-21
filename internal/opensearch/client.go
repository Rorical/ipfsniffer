package opensearch

import (
	"crypto/tls"
	"fmt"
	"net/http"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
)

type Config struct {
	URL      string
	Username string
	Password string
	Insecure bool
}

func New(cfg Config) (*opensearch.Client, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("opensearch url required")
	}

	tr := &http.Transport{}
	if cfg.Insecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	c, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{cfg.URL},
		Username:  cfg.Username,
		Password:  cfg.Password,
		Transport: tr,
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}
