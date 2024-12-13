package go_client

import (
	"fmt"
	"net"
	"net/url"
)

func ParseRpcUrl(url string) (host, port string, err error) {
	host, port, err = net.SplitHostPort(url)
	if err != nil {
		err = fmt.Errorf("invalid RPC URL format: %s, err: %w", url, err)
		return
	}

	if host == "" {
		err = fmt.Errorf("host is missing in RPC URL: %s", url)
		return
	}

	if port == "" {
		err = fmt.Errorf("port is missing in RPC URL: %s", url)
		return
	}

	return
}

func ParseHttpUrl(httpUrl string) (scheme, host, port string, err error) {
	res, err := url.Parse(httpUrl)
	if err != nil {
		err = fmt.Errorf("can't parse the HTTP URL: %s, err: %w", httpUrl, err)
		return
	}

	scheme = res.Scheme
	if scheme == "" {
		err = fmt.Errorf("scheme (http/https) is missing in HTTP URL: %s", httpUrl)
		return
	}

	host, port, err = net.SplitHostPort(res.Host)
	if err != nil {
		err = fmt.Errorf("invalid HTTP URL format: %s, err: %w", httpUrl, err)
		return
	}

	if host == "" {
		err = fmt.Errorf("host is missing in HTTP URL: %s", httpUrl)
		return
	}

	if port == "" {
		err = fmt.Errorf("port is missing in HTTP URL: %s", httpUrl)
		return
	}

	return
}
