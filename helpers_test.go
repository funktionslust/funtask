package funtask

import "net/http"

func closeBody(resp *http.Response) {
	_ = resp.Body.Close()
}
