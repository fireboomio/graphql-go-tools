package httpclient

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/buger/jsonparser"

	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
)

const (
	ContentEncodingHeader = "Content-Encoding"
	AcceptEncodingHeader  = "Accept-Encoding"
)

var (
	DefaultNetHttpClient = &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1024,
			TLSHandshakeTimeout: 0 * time.Second,
		},
	}
	queryParamsKeys = [][]string{
		{"name"},
		{"value"},
	}
)

func Do(client *http.Client, ctx context.Context, requestInput []byte, out io.Writer) (err error) {

	url, method, body, headers, queryParams := requestInputParams(requestInput)
	body = SetUserValue(ctx, body)
	request, err := http.NewRequestWithContext(ctx, string(method), string(url), bytes.NewReader(body))
	if err != nil {
		return err
	}

	if headers != nil {
		err = jsonparser.ObjectEach(headers, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			_, err := jsonparser.ArrayEach(value, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
				if err != nil {
					return
				}
				if len(value) == 0 {
					return
				}
				request.Header.Add(string(key), string(value))
			})
			return err
		})
		if err != nil {
			return err
		}
	}

	if queryParams != nil {
		query := request.URL.Query()
		_, err = jsonparser.ArrayEach(queryParams, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			var (
				parameterName, parameterValue []byte
			)
			jsonparser.EachKey(value, func(i int, bytes []byte, valueType jsonparser.ValueType, err error) {
				switch i {
				case 0:
					parameterName = bytes
				case 1:
					parameterValue = bytes
				}
			}, queryParamsKeys...)
			if len(parameterName) != 0 && len(parameterValue) != 0 {
				if bytes.Equal(parameterValue[:1], literal.LBRACK) {
					_, _ = jsonparser.ArrayEach(parameterValue, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
						query.Add(string(parameterName), string(value))
					})
				} else {
					query.Add(string(parameterName), string(parameterValue))
				}
			}
		})
		if err != nil {
			return err
		}
		request.URL.RawQuery = query.Encode()
	}

	request.Header.Add("accept", "application/json")
	request.Header.Add("content-type", "application/json")
	if traceFunc, ok := TraceRequestFuncFromContext(ctx); ok {
		var callback func(...func(opentracing.Span))
		request, callback = traceFunc(request)
		defer callback(func(span opentracing.Span) {
			if err != nil {
				ext.LogError(span, err)
			}
		})
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	respReader, err := respBodyReader(request, response)
	if err != nil {
		return err
	}

	_, err = io.Copy(out, respReader)
	return
}

func respBodyReader(req *http.Request, resp *http.Response) (io.ReadCloser, error) {
	if req.Header.Get(AcceptEncodingHeader) == "" {
		return resp.Body, nil
	}

	switch resp.Header.Get(ContentEncodingHeader) {
	case "gzip":
		return gzip.NewReader(resp.Body)
	case "deflate":
		return flate.NewReader(resp.Body), nil
	}

	return resp.Body, nil
}

const (
	wgKey               = "__wg"
	clientRequestkey    = "clientRequest"
	headersKey          = "headers"
	UserFlag            = "user"
	ClientRequestKey    = "__wg_clientRequest"
	TraceRequestFuncKey = "traceRequestFunc"
)

func SetUserValue(ctx context.Context, input []byte) []byte {
	if user := ctx.Value(UserFlag); user != nil {
		userJson, _ := json.Marshal(user)
		input, _ = jsonparser.Set(input, userJson, wgKey, UserFlag)
	}
	if clientRequest, ok := ctx.Value(ClientRequestKey).(*http.Request); ok {
		headers := make(map[string]string)
		for k, v := range clientRequest.Header {
			headers[k] = strings.Join(v, ",")
		}
		headersJson, _ := json.Marshal(headers)
		input, _ = jsonparser.Set(input, headersJson, wgKey, clientRequestkey, headersKey)
	}
	return input
}

type TraceRequestFunc func(*http.Request, ...func(span opentracing.Span)) (*http.Request, func(...func(opentracing.Span)))

func TraceRequestFuncFromContext(ctx context.Context) (TraceRequestFunc, bool) {
	traceFunc, ok := ctx.Value(TraceRequestFuncKey).(TraceRequestFunc)
	return traceFunc, ok
}
