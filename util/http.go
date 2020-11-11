package util

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/mikeskali/PerfectScalePoc/util/mapper"
)

//--------------------------------------------------------------------------
//  QueryParams
//--------------------------------------------------------------------------

type QueryParams = mapper.PrimitiveMap

// queryParamsMap is mapper.Map adapter for url.Values
type queryParamsMap struct {
	values url.Values
}

// mapper.Getter implementation
func (qpm *queryParamsMap) Get(key string) string {
	return qpm.values.Get(key)
}

// mapper.Setter implementation
func (qpm *queryParamsMap) Set(key, value string) error {
	qpm.values.Set(key, value)
	return nil
}

// NewQueryParams creates a primitive map using the request query parameters
func NewQueryParams(values url.Values) QueryParams {
	return mapper.NewMapper(&queryParamsMap{values})
}

//--------------------------------------------------------------------------
//  Package Funcs
//--------------------------------------------------------------------------

// HeaderString writes the request/response http.Header to a string.
func HeaderString(h http.Header) string {
	var sb strings.Builder
	var first bool = true
	sb.WriteString("{ ")

	for k, vs := range h {
		if first {
			first = false
		} else {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s: [ ", k)
		for idx, v := range vs {
			sb.WriteString(v)
			if idx != len(vs)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(" ]")
	}
	sb.WriteString(" }")

	return sb.String()
}
