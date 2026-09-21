package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/resource"
)

func TestCatalogQuotaReturnsTerminalQueryLimit(t *testing.T) {
	for _, cause := range []error{query.ErrCatalogLimit, fmt.Errorf("catalog: %w", query.ErrCatalogLimit)} {
		writer := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/search", nil)
		(&ManagementHandler{}).publicQueryError(writer, request, cause)
		if writer.Code != http.StatusUnprocessableEntity || !strings.Contains(writer.Body.String(), `"code":"query_limit_exceeded"`) || !strings.Contains(writer.Body.String(), `"retryable":false`) {
			t.Fatalf("catalog limit response=%d %s", writer.Code, writer.Body.String())
		}
	}
}

func TestPlanningAdmissionReturnsRetryableResponse(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
	}{{resource.ErrLimited, http.StatusTooManyRequests}, {resource.ErrDraining, http.StatusServiceUnavailable}} {
		writer := httptest.NewRecorder()
		(&ManagementHandler{}).publicQueryError(writer, httptest.NewRequest(http.MethodPost, "/v1/search", nil), test.err)
		if writer.Code != test.status || writer.Header().Get("Retry-After") != "1" || !strings.Contains(writer.Body.String(), `"retryable":true`) || !strings.Contains(writer.Body.String(), `"code":"admission_limited"`) {
			t.Fatalf("planning admission response=%d %s", writer.Code, writer.Body.String())
		}
	}
}
