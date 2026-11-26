package runtime

import "net/http"

const uiContentSecurityPolicy = "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:"

// SecurityHeadersMiddleware attaches baseline HTTP security headers to all
// responses and a Content-Security-Policy header on the embedded UI routes.
func SecurityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")

		if r.URL.Path == "/ui" || r.URL.Path == "/ui/" {
			h.Set("Content-Security-Policy", uiContentSecurityPolicy)
		}

		next.ServeHTTP(w, r)
	})
}
