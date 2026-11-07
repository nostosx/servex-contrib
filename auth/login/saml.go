package login

import (
	"mime"
	"net/http"

	"github.com/nostosx/servex-contrib/session"
)

// SAML POST response validation stays within the session boundary. Applications
// mount only this exact ACS path as the cross-origin POST/CSRF exception, using
// Secure SameSite=None sessions; all other browser mutations retain protection.
func (m *Manager[P]) samlCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/x-www-form-urlencoded" {
		http.Error(w, "login failed", http.StatusBadRequest)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "login failed", http.StatusBadRequest)
		return
	}
	if len(r.PostForm["SAMLResponse"]) != 1 || len(r.PostForm["RelayState"]) != 1 || r.PostForm.Get("SAMLart") != "" {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	current, ok := session.FromContext(r.Context())
	if !ok {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	var binding string
	found, err := current.Get(m.bindingKey(), &binding)
	if err != nil || !found {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	identity, err := m.config.SAML.Client.Complete(r.Context(), binding, r.PostForm.Get("RelayState"), r.PostForm.Get("SAMLResponse"))
	if err != nil {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	principal, err := m.config.SAML.Resolve(r.Context(), identity)
	if err != nil || m.SignIn(r.Context(), principal) != nil {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	http.Redirect(w, r, m.config.SuccessURL, http.StatusSeeOther)
}
