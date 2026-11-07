package login

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/xml"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/beevik/etree"
	sdk "github.com/crewjam/saml"
	dsig "github.com/russellhaering/goxmldsig"

	"github.com/nostosx/servex-contrib/auth/saml"
	"github.com/nostosx/servex-contrib/session"
)

func TestSAMLSessionComposition(t *testing.T) {
	t.Parallel()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	idpURL, _ := url.Parse("https://idp.example/metadata")
	ssoURL, _ := url.Parse("https://idp.example/sso")
	idp := &sdk.IdentityProvider{Key: key, Certificate: cert, MetadataURL: *idpURL, SSOURL: *ssoURL, SignatureMethod: dsig.RSASHA256SignatureMethod}
	metadata, err := xml.Marshal(idp.Metadata())
	if err != nil {
		t.Fatal(err)
	}
	states, _ := saml.NewMemoryStore(10)
	sp, err := saml.New(saml.Config{EntityID: "https://sp.example/entity", ACSURL: "https://sp.example/acs", Key: key, Certificate: cert, IDPMetadata: metadata}, states)
	if err != nil {
		t.Fatal(err)
	}
	store, _ := session.NewMemoryStore(10)
	sessions, _ := session.New(store, session.WithSameSite(http.SameSiteNoneMode))
	resolved := false
	manager, err := New(sessions, Config[principal]{Valid: func(p principal) bool { return p.ID != "" }, SuccessURL: "/private", SAML: &SAML[principal]{Client: sp, Resolve: func(_ context.Context, identity saml.Identity) (principal, error) {
		resolved = true
		return principal{ID: identity.Subject}, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/login", manager.Begin)
	mux.HandleFunc("/acs", manager.Callback)
	mux.Handle("/private", manager.Require(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "signed-in") })))
	handler := manager.Middleware(mux)
	begin := httptest.NewRecorder()
	handler.ServeHTTP(begin, httptest.NewRequest(http.MethodGet, "https://sp.example/login", nil))
	response := begin.Result()
	cookies := response.Cookies()
	_ = response.Body.Close()
	if begin.Code != http.StatusFound || len(cookies) != 1 || cookies[0].SameSite != http.SameSiteNoneMode || !cookies[0].Secure {
		t.Fatal(begin.Code, cookies)
	}
	idpRequest, err := sdk.NewIdpAuthnRequest(idp, httptest.NewRequest(http.MethodGet, begin.Header().Get("Location"), nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := xml.Unmarshal(idpRequest.RequestBuffer, &idpRequest.Request); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	idpRequest.ACSEndpoint = &sdk.IndexedEndpoint{Binding: sdk.HTTPPostBinding, Location: "https://sp.example/acs"}
	idpRequest.SPSSODescriptor = &sdk.SPSSODescriptor{}
	idpRequest.Assertion = &sdk.Assertion{ID: "assertion-1", Version: "2.0", IssueInstant: now, Issuer: sdk.Issuer{Value: idpURL.String()},
		Subject:         &sdk.Subject{NameID: &sdk.NameID{Value: "employee-1", Format: string(sdk.PersistentNameIDFormat)}, SubjectConfirmations: []sdk.SubjectConfirmation{{Method: "urn:oasis:names:tc:SAML:2.0:cm:bearer", SubjectConfirmationData: &sdk.SubjectConfirmationData{Recipient: "https://sp.example/acs", InResponseTo: idpRequest.Request.ID, NotOnOrAfter: now.Add(time.Minute)}}}},
		AuthnStatements: []sdk.AuthnStatement{{AuthnInstant: now, SessionIndex: "session-id"}},
		Conditions:      &sdk.Conditions{NotBefore: now.Add(-time.Minute), NotOnOrAfter: now.Add(time.Minute), AudienceRestrictions: []sdk.AudienceRestriction{{Audience: sdk.Audience{Value: "https://sp.example/entity"}}}}}
	if err := idpRequest.MakeResponse(); err != nil {
		t.Fatal(err)
	}
	doc := etree.NewDocument()
	doc.SetRoot(idpRequest.ResponseEl)
	data, err := doc.WriteToBytes()
	if err != nil {
		t.Fatal(err)
	}
	values := url.Values{"SAMLResponse": {base64.StdEncoding.EncodeToString(data)}, "RelayState": {idpRequest.RelayState}}
	send := func(method, contentType, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "https://sp.example/acs", strings.NewReader(body))
		r.Header.Set("Content-Type", contentType)
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	for _, tt := range []struct {
		name, method, contentType, body string
		status                          int
	}{
		{"wrong method", http.MethodGet, "", "", http.StatusMethodNotAllowed},
		{"wrong content type", http.MethodPost, "application/json", "{}", http.StatusBadRequest},
		{"duplicate RelayState", http.MethodPost, "application/x-www-form-urlencoded", "SAMLResponse=x&RelayState=a&RelayState=b", http.StatusUnauthorized},
		{"oversized", http.MethodPost, "application/x-www-form-urlencoded", "SAMLResponse=" + strings.Repeat("x", (2<<20)+1), http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if w := send(tt.method, tt.contentType, tt.body, cookies[0]); w.Code != tt.status || resolved {
				t.Fatal(w.Code, resolved)
			}
		})
	}
	if w := send(http.MethodPost, "application/x-www-form-urlencoded", values.Encode(), nil); w.Code != http.StatusUnauthorized || resolved {
		t.Fatal("unbound callback accepted", w.Code)
	}
	callback := send(http.MethodPost, "application/x-www-form-urlencoded", values.Encode(), cookies[0])
	response = callback.Result()
	logged := response.Cookies()
	_ = response.Body.Close()
	if callback.Code != http.StatusSeeOther || !resolved || len(logged) != 1 || logged[0].Value == cookies[0].Value {
		t.Fatal("login/rotation failed", callback.Code, logged)
	}
	r := httptest.NewRequest(http.MethodGet, "https://sp.example/private", nil)
	r.AddCookie(logged[0])
	private := httptest.NewRecorder()
	handler.ServeHTTP(private, r)
	if private.Code != http.StatusOK {
		t.Fatal(private.Code)
	}
	if w := send(http.MethodPost, "application/x-www-form-urlencoded", values.Encode(), cookies[0]); w.Code != http.StatusUnauthorized {
		t.Fatal("replay accepted", w.Code)
	}
}
