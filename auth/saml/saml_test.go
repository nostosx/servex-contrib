package saml

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"math/big"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beevik/etree"
	sdk "github.com/crewjam/saml"
	dsig "github.com/russellhaering/goxmldsig"
)

func testKey(t *testing.T) (*rsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment}
	data, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(data)
	if err != nil {
		t.Fatal(err)
	}
	return key, parsed
}

func newSAMLFixture(t *testing.T) (*ServiceProvider, *sdk.IdentityProvider, Config) {
	t.Helper()
	key, cert := testKey(t)
	idpURL, _ := url.Parse("https://idp.example/metadata")
	sso, _ := url.Parse("https://idp.example/sso")
	idp := &sdk.IdentityProvider{Key: key, Certificate: cert, MetadataURL: *idpURL, SSOURL: *sso, SignatureMethod: dsig.RSASHA256SignatureMethod}
	metadata, err := xml.Marshal(idp.Metadata())
	if err != nil {
		t.Fatal(err)
	}
	spKey, spCert := testKey(t)
	config := Config{EntityID: "https://sp.example/entity", ACSURL: "https://sp.example/acs", IDPMetadata: metadata, Key: spKey, Certificate: spCert}
	store, _ := NewMemoryStore(20)
	sp, err := New(config, store)
	if err != nil {
		t.Fatal(err)
	}
	return sp, idp, config
}

func beginSAML(t *testing.T, sp *ServiceProvider) (string, sdk.AuthnRequest) {
	t.Helper()
	a, err := sp.Begin(t.Context(), "browser")
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(a.URL)
	if u.Query().Get("Signature") == "" || u.Query().Get("SigAlg") != dsig.RSASHA256SignatureMethod {
		t.Fatal("unsigned AuthnRequest")
	}
	compressed, err := base64.StdEncoding.DecodeString(u.Query().Get("SAMLRequest"))
	if err != nil {
		t.Fatal(err)
	}
	reader := flate.NewReader(bytes.NewReader(compressed))
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	var request sdk.AuthnRequest
	if err := xml.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	if request.ProtocolBinding != sdk.HTTPPostBinding || request.AssertionConsumerServiceURL != sp.provider.AssertionConsumerServiceURL {
		t.Fatal("wrong request binding")
	}
	return u.Query().Get("RelayState"), request
}

func signedSAML(t *testing.T, sp *ServiceProvider, idp *sdk.IdentityProvider, request sdk.AuthnRequest, encrypted bool, change func(*sdk.Assertion)) string {
	t.Helper()
	now := time.Now().UTC()
	assertion := &sdk.Assertion{ID: "assertion-" + rand.Text(), Version: "2.0", IssueInstant: now, Issuer: sdk.Issuer{Value: idp.MetadataURL.String()},
		Subject:             &sdk.Subject{NameID: &sdk.NameID{Value: "user-id", Format: string(sdk.PersistentNameIDFormat)}, SubjectConfirmations: []sdk.SubjectConfirmation{{Method: "urn:oasis:names:tc:SAML:2.0:cm:bearer", SubjectConfirmationData: &sdk.SubjectConfirmationData{Recipient: sp.provider.AssertionConsumerServiceURL, InResponseTo: request.ID, NotOnOrAfter: now.Add(time.Minute)}}}},
		Conditions:          &sdk.Conditions{NotBefore: now.Add(-time.Minute), NotOnOrAfter: now.Add(time.Minute), AudienceRestrictions: []sdk.AudienceRestriction{{Audience: sdk.Audience{Value: sp.provider.ServiceProviderIssuer}}}},
		AuthnStatements:     []sdk.AuthnStatement{{AuthnInstant: now, SessionIndex: "session-id"}},
		AttributeStatements: []sdk.AttributeStatement{{Attributes: []sdk.Attribute{{Name: "displayName", Values: []sdk.AttributeValue{{Value: "Enterprise User"}}}}}},
	}
	if change != nil {
		change(assertion)
	}
	metadataXML, err := sp.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	var metadata sdk.EntityDescriptor
	if err := xml.Unmarshal(metadataXML, &metadata); err != nil {
		t.Fatal(err)
	}
	descriptor := metadata.SPSSODescriptors[0]
	if !encrypted {
		descriptor.KeyDescriptors = nil
	}
	req := sdk.IdpAuthnRequest{IDP: idp, Request: request, Assertion: assertion, Now: now, SPSSODescriptor: &descriptor, ACSEndpoint: &sdk.IndexedEndpoint{Binding: sdk.HTTPPostBinding, Location: sp.provider.AssertionConsumerServiceURL}}
	if err := req.MakeResponse(); err != nil {
		t.Fatal(err)
	}
	doc := etree.NewDocument()
	doc.SetRoot(req.ResponseEl)
	data, err := doc.WriteToBytes()
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func TestSPLoginSignedAndEncrypted(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "signed", true: "encrypted"}[encrypted], func(t *testing.T) {
			t.Parallel()
			sp, idp, _ := newSAMLFixture(t)
			metadata, err := sp.Metadata()
			if err != nil || !bytes.Contains(metadata, []byte("AuthnRequestsSigned=\"true\"")) {
				t.Fatal(string(metadata), err)
			}
			relay, request := beginSAML(t, sp)
			response := signedSAML(t, sp, idp, request, encrypted, nil)
			raw, _ := base64.StdEncoding.DecodeString(response)
			if _, err := sp.inspectResponse(raw); err != nil {
				t.Fatalf("inspect fixture: %v", err)
			}
			if _, err := sp.provider.ValidateEncodedResponse(response); err != nil {
				t.Fatalf("verify fixture: %v", err)
			}
			identity, err := sp.Complete(t.Context(), "browser", relay, response)
			if err != nil || identity.Subject != "user-id" || identity.Issuer != idp.MetadataURL.String() || identity.SessionIndex != "session-id" || len(identity.Attributes["displayName"]) != 1 {
				t.Fatalf("identity=%+v error=%v", identity, err)
			}
			if _, err := sp.Complete(t.Context(), "browser", relay, response); !errors.Is(err, ErrInvalidResponse) {
				t.Fatal("replay accepted", err)
			}
		})
	}
}

func TestSPRejectsInvalidAssertions(t *testing.T) {
	sp, idp, _ := newSAMLFixture(t)
	for _, tt := range []struct {
		name   string
		change func(*sdk.Assertion)
	}{
		{"wrong issuer", func(a *sdk.Assertion) { a.Issuer.Value = "https://attacker.example" }},
		{"wrong audience", func(a *sdk.Assertion) { a.Conditions.AudienceRestrictions[0].Audience.Value = "https://other.example" }},
		{"missing audience", func(a *sdk.Assertion) { a.Conditions.AudienceRestrictions = nil }},
		{"conflicting restrictions", func(a *sdk.Assertion) {
			a.Conditions.AudienceRestrictions = append(a.Conditions.AudienceRestrictions, sdk.AudienceRestriction{Audience: sdk.Audience{Value: "https://other.example"}})
		}},
		{"expired", func(a *sdk.Assertion) { a.Conditions.NotOnOrAfter = time.Now().Add(-time.Hour) }},
		{"not yet valid", func(a *sdk.Assertion) { a.Conditions.NotBefore = time.Now().Add(time.Hour) }},
		{"future issued", func(a *sdk.Assertion) { a.IssueInstant = time.Now().Add(time.Hour) }},
		{"missing subject", func(a *sdk.Assertion) { a.Subject = nil }},
		{"missing conditions", func(a *sdk.Assertion) { a.Conditions = nil }},
		{"missing confirmation", func(a *sdk.Assertion) { a.Subject.SubjectConfirmations = nil }},
		{"wrong recipient", func(a *sdk.Assertion) {
			a.Subject.SubjectConfirmations[0].SubjectConfirmationData.Recipient = "https://other.example"
		}},
		{"confirmation not yet valid", func(a *sdk.Assertion) {
			a.Subject.SubjectConfirmations[0].SubjectConfirmationData.NotBefore = time.Now().Add(time.Hour)
		}},
		{"expired confirmation", func(a *sdk.Assertion) {
			a.Subject.SubjectConfirmations[0].SubjectConfirmationData.NotOnOrAfter = time.Now().Add(-time.Second)
		}},
		{"missing authentication statement", func(a *sdk.Assertion) { a.AuthnStatements = nil }},
		{"wrong request", func(a *sdk.Assertion) {
			a.Subject.SubjectConfirmations[0].SubjectConfirmationData.InResponseTo = "other-request"
		}},
		{"transient NameID", func(a *sdk.Assertion) { a.Subject.NameID.Format = string(sdk.TransientNameIDFormat) }},
		{"wrong SP qualifier", func(a *sdk.Assertion) { a.Subject.NameID.SPNameQualifier = "other-sp" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			relay, request := beginSAML(t, sp)
			response := signedSAML(t, sp, idp, request, false, tt.change)
			if _, err := sp.Complete(t.Context(), "browser", relay, response); !errors.Is(err, ErrInvalidResponse) {
				t.Fatal("invalid assertion accepted", err)
			}
		})
	}
}

func TestSPRejectsUntrustedTamperedAndUnbound(t *testing.T) {
	sp, idp, config := newSAMLFixture(t)
	for _, name := range []string{"wrong browser", "wrong SP", "tampered", "unsigned", "untrusted key", "sha1", "unsolicited", "malformed"} {
		t.Run(name, func(t *testing.T) {
			relay, request := beginSAML(t, sp)
			if name == "unsolicited" {
				request.ID = ""
			}
			copyIDP := *idp
			if name == "untrusted key" {
				copyIDP.Key, copyIDP.Certificate = testKey(t)
			}
			if name == "sha1" {
				copyIDP.SignatureMethod = dsig.RSASHA1SignatureMethod
			}
			response := signedSAML(t, sp, &copyIDP, request, false, nil)
			browser := "browser"
			target := sp
			switch name {
			case "wrong browser":
				browser = "other-browser"
			case "wrong SP":
				config.EntityID = "https://other.example/entity"
				var err error
				target, err = New(config, sp.store)
				if err != nil {
					t.Fatal(err)
				}
			case "tampered", "unsigned":
				data, _ := base64.StdEncoding.DecodeString(response)
				if name == "tampered" {
					data = bytes.ReplaceAll(data, []byte("user-id"), []byte("attacker"))
				} else {
					doc := etree.NewDocument()
					if err := doc.ReadFromBytes(data); err != nil {
						t.Fatal(err)
					}
					for _, sig := range doc.FindElements("//Signature") {
						sig.Parent().RemoveChild(sig)
					}
					data, _ = doc.WriteToBytes()
				}
				response = base64.StdEncoding.EncodeToString(data)
			case "malformed":
				response = "invalid-base64"
			}
			if _, err := target.Complete(t.Context(), browser, relay, response); !errors.Is(err, ErrInvalidResponse) {
				t.Fatal("invalid response accepted", err)
			}
		})
	}
}

func TestMemoryStoreAtomicityAndExpiry(t *testing.T) {
	store, _ := NewMemoryStore(1)
	attempt := Attempt{RequestID: "request", ExpiresAt: time.Now().Add(time.Minute)}
	if err := store.Save(t.Context(), "one", attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), "one", attempt); !errors.Is(err, ErrInvalidResponse) {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), "two", attempt); err == nil {
		t.Fatal("capacity ignored")
	}
	var wait sync.WaitGroup
	var winners atomic.Int32
	for range 20 {
		wait.Go(func() {
			if _, err := store.Take(t.Context(), "one"); err == nil {
				winners.Add(1)
			}
		})
	}
	wait.Wait()
	if winners.Load() != 1 {
		t.Fatal(winners.Load())
	}
	store.states["expired"] = Attempt{RequestID: "old", ExpiresAt: time.Now().Add(-time.Second)}
	if _, err := store.Take(t.Context(), "expired"); !errors.Is(err, ErrInvalidResponse) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.Save(ctx, "one", attempt); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := store.Take(ctx, "one"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := NewMemoryStore(0); err == nil {
		t.Fatal("invalid capacity accepted")
	}
}

func TestSignedForeignNameIDAndWrappingRejected(t *testing.T) {
	sp, idp, _ := newSAMLFixture(t)
	for _, name := range []string{"foreign NameID", "duplicate assertions", "missing issuer"} {
		t.Run(name, func(t *testing.T) {
			relay, request := beginSAML(t, sp)
			raw, _ := base64.StdEncoding.DecodeString(signedSAML(t, sp, idp, request, false, nil))
			doc := etree.NewDocument()
			if err := doc.ReadFromBytes(raw); err != nil {
				t.Fatal(err)
			}
			root := doc.Root()
			assertion := children(root, assertionNS, "Assertion")[0]
			// Re-sign the malicious fixture's envelope so rejection cannot be
			// attributed merely to a broken signature after test mutation.
			for _, element := range []*etree.Element{root, assertion} {
				for _, sig := range children(element, signatureNS, "Signature") {
					element.RemoveChild(sig)
				}
			}
			switch name {
			case "foreign NameID":
				subject := children(assertion, assertionNS, "Subject")[0]
				nameID := children(subject, assertionNS, "NameID")[0]
				nameID.Space = "foreign"
				nameID.CreateAttr("xmlns:foreign", "https://attacker.example/namespace")
			case "duplicate assertions":
				root.AddChild(assertion.Copy())
			case "missing issuer":
				assertion.RemoveChild(children(assertion, assertionNS, "Issuer")[0])
			}
			signer, err := sdk.GetSigningContext(&sdk.ServiceProvider{Key: idp.Key.(*rsa.PrivateKey), Certificate: idp.Certificate, SignatureMethod: dsig.RSASHA256SignatureMethod})
			if err != nil {
				t.Fatal(err)
			}
			signed, err := signer.SignEnveloped(root)
			if err != nil {
				t.Fatal(err)
			}
			doc.SetRoot(signed)
			raw, err = doc.WriteToBytes()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sp.Complete(t.Context(), "browser", relay, base64.StdEncoding.EncodeToString(raw)); !errors.Is(err, ErrInvalidResponse) {
				t.Fatal("signed malicious assertion accepted", err)
			}
		})
	}
}

func TestXMLBounds(t *testing.T) {
	for _, body := range []string{
		strings.Repeat("<a>", 65) + strings.Repeat("</a>", 65),
		`<!DOCTYPE a [<!ENTITY value "payload">]><a>&value;</a>`,
		"<a>" + strings.Repeat("<b/>", 11000) + "</a>",
	} {
		if _, err := parseXML([]byte(body)); err == nil {
			t.Fatal("unbounded XML accepted")
		}
	}
}

func TestSPInvalidConfiguration(t *testing.T) {
	_, _, valid := newSAMLFixture(t)
	for _, tt := range []struct {
		name   string
		change func(*Config)
	}{
		{"no key", func(c *Config) { c.Key = nil }},
		{"zero RSA key", func(c *Config) { c.Key = &rsa.PrivateKey{} }},
		{"no cert", func(c *Config) { c.Certificate = nil }},
		{"empty entity", func(c *Config) { c.EntityID = "" }},
		{"http ACS", func(c *Config) { c.ACSURL = "http://sp.example/acs" }},
		{"query ACS", func(c *Config) { c.ACSURL += "?x=y" }},
		{"untrusted XML", func(c *Config) { c.IDPMetadata = []byte("broken") }},
		{"transient requested", func(c *Config) { c.NameIDFormat = string(sdk.TransientNameIDFormat) }},
		{"invalid TTL", func(c *Config) { c.StateTTL = time.Hour }},
		{"oversized metadata", func(c *Config) { c.IDPMetadata = []byte(strings.Repeat(" ", (1<<20)+1)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := valid
			tt.change(&config)
			store, _ := NewMemoryStore(1)
			if _, err := New(config, store); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
