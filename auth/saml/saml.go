// Package saml implements SAML 2.0 SP-initiated browser SSO using gosaml2.
// Applications own account mapping, local sessions and HTTP middleware.
package saml

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/beevik/etree"
	xrv "github.com/mattermost/xml-roundtrip-validator"
	sdk "github.com/russellhaering/gosaml2"
	"github.com/russellhaering/gosaml2/types"
	dsig "github.com/russellhaering/goxmldsig"
)

const (
	assertionNS            = "urn:oasis:names:tc:SAML:2.0:assertion"
	metadataNS             = "urn:oasis:names:tc:SAML:2.0:metadata"
	protocolNS             = "urn:oasis:names:tc:SAML:2.0:protocol"
	signatureNS            = "http://www.w3.org/2000/09/xmldsig#"
	PersistentNameIDFormat = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"
	TransientNameIDFormat  = "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"
)

var ErrInvalidResponse = errors.New("saml: invalid, expired or consumed login response")

// Config is trusted administrator input. Metadata comes from an authenticated
// IdP administration channel, never from the browser or assertion. RSA key and
// certificate are borrowed immutable signing/decryption material.
type Config struct {
	EntityID     string
	ACSURL       string
	IDPMetadata  []byte
	Key          *rsa.PrivateKey
	Certificate  *x509.Certificate
	StateTTL     time.Duration
	NameIDFormat string
}

// Attempt is sensitive server-side correlation state. Binding combines the
// original browser/session and trusted provider configuration.
type Attempt struct {
	RequestID string
	Binding   [32]byte
	ExpiresAt time.Time
}

// StateStore saves insert-only and atomically takes/deletes a live attempt.
// Replicas require shared state; never retry an uncertain Take operation.
type StateStore interface {
	Save(context.Context, string, Attempt) error
	Take(context.Context, string) (Attempt, error)
}

// Identity contains verified IdP claims, not local permissions. Account keys
// include the configured tenant, issuer, subject, NameID format and qualifiers.
type Identity struct {
	Issuer          string
	Subject         string
	NameIDFormat    string
	NameQualifier   string
	SPNameQualifier string
	Attributes      map[string][]string
	SessionIndex    string
	ExpiresAt       time.Time
}

type Authorization struct {
	URL       string
	ExpiresAt time.Time
}

// ServiceProvider signs Redirect AuthnRequests and validates POST responses,
// including signed encrypted assertions. No implicit router or metadata fetch.
// IdP-initiated SSO, artifacts, SOAP and SLO are not accepted.
type ServiceProvider struct {
	provider       *sdk.SAMLServiceProvider
	store          StateStore
	ttl            time.Duration
	fingerprint    string
	metadataExpiry time.Time
	certificate    *x509.Certificate
	key            *rsa.PrivateKey
}

func New(config Config, store StateStore) (*ServiceProvider, error) {
	if store == nil || config.Key == nil || config.Certificate == nil || config.Key.N.BitLen() < 2048 {
		return nil, errors.New("saml: state store and RSA key/certificate of at least 2048 bits required")
	}
	if err := config.Key.Validate(); err != nil {
		return nil, errors.New("saml: invalid RSA key")
	}
	public, ok := config.Certificate.PublicKey.(*rsa.PublicKey)
	now := time.Now()
	if !ok || !public.Equal(&config.Key.PublicKey) || now.Before(config.Certificate.NotBefore) || !now.Before(config.Certificate.NotAfter) {
		return nil, errors.New("saml: current SP certificate matching key required")
	}
	if err := httpsURL(config.ACSURL); err != nil {
		return nil, err
	}
	if strings.TrimSpace(config.EntityID) == "" || len(config.EntityID) > 2048 {
		return nil, errors.New("saml: explicit SP entity ID required")
	}
	if len(config.IDPMetadata) == 0 || len(config.IDPMetadata) > 1<<20 {
		return nil, errors.New("saml: bounded trusted metadata required")
	}
	metadataDoc, err := parseXML(config.IDPMetadata)
	if err != nil || metadataDoc.Root().Tag != "EntityDescriptor" || metadataDoc.Root().NamespaceURI() != metadataNS ||
		len(children(metadataDoc.Root(), metadataNS, "IDPSSODescriptor")) != 1 {
		return nil, errors.New("saml: select exactly one explicit IdP EntityDescriptor")
	}
	var metadata types.EntityDescriptor
	if err := xml.Unmarshal(config.IDPMetadata, &metadata); err != nil || metadata.EntityID == "" || metadata.IDPSSODescriptor == nil {
		return nil, errors.New("saml: invalid IdP metadata")
	}
	if !metadata.ValidUntil.IsZero() && !now.Before(metadata.ValidUntil) {
		return nil, errors.New("saml: IdP metadata expired")
	}
	var certs []*x509.Certificate
	for _, descriptor := range metadata.IDPSSODescriptor.KeyDescriptors {
		if descriptor.Use != "" && descriptor.Use != "signing" {
			continue
		}
		for _, value := range descriptor.KeyInfo.X509Data.X509Certificates {
			data, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(value.Data), ""))
			if err != nil {
				return nil, errors.New("saml: invalid IdP signing certificate")
			}
			cert, err := x509.ParseCertificate(data)
			if err != nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
				return nil, errors.New("saml: invalid or expired IdP signing certificate")
			}
			certs = append(certs, cert)
		}
	}
	if len(certs) == 0 {
		return nil, errors.New("saml: trusted IdP signing certificate required")
	}
	var sso string
	for _, endpoint := range metadata.IDPSSODescriptor.SingleSignOnServices {
		if endpoint.Binding == sdk.BindingHttpRedirect {
			sso = endpoint.Location
			break
		}
	}
	if err := httpsURL(sso); err != nil {
		return nil, errors.New("saml: HTTPS Redirect SSO endpoint required")
	}
	if config.StateTTL == 0 {
		config.StateTTL = 5 * time.Minute
	}
	if config.StateTTL < time.Second || config.StateTTL > 10*time.Minute {
		return nil, errors.New("saml: state TTL must be one second to ten minutes")
	}
	if config.NameIDFormat == "" {
		config.NameIDFormat = PersistentNameIDFormat
	}
	if config.NameIDFormat == TransientNameIDFormat {
		return nil, errors.New("saml: transient NameID cannot identify a persistent account")
	}
	provider := &sdk.SAMLServiceProvider{
		IdentityProviderSSOURL: sso, IdentityProviderSSOBinding: sdk.BindingHttpRedirect, IdentityProviderIssuer: metadata.EntityID,
		AssertionConsumerServiceURL: config.ACSURL, ServiceProviderIssuer: config.EntityID, AudienceURI: config.EntityID,
		SignAuthnRequests: true, SignAuthnRequestsAlgorithm: dsig.RSASHA256SignatureMethod, NameIdFormat: config.NameIDFormat,
		IDPCertificateStore: &dsig.MemoryX509CertificateStore{Roots: certs}, MaximumDecompressedBodySize: 1 << 20, MaximumXMLTokens: 20000,
	}
	if err := provider.SetSPKeyStore(&sdk.KeyStore{Signer: config.Key, Cert: config.Certificate.Raw}); err != nil {
		return nil, err
	}
	// gosaml2 v0.12's response decryption still reads this native field even when
	// SetSPKeyStore is used. Supply the same key; never disable signature checks.
	provider.SPKeyStore = dsig.TLSCertKeyStore(tls.Certificate{Certificate: [][]byte{config.Certificate.Raw}, PrivateKey: config.Key})
	digest := sha256.Sum256([]byte(strings.Join([]string{config.EntityID, config.ACSURL, metadata.EntityID, sso}, "\x00")))
	return &ServiceProvider{provider: provider, store: store, ttl: config.StateTTL, fingerprint: hex.EncodeToString(digest[:]), metadataExpiry: metadata.ValidUntil, certificate: config.Certificate, key: config.Key}, nil
}

func httpsURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.ForceQuery {
		return errors.New("saml: absolute HTTPS URL without query, userinfo or fragment required")
	}
	return nil
}

// Metadata returns a fresh serialized descriptor for registration with the IdP.
func (s *ServiceProvider) Metadata() ([]byte, error) {
	metadata, err := s.provider.Metadata()
	if err != nil {
		return nil, err
	}
	return xml.MarshalIndent(metadata, "", "  ")
}
func (s *ServiceProvider) binding(browser string) [32]byte {
	return sha256.Sum256([]byte(s.fingerprint + "\x00" + browser))
}
func stateKey(state string) string {
	digest := sha256.Sum256([]byte(state))
	return hex.EncodeToString(digest[:])
}

func (s *ServiceProvider) Begin(ctx context.Context, browser string) (Authorization, error) {
	if err := context.Cause(ctx); err != nil {
		return Authorization{}, err
	}
	if browser == "" {
		return Authorization{}, ErrInvalidResponse
	}
	now := time.Now()
	if !now.Before(s.certificate.NotAfter) || (!s.metadataExpiry.IsZero() && !now.Before(s.metadataExpiry)) {
		return Authorization{}, errors.New("saml: certificate or metadata must be refreshed")
	}
	request, err := s.provider.BuildAuthRequestDocumentNoSig()
	if err != nil {
		return Authorization{}, errors.New("saml: cannot construct authentication request")
	}
	relay := rand.Text()
	redirect, err := s.provider.BuildAuthURLRedirect(relay, request)
	if err != nil {
		return Authorization{}, errors.New("saml: cannot sign authentication request")
	}
	expires := now.Add(s.ttl)
	if err := s.store.Save(ctx, stateKey(relay), Attempt{RequestID: request.Root().SelectAttrValue("ID", ""), Binding: s.binding(browser), ExpiresAt: expires}); err != nil {
		return Authorization{}, err
	}
	return Authorization{URL: redirect, ExpiresAt: expires}, nil
}

// Complete consumes correlation before validating XML. The caller extracts
// exactly one RelayState/SAMLResponse from a bounded POST form, never the query.
// Signature errors and raw XML are deliberately not exposed to the caller.
func (s *ServiceProvider) Complete(ctx context.Context, browser, relay, response string) (Identity, error) {
	if err := context.Cause(ctx); err != nil {
		return Identity{}, err
	}
	if browser == "" || relay == "" || len(relay) > 80 || response == "" || len(response) > 1<<20 {
		return Identity{}, ErrInvalidResponse
	}
	attempt, err := s.store.Take(ctx, stateKey(relay))
	if err != nil {
		return Identity{}, err
	}
	binding := s.binding(browser)
	now := time.Now()
	if attempt.RequestID == "" || !now.Before(attempt.ExpiresAt) || subtle.ConstantTimeCompare(attempt.Binding[:], binding[:]) != 1 ||
		(!s.metadataExpiry.IsZero() && !now.Before(s.metadataExpiry)) {
		return Identity{}, ErrInvalidResponse
	}
	raw, err := base64.StdEncoding.DecodeString(response)
	if err != nil {
		return Identity{}, ErrInvalidResponse
	}
	// Inspect structure/algorithms only to reject unsupported inputs. No identity
	// is accepted from this unverified tree. The SDK validates the ORIGINAL response.
	assertionXML, err := s.inspectResponse(raw)
	if err != nil {
		return Identity{}, ErrInvalidResponse
	}
	verified, err := s.provider.ValidateEncodedResponse(response)
	if err != nil || len(verified.Assertions) != 1 || verified.InResponseTo != attempt.RequestID {
		return Identity{}, ErrInvalidResponse
	}
	if err := context.Cause(ctx); err != nil {
		return Identity{}, err
	}
	assertion := verified.Assertions[0]
	if assertion.ID == "" || assertion.ID != assertionXML.SelectAttrValue("ID", "") || assertion.Subject == nil || assertion.Subject.NameID == nil ||
		assertion.Subject.NameID.Value == "" || assertion.Subject.NameID.Format == TransientNameIDFormat || assertion.AuthnStatement == nil ||
		assertion.Conditions == nil || assertion.Issuer == nil ||
		assertion.IssueInstant.After(now.Add(time.Minute)) || assertion.IssueInstant.Before(now.Add(-90*time.Second)) {
		return Identity{}, ErrInvalidResponse
	}
	confirmation := assertion.Subject.SubjectConfirmation
	if confirmation == nil || confirmation.SubjectConfirmationData == nil || confirmation.SubjectConfirmationData.InResponseTo != attempt.RequestID {
		return Identity{}, ErrInvalidResponse
	}
	warning, err := s.provider.VerifyAssertionConditions(&assertion)
	if err != nil || warning.InvalidTime || warning.NotInAudience || len(assertion.Conditions.AudienceRestrictions) == 0 {
		return Identity{}, ErrInvalidResponse
	}
	expiry, err := time.Parse(time.RFC3339, assertion.Conditions.NotOnOrAfter)
	if err != nil || !now.Before(expiry) {
		return Identity{}, ErrInvalidResponse
	}
	name := assertion.Subject.NameID
	if name.SPNameQualifier != "" && name.SPNameQualifier != s.provider.ServiceProviderIssuer {
		return Identity{}, ErrInvalidResponse
	}
	identity := Identity{Issuer: assertion.Issuer.Value, Subject: name.Value, NameIDFormat: name.Format, NameQualifier: name.NameQualifier, SPNameQualifier: name.SPNameQualifier,
		Attributes: make(map[string][]string), ExpiresAt: expiry, SessionIndex: assertion.AuthnStatement.SessionIndex}
	if assertion.AttributeStatement != nil {
		for _, attribute := range assertion.AttributeStatement.Attributes {
			for _, value := range attribute.Values {
				identity.Attributes[attribute.Name] = append(identity.Attributes[attribute.Name], value.Value)
			}
		}
	}
	if expiry := assertion.AuthnStatement.SessionNotOnOrAfter; expiry != nil && expiry.Before(identity.ExpiresAt) {
		identity.ExpiresAt = *expiry
	}
	if !now.Before(identity.ExpiresAt) {
		return Identity{}, ErrInvalidResponse
	}
	return identity, nil
}

func parseXML(raw []byte) (*etree.Document, error) {
	if len(raw) == 0 || len(raw) > 1<<20 {
		return nil, ErrInvalidResponse
	}
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	depth, tokens := 0, 0
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, ErrInvalidResponse
		}
		tokens++
		switch token.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		case xml.Directive:
			return nil, ErrInvalidResponse
		}
		if tokens > 20000 || depth > 64 {
			return nil, ErrInvalidResponse
		}
	}
	if err := xrv.Validate(bytes.NewReader(raw)); err != nil {
		return nil, err
	}
	doc := etree.NewDocument()
	if err := doc.ReadFromBytes(raw); err != nil {
		return nil, err
	}
	if doc.Root() == nil {
		return nil, ErrInvalidResponse
	}
	return doc, nil
}
func children(element *etree.Element, namespace, tag string) []*etree.Element {
	var found []*etree.Element
	for _, child := range element.ChildElements() {
		if child.Tag == tag && child.NamespaceURI() == namespace {
			found = append(found, child)
		}
	}
	return found
}

// Extra profile constraints and SHA-256+ policy supplement the SDK. This tree
// is used only for rejection; identity claims come from SDK-verified assertions.
func (s *ServiceProvider) inspectResponse(raw []byte) (*etree.Element, error) {
	doc, err := parseXML(raw)
	if err != nil {
		return nil, err
	}
	root := doc.Root()
	if root.Tag != "Response" || root.NamespaceURI() != protocolNS {
		return nil, ErrInvalidResponse
	}
	if err := strongAlgorithms(root); err != nil {
		return nil, err
	}
	assertions := children(root, assertionNS, "Assertion")
	encrypted := children(root, assertionNS, "EncryptedAssertion")
	if len(assertions)+len(encrypted) != 1 {
		return nil, ErrInvalidResponse
	}
	var assertion *etree.Element
	if len(encrypted) == 1 {
		encryptedDoc := etree.NewDocument()
		encryptedDoc.SetRoot(encrypted[0].Copy())
		data, err := encryptedDoc.WriteToBytes()
		if err != nil {
			return nil, err
		}
		var encryptedAssertion types.EncryptedAssertion
		if err := xml.Unmarshal(data, &encryptedAssertion); err != nil {
			return nil, err
		}
		plaintext, err := encryptedAssertion.DecryptBytes(&tls.Certificate{Certificate: [][]byte{s.certificate.Raw}, PrivateKey: s.key})
		if err != nil {
			return nil, err
		}
		decrypted, err := parseXML(plaintext)
		if err != nil {
			return nil, err
		}
		assertion = decrypted.Root()
	} else {
		assertion = assertions[0]
	}
	if assertion.Tag != "Assertion" || assertion.NamespaceURI() != assertionNS {
		return nil, ErrInvalidResponse
	}
	if err := strongAlgorithms(assertion); err != nil {
		return nil, err
	}
	subjects := children(assertion, assertionNS, "Subject")
	if len(subjects) != 1 || len(children(subjects[0], assertionNS, "NameID")) != 1 {
		return nil, ErrInvalidResponse
	}
	if len(children(assertion, assertionNS, "Issuer")) != 1 || len(children(assertion, assertionNS, "Conditions")) != 1 || len(children(assertion, assertionNS, "AuthnStatement")) != 1 {
		return nil, ErrInvalidResponse
	}
	confirmations := children(subjects[0], assertionNS, "SubjectConfirmation")
	if len(confirmations) != 1 {
		return nil, ErrInvalidResponse
	}
	data := children(confirmations[0], assertionNS, "SubjectConfirmationData")
	if len(data) != 1 {
		return nil, ErrInvalidResponse
	}
	if value := data[0].SelectAttrValue("NotBefore", ""); value != "" {
		instant, err := time.Parse(time.RFC3339, value)
		if err != nil || instant.After(time.Now().Add(time.Minute)) {
			return nil, ErrInvalidResponse
		}
	}
	return assertion, nil
}

func strongAlgorithms(element *etree.Element) error {
	for _, child := range element.ChildElements() {
		if child.NamespaceURI() == signatureNS {
			switch child.Tag {
			case "SignatureMethod":
				if element.Tag != "SignedInfo" || element.NamespaceURI() != signatureNS {
					continue
				}
				switch child.SelectAttrValue("Algorithm", "") {
				case dsig.RSASHA256SignatureMethod, dsig.RSASHA384SignatureMethod, dsig.RSASHA512SignatureMethod, dsig.ECDSASHA256SignatureMethod, dsig.ECDSASHA384SignatureMethod, dsig.ECDSASHA512SignatureMethod:
				default:
					return ErrInvalidResponse
				}
			case "DigestMethod":
				// XML encryption's OAEP DigestMethod is not a signing digest.
				if element.Tag != "Reference" || element.NamespaceURI() != signatureNS {
					continue
				}
				switch child.SelectAttrValue("Algorithm", "") {
				case "http://www.w3.org/2001/04/xmlenc#sha256", "http://www.w3.org/2001/04/xmldsig-more#sha384", "http://www.w3.org/2001/04/xmlenc#sha512":
				default:
					return ErrInvalidResponse
				}
			}
		}
		if err := strongAlgorithms(child); err != nil {
			return err
		}
	}
	return nil
}
