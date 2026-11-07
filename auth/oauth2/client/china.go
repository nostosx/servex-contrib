package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func (p *Provider) chineseProvider() bool {
	return p.kind == WeChat || p.kind == WeChatWeb || p.kind == QQ || p.kind == Weibo
}

// Provider APIs require credentials in URL parameters for WeChat/QQ and user
// queries. Never return URL-bearing transport errors or provider response bodies.
func (p *Provider) protocolJSON(ctx context.Context, method, endpoint string, values url.Values, target any) error {
	var body io.Reader
	if method == http.MethodGet {
		endpoint += "?" + values.Encode()
	} else {
		body = strings.NewReader(values.Encode())
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return errors.New("oauth2: invalid provider request")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "nostosx-oauth2")
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	response, err := p.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return errors.New("oauth2: provider request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("oauth2: provider request rejected")
	}
	bodyBytes, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(bodyBytes) > 1<<20 {
		return errors.New("oauth2: invalid provider response size")
	}
	// QQ fmt=json avoids executable callback wrappers. We do not eval JSONP.
	if err := json.Unmarshal(bodyBytes, target); err != nil {
		return errors.New("oauth2: invalid provider JSON response")
	}
	return nil
}

type chinaToken struct {
	AccessToken string          `json:"access_token"`
	OpenID      string          `json:"openid"`
	UnionID     string          `json:"unionid"`
	UID         string          `json:"uid"`
	Error       json.RawMessage `json:"error"`
	ErrorCode   int64           `json:"error_code"`
	ErrCode     int64           `json:"errcode"`
}

func protocolError(raw json.RawMessage) bool {
	return len(raw) != 0 && string(raw) != "0" && string(raw) != `"0"` && string(raw) != "null"
}

func (p *Provider) chineseIdentity(ctx context.Context, code string) (Identity, error) {
	config := p.flow.config
	values := url.Values{"grant_type": {"authorization_code"}, "code": {code}}
	method := http.MethodGet
	if p.kind == WeChat || p.kind == WeChatWeb {
		values.Set("appid", config.ClientID)
		values.Set("secret", config.ClientSecret)
	} else {
		values.Set("client_id", config.ClientID)
		values.Set("client_secret", config.ClientSecret)
		values.Set("redirect_uri", config.RedirectURL)
		if p.kind == QQ {
			values.Set("fmt", "json")
		} else {
			method = http.MethodPost
		}
	}
	var token chinaToken
	if err := p.protocolJSON(ctx, method, config.Endpoint.TokenURL, values, &token); err != nil {
		return Identity{}, err
	}
	if token.AccessToken == "" || protocolError(token.Error) || token.ErrCode != 0 || token.ErrorCode != 0 {
		return Identity{}, errors.New("oauth2: provider code exchange failed")
	}
	identity := Identity{Provider: p.kind, Issuer: p.issuer}
	switch p.kind {
	case WeChat, WeChatWeb:
		if token.OpenID == "" {
			return Identity{}, errors.New("oauth2: WeChat omitted OpenID")
		}
		var profile struct {
			OpenID   string `json:"openid"`
			UnionID  string `json:"unionid"`
			Nickname string `json:"nickname"`
			ErrCode  int64  `json:"errcode"`
		}
		if err := p.protocolJSON(ctx, http.MethodGet, "https://api.weixin.qq.com/sns/userinfo",
			url.Values{"access_token": {token.AccessToken}, "openid": {token.OpenID}, "lang": {"zh_CN"}}, &profile); err != nil {
			return Identity{}, err
		}
		if profile.ErrCode != 0 || profile.OpenID != token.OpenID || profile.OpenID == "" ||
			(token.UnionID != "" && profile.UnionID != "" && token.UnionID != profile.UnionID) {
			return Identity{}, errors.New("oauth2: invalid WeChat identity")
		}
		identity.Subject, identity.Name, identity.UnionID = profile.OpenID, profile.Nickname, profile.UnionID
		if identity.UnionID == "" {
			identity.UnionID = token.UnionID
		}
	case QQ:
		var me struct {
			ClientID string          `json:"client_id"`
			OpenID   string          `json:"openid"`
			Error    json.RawMessage `json:"error"`
		}
		if err := p.protocolJSON(ctx, http.MethodGet, "https://graph.qq.com/oauth2.0/me",
			url.Values{"access_token": {token.AccessToken}, "fmt": {"json"}}, &me); err != nil {
			return Identity{}, err
		}
		if me.ClientID != config.ClientID || me.OpenID == "" || protocolError(me.Error) {
			return Identity{}, errors.New("oauth2: QQ token is not bound to this application")
		}
		var profile struct {
			Ret      *int   `json:"ret"`
			Nickname string `json:"nickname"`
		}
		if err := p.protocolJSON(ctx, http.MethodGet, "https://graph.qq.com/user/get_user_info",
			url.Values{"access_token": {token.AccessToken}, "oauth_consumer_key": {config.ClientID}, "openid": {me.OpenID}}, &profile); err != nil {
			return Identity{}, err
		}
		if profile.Ret == nil || *profile.Ret != 0 {
			return Identity{}, errors.New("oauth2: QQ user information rejected")
		}
		identity.Subject, identity.Name = me.OpenID, profile.Nickname
	case Weibo:
		if !decimalID(token.UID) {
			return Identity{}, errors.New("oauth2: Weibo omitted UID")
		}
		var profile struct {
			ID        json.Number `json:"id"`
			IDString  string      `json:"idstr"`
			Name      string      `json:"screen_name"`
			ErrorCode int64       `json:"error_code"`
		}
		if err := p.protocolJSON(ctx, http.MethodGet, "https://api.weibo.com/2/users/show.json",
			url.Values{"access_token": {token.AccessToken}, "uid": {token.UID}}, &profile); err != nil {
			return Identity{}, err
		}
		id := profile.IDString
		if id == "" {
			id = profile.ID.String()
		}
		if profile.ErrorCode != 0 || id != token.UID || (profile.ID != "" && profile.ID.String() != token.UID) {
			return Identity{}, errors.New("oauth2: Weibo profile does not match token UID")
		}
		identity.Subject, identity.Name = token.UID, profile.Name
	}
	return identity, nil
}

func decimalID(value string) bool {
	if value == "" || value[0] == '0' || len(value) > 32 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
