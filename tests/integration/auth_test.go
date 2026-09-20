package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestSetupSessionCSRFRateLimitsAndAuthorizationInvariants(t *testing.T) {
	fixture := setupAcceptFixture(t, 950)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `DELETE FROM audit_events; DELETE FROM sessions; DELETE FROM project_grants; DELETE FROM memberships; DELETE FROM users`); err != nil {
		t.Fatal(err)
	}
	bootstrap := bytes.Repeat([]byte{0x42}, 32)
	bootstrapHash := sha256.Sum256(bootstrap)
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET setup_state='uninitialized',setup_attempt=NULL,
		setup_request_fingerprint=NULL,setup_marker_key=NULL,setup_marker_sha256=NULL,setup_owner=NULL,setup_fence=0,
		setup_lease_until=NULL,bootstrap_token_hash=$1,setup_completed_at=NULL WHERE singleton`, bootstrapHash[:]); err != nil {
		t.Fatal(err)
	}
	environment := requiredEnvironment(t, "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET")
	s3Config := storage.S3Config{Endpoint: environment["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: environment["EVENTGLASS_S3_BUCKET"], Prefix: "auth-setup-950", PathStyle: true}
	identity, err := app.StorageIdentity(s3Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET storage_identity=$1 WHERE singleton`, identity); err != nil {
		t.Fatal(err)
	}
	store := integrationStore(t, s3Config.Prefix)
	auth, err := control.NewAuthOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	passwords, err := api.NewPasswordHasher(resource.NewBudget(64 << 20))
	if err != nil {
		t.Fatal(err)
	}
	loginKey := sha256.Sum256([]byte("integration login limiter key"))
	handler, err := api.NewManagementHandler(api.ManagementConfig{
		Auth: auth, Passwords: passwords, PublicOrigin: "http://127.0.0.1", CookieName: "eventglass_dev_session",
		SecureCookie: false, LoginBucketKey: loginKey, BuildMarker: app.InstallationMarker,
		StoreMarker: func(ctx context.Context, key string, body []byte, checksum string) error {
			return store.EnsureImmutableObject(ctx, key, body, checksum)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	handler.Register(mux)
	body, _ := json.Marshal(generated.SetupRequest{BootstrapToken: hex.EncodeToString(bootstrap), Email: "ADMIN@Example.invalid ", Password: "correct horse battery staple", TenantName: "Primary"})
	descriptor, err := auth.SetupDescriptor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, markerKey, _, err := app.InstallationMarker(descriptor.InstallationID, descriptor.StorageIdentity)
	if err != nil {
		t.Fatal(err)
	}
	conflictingMarker := []byte("conflicting installation marker")
	if _, err := store.Put(ctx, markerKey, conflictingMarker); err != nil {
		t.Fatal(err)
	}
	conflictRequest := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/setup", bytes.NewReader(body))
	conflictRequest.Header.Set("Origin", "http://127.0.0.1")
	conflictRequest.Header.Set("Content-Type", "application/json")
	conflictResponse := httptest.NewRecorder()
	mux.ServeHTTP(conflictResponse, conflictRequest)
	storedConflict, readErr := store.ReadRange(ctx, markerKey, 0, int64(len(conflictingMarker)))
	if conflictResponse.Code != http.StatusServiceUnavailable || readErr != nil || !bytes.Equal(storedConflict, conflictingMarker) {
		t.Fatalf("conflicting marker status=%d stored=%q err=%v body=%s", conflictResponse.Code, storedConflict, readErr, conflictResponse.Body.String())
	}
	differentBody, _ := json.Marshal(generated.SetupRequest{BootstrapToken: hex.EncodeToString(bootstrap), Email: "other@example.invalid", Password: "different bootstrap password", TenantName: "Primary"})
	differentRequest := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/setup", bytes.NewReader(differentBody))
	differentRequest.Header.Set("Origin", "http://127.0.0.1")
	differentRequest.Header.Set("Content-Type", "application/json")
	differentResponse := httptest.NewRecorder()
	mux.ServeHTTP(differentResponse, differentRequest)
	if differentResponse.Code != http.StatusConflict {
		t.Fatalf("different setup request reused attempt: status=%d body=%s", differentResponse.Code, differentResponse.Body.String())
	}
	if err := store.Delete(ctx, []string{markerKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET setup_lease_until=clock_timestamp()-interval '1 second' WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	type setupAnswer struct {
		code   int
		cookie *http.Cookie
		body   []byte
	}
	answers := make(chan setupAnswer, 2)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			ready.Done()
			<-start
			request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/setup", bytes.NewReader(body))
			request.Header.Set("Origin", "http://127.0.0.1")
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			result := response.Result()
			var cookie *http.Cookie
			if cookies := result.Cookies(); len(cookies) > 0 {
				cookie = cookies[0]
			}
			answers <- setupAnswer{code: response.Code, cookie: cookie, body: response.Body.Bytes()}
		}()
	}
	ready.Wait()
	close(start)
	var success setupAnswer
	conflicts := 0
	for range 2 {
		answer := <-answers
		if answer.code == http.StatusCreated {
			success = answer
		} else if answer.code == http.StatusConflict {
			conflicts++
		} else {
			t.Fatalf("setup status=%d body=%s", answer.code, answer.body)
		}
	}
	if success.cookie == nil || conflicts != 1 {
		t.Fatalf("setup success=%#v conflicts=%d", success, conflicts)
	}
	var retentionFloor, retentionRevision int64
	var retentionDays int
	if err := fixture.pool.QueryRow(ctx, `SELECT retention_days,retention_revision,retention_floor_us FROM installations WHERE singleton`).Scan(&retentionDays, &retentionRevision, &retentionFloor); err != nil || retentionDays != 30 || retentionRevision != 1 || retentionFloor <= 0 {
		t.Fatalf("setup retention days=%d revision=%d floor=%d err=%v", retentionDays, retentionRevision, retentionFloor, err)
	}
	var session generated.Session
	if err := json.Unmarshal(success.body, &session); err != nil || session.Email != "admin@example.invalid" || len(session.Tenants) != 1 || session.CsrfToken == "" {
		t.Fatalf("session=%#v err=%v body=%s", session, err, success.body)
	}

	get := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/v1/session", nil)
	get.AddCookie(success.cookie)
	getResponse := httptest.NewRecorder()
	mux.ServeHTTP(getResponse, get)
	var reloaded generated.Session
	if getResponse.Code != http.StatusOK || json.Unmarshal(getResponse.Body.Bytes(), &reloaded) != nil || reloaded.CsrfToken != session.CsrfToken {
		t.Fatalf("reload status=%d session=%#v body=%s", getResponse.Code, reloaded, getResponse.Body.String())
	}
	secondLoginBody, _ := json.Marshal(generated.LoginRequest{Email: "admin@example.invalid", Password: "correct horse battery staple"})
	secondLogin := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/sessions", bytes.NewReader(secondLoginBody))
	secondLogin.RemoteAddr = "192.0.2.11:1234"
	secondLogin.Header.Set("Origin", "http://127.0.0.1")
	secondLogin.Header.Set("Content-Type", "application/json")
	secondLoginResponse := httptest.NewRecorder()
	mux.ServeHTTP(secondLoginResponse, secondLogin)
	var secondSession generated.Session
	if secondLoginResponse.Code != http.StatusOK || json.Unmarshal(secondLoginResponse.Body.Bytes(), &secondSession) != nil || secondSession.CsrfToken == session.CsrfToken {
		t.Fatalf("independent session status=%d session=%#v body=%s", secondLoginResponse.Code, secondSession, secondLoginResponse.Body.String())
	}
	badLogout := httptest.NewRequest(http.MethodDelete, "http://127.0.0.1/v1/session", nil)
	badLogout.AddCookie(success.cookie)
	badLogout.Header.Set("Origin", "http://127.0.0.1")
	badLogout.Header.Set("X-CSRF-Token", "wrong")
	badResponse := httptest.NewRecorder()
	mux.ServeHTTP(badResponse, badLogout)
	if badResponse.Code != http.StatusForbidden {
		t.Fatalf("bad CSRF status=%d", badResponse.Code)
	}

	tenantID := parseWireInt64(t, session.Tenants[0].TenantId)
	userID := parseWireInt64(t, session.UserId)
	role := "member"
	if _, err := auth.ReplaceMembership(ctx, control.MembershipUpdate{TenantID: tenantID, TargetUserID: userID, ExpectedRevision: 1, Role: &role}); err != control.ErrLastAdmin {
		t.Fatalf("last admin mutation=%v", err)
	}
	otherUserID := userID + 1000
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO users(user_id,email_normalized,password_phc) VALUES($1,$2,$3)`, otherUserID, "member@example.invalid", "$argon2id$v=19$m=65536,t=3,p=2$ZXZlbnRnbGFzcy1kdW1teQ$3fS7jbTBnU4p8kMPSY1XANFKxf3f6OUt0OXbKsCB9BA"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'member')`, tenantID, otherUserID); err != nil {
		t.Fatal(err)
	}
	otherTenant := tenantID + 1000
	otherProject := otherTenant*10 + 1
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO tenants(tenant_id,name) VALUES($1,'Other')`, otherTenant); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES($1,$2,1)`, otherTenant, otherProject); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.ReplaceMembership(ctx, control.MembershipUpdate{TenantID: tenantID, TargetUserID: otherUserID, ExpectedRevision: 1, Role: &role, ProjectGrants: []control.ProjectGrant{{ProjectID: otherProject, Role: "viewer"}}}); err != control.ErrForbidden {
		t.Fatalf("cross-tenant grant=%v", err)
	}

	projectID := tenantID*10 + 77
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES($1,$2,1)`, tenantID, projectID); err != nil {
		t.Fatal(err)
	}
	memberTokenHash := sha256.Sum256([]byte("member session token"))
	memberCSRFHash := sha256.Sum256([]byte("member csrf token"))
	memberCredential, err := auth.LoadUserCredential(ctx, otherUserID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.CreateSession(ctx, memberCredential, memberTokenHash, memberCSRFHash, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if revision, err := auth.ReplaceMembership(ctx, control.MembershipUpdate{
		TenantID: tenantID, TargetUserID: otherUserID, ExpectedRevision: 1, Role: &role,
		ProjectGrants: []control.ProjectGrant{{ProjectID: projectID, Role: "viewer"}},
	}); err != nil || revision != 2 {
		t.Fatalf("add project grant revision=%d err=%v", revision, err)
	}
	memberSession, err := auth.AuthenticateSession(ctx, memberTokenHash)
	if err != nil || len(memberSession.Tenants) != 1 || len(memberSession.Tenants[0].ProjectGrants) != 1 {
		t.Fatalf("session did not reload grant: %#v err=%v", memberSession, err)
	}
	if revision, err := auth.ReplaceMembership(ctx, control.MembershipUpdate{
		TenantID: tenantID, TargetUserID: otherUserID, ExpectedRevision: 2, Role: &role,
	}); err != nil || revision != 3 {
		t.Fatalf("remove project grant revision=%d err=%v", revision, err)
	}
	memberSession, err = auth.AuthenticateSession(ctx, memberTokenHash)
	if err != nil || len(memberSession.Tenants) != 1 || len(memberSession.Tenants[0].ProjectGrants) != 0 {
		t.Fatalf("session retained revoked grant: %#v err=%v", memberSession, err)
	}
	keyBody, _ := json.Marshal(generated.KeyCreate{TenantId: generated.Int64(fmt.Sprint(tenantID)), Label: "browser SDK"})
	createKey := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1/v1/projects/%d/keys", projectID), bytes.NewReader(keyBody))
	createKey.AddCookie(success.cookie)
	createKey.Header.Set("Origin", "http://127.0.0.1")
	createKey.Header.Set("X-CSRF-Token", session.CsrfToken)
	createKey.Header.Set("Content-Type", "application/json")
	createdKeyResponse := httptest.NewRecorder()
	mux.ServeHTTP(createdKeyResponse, createKey)
	var createdKey generated.CreatedKey
	if createdKeyResponse.Code != http.StatusCreated || json.Unmarshal(createdKeyResponse.Body.Bytes(), &createdKey) != nil || len(createdKey.PublicKey) != 64 || createdKey.Dsn == "" {
		t.Fatalf("create key status=%d key=%#v body=%s", createdKeyResponse.Code, createdKey, createdKeyResponse.Body.String())
	}
	listKeys := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1/v1/projects/%d/keys?tenant_id=%d", projectID, tenantID), nil)
	listKeys.AddCookie(success.cookie)
	listedKeyResponse := httptest.NewRecorder()
	mux.ServeHTTP(listedKeyResponse, listKeys)
	if listedKeyResponse.Code != http.StatusOK || bytes.Contains(listedKeyResponse.Body.Bytes(), []byte(createdKey.PublicKey)) || bytes.Contains(listedKeyResponse.Body.Bytes(), []byte("dsn")) {
		t.Fatalf("key secret repeated status=%d body=%s", listedKeyResponse.Code, listedKeyResponse.Body.String())
	}
	secret, tokenHash := decodeCookieHash(t, success.cookie)
	_ = secret
	if _, err := auth.AuthorizeProjects(ctx, tokenHash, tenantID, []int64{projectID}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE projects SET state='disabled',auth_revision=auth_revision+1 WHERE tenant_id=$1 AND project_id=$2`, tenantID, projectID); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.AuthorizeProjects(ctx, tokenHash, tenantID, []int64{projectID}, false); err != control.ErrForbidden {
		t.Fatalf("disabled scope=%v", err)
	}
	publicKeyHash := sha256.Sum256([]byte("sdk-public-key-does-not-read"))
	if _, err := auth.AuthorizeProjects(ctx, publicKeyHash, tenantID, nil, false); err != control.ErrUnauthenticated {
		t.Fatalf("SDK key read authority=%v", err)
	}

	userCreateBody, _ := json.Marshal(generated.UserCreate{TenantId: fmt.Sprint(tenantID), Email: "managed@example.invalid", InitialPassword: "managed password value", Role: generated.UserCreateRole("member"), ProjectGrants: []generated.ProjectGrant{{ProjectId: fmt.Sprint(projectID), Role: generated.ProjectGrantRole("viewer")}}})
	badUserCreate := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/users", bytes.NewReader(userCreateBody))
	badUserCreate.AddCookie(success.cookie)
	badUserCreate.Header.Set("Origin", "http://127.0.0.1")
	badUserCreate.Header.Set("Content-Type", "application/json")
	badUserResponse := httptest.NewRecorder()
	mux.ServeHTTP(badUserResponse, badUserCreate)
	if badUserResponse.Code != http.StatusForbidden {
		t.Fatalf("user mutation without CSRF status=%d body=%s", badUserResponse.Code, badUserResponse.Body.String())
	}
	userCreate := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/users", bytes.NewReader(userCreateBody))
	userCreate.AddCookie(success.cookie)
	userCreate.Header.Set("Origin", "http://127.0.0.1")
	userCreate.Header.Set("Content-Type", "application/json")
	userCreate.Header.Set("X-CSRF-Token", session.CsrfToken)
	userResponse := httptest.NewRecorder()
	mux.ServeHTTP(userResponse, userCreate)
	var managed generated.User
	if userResponse.Code != http.StatusCreated || json.Unmarshal(userResponse.Body.Bytes(), &managed) != nil || managed.Email != "managed@example.invalid" || managed.Role == nil || *managed.Role != generated.UserRole("member") {
		t.Fatalf("create user status=%d user=%#v body=%s", userResponse.Code, managed, userResponse.Body.String())
	}
	listUsers := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1/v1/users?tenant_id=%d&limit=1", tenantID), nil)
	listUsers.AddCookie(success.cookie)
	listUsersResponse := httptest.NewRecorder()
	mux.ServeHTTP(listUsersResponse, listUsers)
	var users generated.UserList
	if listUsersResponse.Code != http.StatusOK || json.Unmarshal(listUsersResponse.Body.Bytes(), &users) != nil || len(users.Items) != 1 || users.NextCursor == "" {
		t.Fatalf("user page status=%d users=%#v body=%s", listUsersResponse.Code, users, listUsersResponse.Body.String())
	}
	patchUserBody := []byte(fmt.Sprintf(`{"tenant_id":"%d","revision":"%s","state":"disabled","role":"member","project_grants":[]}`, tenantID, managed.Revision))
	patchUser := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("http://127.0.0.1/v1/users/%s", managed.UserId), bytes.NewReader(patchUserBody))
	patchUser.AddCookie(success.cookie)
	patchUser.Header.Set("Origin", "http://127.0.0.1")
	patchUser.Header.Set("Content-Type", "application/json")
	patchUser.Header.Set("X-CSRF-Token", session.CsrfToken)
	patchUserResponse := httptest.NewRecorder()
	mux.ServeHTTP(patchUserResponse, patchUser)
	var disabledUser generated.User
	if patchUserResponse.Code != http.StatusOK || json.Unmarshal(patchUserResponse.Body.Bytes(), &disabledUser) != nil || disabledUser.State != generated.UserState("disabled") || disabledUser.Revision == managed.Revision {
		t.Fatalf("patch user status=%d user=%#v body=%s", patchUserResponse.Code, disabledUser, patchUserResponse.Body.String())
	}
	stalePatch := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("http://127.0.0.1/v1/users/%s", managed.UserId), bytes.NewReader(patchUserBody))
	stalePatch.AddCookie(success.cookie)
	stalePatch.Header.Set("Origin", "http://127.0.0.1")
	stalePatch.Header.Set("Content-Type", "application/json")
	stalePatch.Header.Set("X-CSRF-Token", session.CsrfToken)
	staleResponse := httptest.NewRecorder()
	mux.ServeHTTP(staleResponse, stalePatch)
	if staleResponse.Code != http.StatusConflict {
		t.Fatalf("stale user patch status=%d body=%s", staleResponse.Code, staleResponse.Body.String())
	}

	systemRequest := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1/v1/system?tenant_id=%d", tenantID), nil)
	systemRequest.AddCookie(success.cookie)
	systemResponse := httptest.NewRecorder()
	mux.ServeHTTP(systemResponse, systemRequest)
	var system generated.System
	if systemResponse.Code != http.StatusOK || json.Unmarshal(systemResponse.Body.Bytes(), &system) != nil || len(system.Lanes) != 16 || system.Backup.State == "" {
		t.Fatalf("system status=%d system=%#v body=%s", systemResponse.Code, system, systemResponse.Body.String())
	}
	badRetentionBody := []byte(fmt.Sprintf(`{"tenant_id":"%d","revision":"%s","retention_days":45}`, tenantID, system.Retention.Revision))
	badRetention := httptest.NewRequest(http.MethodPatch, "http://127.0.0.1/v1/system/retention", bytes.NewReader(badRetentionBody))
	badRetention.AddCookie(success.cookie)
	badRetention.Header.Set("Origin", "http://127.0.0.1")
	badRetention.Header.Set("Content-Type", "application/json")
	badRetentionResponse := httptest.NewRecorder()
	mux.ServeHTTP(badRetentionResponse, badRetention)
	if badRetentionResponse.Code != http.StatusForbidden {
		t.Fatalf("retention without CSRF status=%d body=%s", badRetentionResponse.Code, badRetentionResponse.Body.String())
	}
	retentionRequest := httptest.NewRequest(http.MethodPatch, "http://127.0.0.1/v1/system/retention", bytes.NewReader(badRetentionBody))
	retentionRequest.AddCookie(success.cookie)
	retentionRequest.Header.Set("Origin", "http://127.0.0.1")
	retentionRequest.Header.Set("Content-Type", "application/json")
	retentionRequest.Header.Set("X-CSRF-Token", session.CsrfToken)
	retentionResponse := httptest.NewRecorder()
	mux.ServeHTTP(retentionResponse, retentionRequest)
	var retention generated.Retention
	if retentionResponse.Code != http.StatusOK || json.Unmarshal(retentionResponse.Body.Bytes(), &retention) != nil || retention.Days != 45 || retention.Revision == system.Retention.Revision {
		t.Fatalf("retention status=%d retention=%#v body=%s", retentionResponse.Code, retention, retentionResponse.Body.String())
	}

	loginBody := []byte(`{"email":"missing@example.invalid","password":"not the right password"}`)
	for attempt := 1; attempt <= 6; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/sessions", bytes.NewReader(loginBody))
		request.RemoteAddr = "192.0.2.10:1234"
		request.Header.Set("Origin", "http://127.0.0.1")
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if attempt <= 5 && response.Code != http.StatusUnauthorized {
			t.Fatalf("login attempt %d status=%d body=%s", attempt, response.Code, response.Body.String())
		}
		if attempt == 6 && response.Code != http.StatusTooManyRequests {
			t.Fatalf("limited login status=%d body=%s", response.Code, response.Body.String())
		}
	}
	credential, found, err := auth.LoadLoginCredential(ctx, "admin@example.invalid")
	if err != nil || !found {
		t.Fatalf("load credential found=%v err=%v", found, err)
	}
	newPasswordPHC, err := passwords.Hash("new correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.ChangePassword(ctx, credential.UserID, credential.CredentialRevision, newPasswordPHC); err != nil {
		t.Fatal(err)
	}
	staleTokenHash := sha256.Sum256([]byte("stale login token"))
	staleCSRFHash := sha256.Sum256([]byte("stale login csrf"))
	if _, err := auth.CreateSession(ctx, credential, staleTokenHash, staleCSRFHash, time.Now().Add(time.Hour)); err != control.ErrUnauthenticated {
		t.Fatalf("stale verified credential created session: %v", err)
	}
	if _, err := auth.AuthenticateSession(ctx, tokenHash); err != control.ErrUnauthenticated {
		t.Fatalf("password change retained original session: %v", err)
	}
}

func parseWireInt64(t *testing.T, value string) int64 {
	t.Helper()
	var result int64
	if _, err := fmt.Sscan(value, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func decodeCookieHash(t *testing.T, cookie *http.Cookie) ([]byte, [32]byte) {
	t.Helper()
	secret, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		t.Fatal(err)
	}
	return secret, sha256.Sum256(secret)
}
