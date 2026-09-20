package app

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/storage"
)

type Role string

const (
	RoleAPI       Role = "api"
	RoleWorker    Role = "worker"
	RoleScheduler Role = "scheduler"
)

type Config struct {
	DatabaseURL            string
	HTTPAddr               string
	PublicURL              string
	ScratchDir             string
	BootstrapTokenFile     string
	AuthHashKeyFile        string
	TokenKeyFile           string
	AlertEncryptionKeyFile string
	InsecureCookie         bool
	Roles                  map[Role]bool
	S3                     storage.S3Config
	DrainTimeout           time.Duration
}

func LoadConfigFromEnv(lookup func(string) (string, bool)) (Config, error) {
	value := func(name string) string {
		result, _ := lookup(name)
		return strings.TrimSpace(result)
	}
	roles, err := parseRoles(value("EVENTGLASS_ROLES"))
	if err != nil {
		return Config{}, err
	}
	endpoint := strings.TrimRight(value("EVENTGLASS_S3_ENDPOINT"), "/")
	pathStyle := endpoint != ""
	if raw := value("EVENTGLASS_S3_PATH_STYLE"); raw != "" {
		pathStyle, err = strconv.ParseBool(raw)
		if err != nil {
			return Config{}, errors.New("EVENTGLASS_S3_PATH_STYLE must be true or false")
		}
	}
	config := Config{
		DatabaseURL: value("EVENTGLASS_DATABASE_URL"), HTTPAddr: value("EVENTGLASS_HTTP_ADDR"),
		PublicURL: value("EVENTGLASS_PUBLIC_URL"), ScratchDir: value("EVENTGLASS_SCRATCH_DIR"),
		BootstrapTokenFile: value("EVENTGLASS_BOOTSTRAP_TOKEN_FILE"), AuthHashKeyFile: value("EVENTGLASS_AUTH_HASH_KEY_FILE"),
		TokenKeyFile: value("EVENTGLASS_TOKEN_KEY_FILE"), AlertEncryptionKeyFile: value("EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE"), Roles: roles,
		S3:           storage.S3Config{Endpoint: endpoint, Region: value("EVENTGLASS_S3_REGION"), Bucket: value("EVENTGLASS_S3_BUCKET"), Prefix: value("EVENTGLASS_S3_PREFIX"), PathStyle: pathStyle},
		DrainTimeout: 30 * time.Second,
	}
	if raw := value("EVENTGLASS_INSECURE_COOKIE"); raw != "" {
		config.InsecureCookie, err = strconv.ParseBool(raw)
		if err != nil {
			return Config{}, errors.New("EVENTGLASS_INSECURE_COOKIE must be true or false")
		}
	}
	if config.HTTPAddr == "" {
		config.HTTPAddr = ":8080"
	}
	return config, config.Validate()
}

func (config Config) Validate() error {
	if config.DatabaseURL == "" || config.PublicURL == "" || config.ScratchDir == "" || config.S3.Region == "" || config.S3.Bucket == "" {
		return errors.New("database URL, public URL, scratch directory, S3 region, and S3 bucket are required")
	}
	if config.HTTPAddr == "" || config.DrainTimeout <= 0 || config.DrainTimeout > 30*time.Second {
		return errors.New("HTTP address and a positive drain timeout up to 30s are required")
	}
	if len(config.Roles) == 0 {
		return errors.New("at least one Eventglass role is required")
	}
	for role, enabled := range config.Roles {
		if !enabled || (role != RoleAPI && role != RoleWorker && role != RoleScheduler) {
			return fmt.Errorf("unsupported role %q", role)
		}
	}
	if config.Roles[RoleAPI] && (config.AuthHashKeyFile == "" || config.TokenKeyFile == "") {
		return errors.New("EVENTGLASS_AUTH_HASH_KEY_FILE and EVENTGLASS_TOKEN_KEY_FILE are required for the API role")
	}
	if (config.Roles[RoleAPI] || config.Roles[RoleWorker]) && config.AlertEncryptionKeyFile == "" {
		return errors.New("EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE is required for API and worker roles")
	}
	parsed, err := url.Parse(config.PublicURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("EVENTGLASS_PUBLIC_URL must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	if strings.HasPrefix(config.S3.Prefix, "/") || strings.Contains(config.S3.Prefix, "..") || strings.ContainsRune(config.S3.Prefix, '\\') {
		return errors.New("S3 prefix must be a safe relative prefix")
	}
	if config.InsecureCookie && (parsed.Scheme != "http" || !loopbackHost(parsed.Hostname())) {
		return errors.New("insecure cookies require a loopback-only HTTP public URL")
	}
	if config.Roles[RoleAPI] && parsed.Scheme != "https" && !config.InsecureCookie {
		return errors.New("the API role requires HTTPS unless loopback insecure-cookie mode is explicit")
	}
	return nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func parseRoles(raw string) (map[Role]bool, error) {
	if raw == "" {
		return nil, errors.New("EVENTGLASS_ROLES is required")
	}
	roles := make(map[Role]bool)
	for _, part := range strings.Split(raw, ",") {
		role := Role(strings.TrimSpace(part))
		if role != RoleAPI && role != RoleWorker && role != RoleScheduler {
			return nil, fmt.Errorf("unsupported role %q", role)
		}
		if roles[role] {
			return nil, fmt.Errorf("duplicate role %q", role)
		}
		roles[role] = true
	}
	return roles, nil
}
