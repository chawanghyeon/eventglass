package app

import (
	"errors"
	"fmt"
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
	DatabaseURL  string
	HTTPAddr     string
	PublicURL    string
	ScratchDir   string
	Roles        map[Role]bool
	S3           storage.S3Config
	DrainTimeout time.Duration
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
		PublicURL: value("EVENTGLASS_PUBLIC_URL"), ScratchDir: value("EVENTGLASS_SCRATCH_DIR"), Roles: roles,
		S3:           storage.S3Config{Endpoint: endpoint, Region: value("EVENTGLASS_S3_REGION"), Bucket: value("EVENTGLASS_S3_BUCKET"), Prefix: value("EVENTGLASS_S3_PREFIX"), PathStyle: pathStyle},
		DrainTimeout: 30 * time.Second,
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
	parsed, err := url.Parse(config.PublicURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("EVENTGLASS_PUBLIC_URL must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	if strings.HasPrefix(config.S3.Prefix, "/") || strings.Contains(config.S3.Prefix, "..") || strings.ContainsRune(config.S3.Prefix, '\\') {
		return errors.New("S3 prefix must be a safe relative prefix")
	}
	return nil
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
