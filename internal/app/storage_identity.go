package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/storage"
)

type storageIdentityDocument struct {
	Version  int    `json:"version"`
	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix"`
}

type installationMarker struct {
	FormatVersion      int    `json:"format_version"`
	InstallationID     string `json:"installation_id"`
	StorageIdentitySHA string `json:"storage_identity_sha"`
}

func StorageIdentity(config storage.S3Config) (string, error) {
	endpoint := strings.TrimRight(config.Endpoint, "/")
	if endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return "", errors.New("invalid S3 endpoint")
		}
		parsed.Scheme = strings.ToLower(parsed.Scheme)
		parsed.Host = strings.ToLower(parsed.Host)
		endpoint = strings.TrimRight(parsed.String(), "/")
	}
	document := storageIdentityDocument{Version: 1, Endpoint: endpoint, Region: config.Region, Bucket: config.Bucket, Prefix: strings.Trim(config.Prefix, "/")}
	encoded, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func InstallationMarker(installationID, storageIdentity string) ([]byte, string, string, error) {
	identityDigest := sha256.Sum256([]byte(storageIdentity))
	document := installationMarker{FormatVersion: 1, InstallationID: installationID, StorageIdentitySHA: hex.EncodeToString(identityDigest[:])}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, "", "", err
	}
	digest := sha256.Sum256(encoded)
	key := "v1/" + installationID + "/installation.json"
	return encoded, key, hex.EncodeToString(digest[:]), nil
}
