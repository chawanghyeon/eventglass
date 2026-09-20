package alerts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
)

type DeliveryControl interface {
	ClaimDelivery(context.Context, string, int64, string, time.Duration) (*control.Delivery, error)
	HeartbeatDelivery(context.Context, control.DeliveryAuthority, time.Duration) error
	CompleteDelivery(context.Context, control.DeliveryAuthority, int) error
	FailDelivery(context.Context, control.DeliveryAuthority, int, string, bool, time.Duration) error
}

type DeliveryWorker struct {
	Control           DeliveryControl
	Cipher            *SecretCipher
	Sender            Sender
	InstallationID    string
	StorageGeneration int64
	Owner             string
}

func (worker *DeliveryWorker) RunOnce(ctx context.Context) (bool, error) {
	if worker == nil || worker.Control == nil || worker.Cipher == nil || worker.InstallationID == "" || worker.StorageGeneration <= 0 || worker.Owner == "" {
		return false, errors.New("delivery worker dependencies are required")
	}
	delivery, err := worker.Control.ClaimDelivery(ctx, worker.InstallationID, worker.StorageGeneration, worker.Owner, DeliveryLease)
	if err != nil || delivery == nil {
		return false, err
	}
	bodyDigest := sha256.Sum256(delivery.BodyBytes)
	if hex.EncodeToString(bodyDigest[:]) != delivery.BodySHA256 {
		failErr := worker.Control.FailDelivery(context.WithoutCancel(ctx), delivery.Authority, 0, "body_checksum_mismatch", false, 0)
		return true, errors.Join(errors.New("delivery body checksum mismatch"), failErr)
	}
	var secret []byte
	if len(delivery.DestinationSecretCiphertext) > 0 {
		secret, err = worker.Cipher.Decrypt(delivery.DestinationEncryptionKeyID, delivery.DestinationSecretCiphertext)
		if err != nil {
			failErr := worker.Control.FailDelivery(context.WithoutCancel(ctx), delivery.Authority, 0, "credential_unavailable", false, 0)
			return true, errors.Join(err, failErr)
		}
		defer clear(secret)
	}
	sendContext, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan SendOutcome, 1)
	go func() {
		result <- worker.Sender.Send(sendContext, SendRequest{DeliveryID: delivery.Authority.DeliveryID, URL: delivery.DestinationURL, Body: delivery.BodyBytes, Secret: secret})
	}()
	ticker := time.NewTicker(DeliveryHeartbeat)
	defer ticker.Stop()
	var outcome SendOutcome
	for {
		select {
		case outcome = <-result:
			goto sent
		case <-ctx.Done():
			cancel()
			<-result
			return true, ctx.Err()
		case <-ticker.C:
			if heartbeatErr := worker.Control.HeartbeatDelivery(ctx, delivery.Authority, DeliveryLease); heartbeatErr != nil {
				cancel()
				<-result
				return true, heartbeatErr
			}
		}
	}

sent:
	if outcome.ErrorCode == "" && outcome.Status >= 200 && outcome.Status <= 299 {
		return true, worker.Control.CompleteDelivery(ctx, delivery.Authority, outcome.Status)
	}
	delay := time.Duration(0)
	if outcome.Retry {
		delay, err = RetryDelay(delivery.Attempt, outcome.RetryAfter)
		if err != nil {
			return true, err
		}
	}
	err = worker.Control.FailDelivery(ctx, delivery.Authority, outcome.Status, outcome.ErrorCode, outcome.Retry, delay)
	return true, err
}
