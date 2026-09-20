package app

import "github.com/google/uuid"

func randomUUID() (string, error) { value, err := uuid.NewRandom(); return value.String(), err }
