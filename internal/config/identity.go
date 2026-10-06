package config

import (
	"crypto/sha256"
	"encoding/hex"
)

// Fingerprint binds checkpoints to the resolved endpoints without persisting secrets.
// V1 intentionally requires the same connection strings on resume, even after a
// password rotation. A changed endpoint must never silently reuse an old cursor.
func (c Config) Fingerprint() (string, error) {
	a, err := c.Source.Resolve()
	if err != nil {
		return "", err
	}
	b, err := c.Destination.Resolve()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(c.Source.Type + "\x00" + a + "\x00" + c.Source.Database + "\x00" + c.Destination.Type + "\x00" + b + "\x00" + c.Destination.Database))
	return hex.EncodeToString(sum[:]), nil
}
