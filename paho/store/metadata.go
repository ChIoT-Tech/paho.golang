/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * All rights reserved. This program and the accompanying materials are made
 * available under the Eclipse Public License v2.0 and Eclipse Distribution
 * License v1.0 which accompany this distribution.
 * https://www.eclipse.org/legal/epl-2.0/
 * http://www.eclipse.org/org/documents/edl-v10.php
 *
 * AI Disclosure: This file was largely generated with OpenAI Codex (GPT-6).
 * The AI-generated portions are made available under CC0-1.0.
 * SPDX-License-Identifier: (EPL-2.0 OR BSD-3-Clause) AND CC0-1.0
 * Assisted-by: OpenAI GPT-6
 */

// Package store defines local metadata shared by publication queues and session stores.
package store

import (
	"errors"
	"fmt"
)

// MaxTrackingIDLength bounds metadata reads independently of the MQTT packet size.
const MaxTrackingIDLength = 65535

// ErrInvalidMetadata identifies a malformed persisted metadata record, as opposed
// to a transient read error. Queue entries with this error may be quarantined.
var ErrInvalidMetadata = errors.New("invalid persisted publication metadata")

// Metadata belongs to a publication, independently of its MQTT packet identifier
// and whether its current stored packet is PUBLISH or PUBREL. It is never sent
// to the broker. Values may be retained after a store operation or callback.
type Metadata struct {
	// ID is an application-supplied correlation ID. Empty means untracked.
	// IDs are opaque strings; no UUID format or MQTT UTF-8 restriction applies.
	// Applications are responsible for uniqueness across their publications.
	ID string
	// QoS is the submitted publication's QoS, including after replacement by PUBREL.
	// Queues can contain invalid submissions; protocol validation remains the client's responsibility.
	QoS byte
}

// Validate checks the limits supported by the built-in queues and stores.
func (m Metadata) Validate() error {
	if len(m.ID) > MaxTrackingIDLength {
		return fmt.Errorf("tracking ID exceeds %d bytes", MaxTrackingIDLength)
	}
	return nil
}
