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

// Package persistence encodes local metadata before an otherwise unmodified MQTT packet.
package persistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/eclipse/paho.golang/paho/store"
)

// The zero first byte cannot begin a valid MQTT packet. Legacy packet-only files
// remain readable. Version 1 contains magic, version, QoS, uint16 ID length, ID,
// and then the MQTT packet. No separate metadata file is needed.
var magic = [5]byte{0, 'P', 'A', 'H', 'O'}

// WriteHeader writes a versioned metadata prefix. The caller must publish the
// prefix and packet atomically; this function does not commit a record.
func WriteHeader(w io.Writer, metadata store.Metadata) error {
	if err := metadata.Validate(); err != nil {
		return err
	}
	header := make([]byte, 9+len(metadata.ID))
	copy(header, magic[:])
	header[5], header[6] = 1, metadata.QoS
	binary.BigEndian.PutUint16(header[7:9], uint16(len(metadata.ID)))
	copy(header[9:], metadata.ID)
	n, err := w.Write(header)
	if err == nil && n != len(header) {
		err = io.ErrShortWrite
	}
	return err
}

// ReadHeader returns copied metadata and a reader positioned at the MQTT packet.
// It reads no packet body and performs no read-ahead. Legacy files return empty
// metadata and a reader that includes the first byte consumed for detection.
func ReadHeader(r io.Reader) (store.Metadata, io.Reader, error) {
	var metadata store.Metadata
	var header [9]byte
	if _, err := io.ReadFull(r, header[:1]); err != nil {
		return metadata, nil, headerReadError(err)
	}
	if header[0] != magic[0] {
		return metadata, io.MultiReader(bytes.NewReader(header[:1]), r), nil
	}
	if _, err := io.ReadFull(r, header[1:]); err != nil {
		return metadata, nil, fmt.Errorf("reading persistence header: %w", headerReadError(err))
	}
	if !bytes.Equal(header[:5], magic[:]) || header[5] != 1 {
		return metadata, nil, fmt.Errorf("%w: unrecognised header or version", store.ErrInvalidMetadata)
	}
	metadata.QoS = header[6]
	id := make([]byte, int(binary.BigEndian.Uint16(header[7:9])))
	if _, err := io.ReadFull(r, id); err != nil {
		return store.Metadata{}, nil, fmt.Errorf("reading persistence tracking ID: %w", headerReadError(err))
	}
	metadata.ID = string(id)
	return metadata, r, nil
}

func headerReadError(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.Join(store.ErrInvalidMetadata, err)
	}
	return err
}
