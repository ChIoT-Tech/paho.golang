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

package persistence

import (
	"bytes"
	"io"
	"testing"

	"github.com/eclipse/paho.golang/paho/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countedReader struct {
	io.Reader
	bytes int
}

func (r *countedReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += n
	return n, err
}

func TestMetadataReadDoesNotReadPacket(t *testing.T) {
	metadata := store.Metadata{ID: "application\x00\xff-id", QoS: 2}
	var record bytes.Buffer
	require.NoError(t, WriteHeader(&record, metadata))
	headerSize := record.Len()
	payload := bytes.Repeat([]byte{0x31}, 1024*1024)
	record.Write(payload)
	counted := &countedReader{Reader: &record}
	got, reader, err := ReadHeader(counted)
	require.NoError(t, err)
	assert.Equal(t, metadata, got)
	assert.Equal(t, headerSize, counted.bytes, "metadata read must not touch the packet")
	packet, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, payload, packet)
}

func TestLegacyAndInvalidMetadata(t *testing.T) {
	legacy := []byte{0x30, 3, 0, 1, 't'}
	metadata, reader, err := ReadHeader(bytes.NewReader(legacy))
	require.NoError(t, err)
	assert.Empty(t, metadata)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, legacy, got)
	for _, data := range [][]byte{
		{}, {0}, {0, 'P', 'A', 'H', 'O', 2, 1, 0, 0},
		{0, 'P', 'A', 'H', 'O', 1, 1, 0, 5, 'x'},
	} {
		_, _, err := ReadHeader(bytes.NewReader(data))
		require.ErrorIs(t, err, store.ErrInvalidMetadata)
	}
}
