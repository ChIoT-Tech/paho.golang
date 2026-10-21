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

package queue_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eclipse/paho.golang/autopaho/queue"
	filequeue "github.com/eclipse/paho.golang/autopaho/queue/file"
	"github.com/eclipse/paho.golang/autopaho/queue/memory"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failedReader struct{ err error }

func (r failedReader) Read([]byte) (int, error) { return 0, r.err }

func TestQueueMetadata(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			var q queue.Queue = memory.New()
			reopen := func() queue.Queue { return q }
			if backend == "file" {
				dir := t.TempDir()
				reopen = func() queue.Queue { f, err := filequeue.New(dir, "queue-", ".pkt"); require.NoError(t, err); return f }
				q = reopen()
			}
			metadata := store.Metadata{ID: "queued\x00\xff", QoS: 2}
			var packet bytes.Buffer
			_, err := (&packets.Publish{QoS: 2, Topic: "queue", Payload: []byte("body")}).WriteTo(&packet)
			require.NoError(t, err)
			require.NoError(t, q.Enqueue(bytes.NewReader(packet.Bytes()), metadata))
			q = reopen()
			entry, err := q.Peek()
			require.NoError(t, err)
			got, err := entry.Metadata()
			require.NoError(t, err)
			assert.Equal(t, metadata, got)
			reader, err := entry.Reader()
			require.NoError(t, err)
			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, packet.Bytes(), body)
			got, err = entry.Metadata()
			require.NoError(t, err)
			assert.Equal(t, metadata, got, "reading the packet must not consume its metadata")
			require.NoError(t, entry.Leave())
			entry, err = q.Peek()
			require.NoError(t, err)
			require.NoError(t, entry.Remove())
			_, err = q.Peek()
			require.ErrorIs(t, err, queue.ErrEmpty)
			readErr := errors.New("reader failed")
			require.ErrorIs(t, q.Enqueue(failedReader{readErr}, metadata), readErr)
			q = reopen()
			_, err = q.Peek()
			require.ErrorIs(t, err, queue.ErrEmpty, "partial metadata must not be visible as a queue entry")
		})
	}
}

func TestFileQueueReadsLegacyPacket(t *testing.T) {
	dir := t.TempDir()
	var packet bytes.Buffer
	_, err := (&packets.Publish{Topic: "legacy", Payload: []byte("body")}).WriteTo(&packet)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "queue-old.pkt"), packet.Bytes(), 0600))
	q, err := filequeue.New(dir, "queue-", ".pkt")
	require.NoError(t, err)
	entry, err := q.Peek()
	require.NoError(t, err)
	metadata, err := entry.Metadata()
	require.NoError(t, err)
	assert.Empty(t, metadata)
	reader, err := entry.Reader()
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, packet.Bytes(), body)
	require.NoError(t, entry.Remove())
}

func TestFileQueueIgnoresIncompleteRecordsWithoutExtension(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "queue-partial.tmp"), []byte{0}, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "queue-invalid.CORRUPT"), []byte{0}, 0600))
	q, err := filequeue.New(dir, "queue-", "")
	require.NoError(t, err)
	_, err = q.Peek()
	require.ErrorIs(t, err, queue.ErrEmpty)
}
