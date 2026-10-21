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

package store_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/store"
	filestore "github.com/eclipse/paho.golang/paho/store/file"
	"github.com/eclipse/paho.golang/paho/store/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordStore interface {
	Put(uint16, byte, io.WriterTo, store.Metadata) error
	Get(uint16) (io.ReadCloser, error)
	GetMetadata(uint16) (store.Metadata, error)
	Delete(uint16) error
	Quarantine(uint16) error
	Reset() error
}

type failedPacket struct{ err error }

func (p failedPacket) WriteTo(w io.Writer) (int64, error) {
	n, _ := w.Write([]byte("partial"))
	return int64(n), p.err
}

func TestMetadataSurvivesReplacementAndFailedWrites(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			var s recordStore = memory.New()
			var reopen func() recordStore = func() recordStore { return s }
			if backend == "file" {
				dir := t.TempDir()
				reopen = func() recordStore { f, err := filestore.New(dir, "client-", ".pkt"); require.NoError(t, err); return f }
				s = reopen()
			}
			original := store.Metadata{ID: "original", QoS: 2}
			pub := &packets.Publish{PacketID: 7, QoS: 2, Topic: "t", Payload: []byte("payload")}
			require.NoError(t, s.Put(7, packets.PUBLISH, pub, original))
			s = reopen()
			got, err := s.GetMetadata(7)
			require.NoError(t, err)
			assert.Equal(t, original, got)
			require.NoError(t, s.Put(7, packets.PUBREL, &packets.Pubrel{PacketID: 7}, original))
			diskFull := errors.New("disk full")
			require.ErrorIs(t, s.Put(7, packets.PUBLISH, failedPacket{diskFull}, store.Metadata{ID: "failed", QoS: 1}), diskFull)
			s = reopen()
			got, err = s.GetMetadata(7)
			require.NoError(t, err)
			assert.Equal(t, original, got, "failed replacement must retain old metadata")
			reader, err := s.Get(7)
			require.NoError(t, err)
			packet, err := packets.ReadPacket(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			assert.Equal(t, byte(packets.PUBREL), packet.Type, "failed replacement must retain old packet")
			require.NoError(t, s.Put(7, packets.PUBLISH, pub, store.Metadata{}))
			got, err = s.GetMetadata(7)
			require.NoError(t, err)
			assert.Empty(t, got, "a reused identifier must not inherit metadata")
			require.NoError(t, s.Delete(7))
			_, err = s.GetMetadata(7)
			require.Error(t, err)
			require.NoError(t, s.Put(7, packets.PUBLISH, pub, original))
			require.NoError(t, s.Quarantine(7))
			_, err = s.GetMetadata(7)
			require.Error(t, err)
			require.NoError(t, s.Put(7, packets.PUBLISH, pub, original))
			require.NoError(t, s.Reset())
			_, err = s.GetMetadata(7)
			require.Error(t, err)
		})
	}
}

func TestFileStoreReadsLegacyPacket(t *testing.T) {
	dir := t.TempDir()
	s, err := filestore.New(dir, "client-", ".pkt")
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = (&packets.Publish{PacketID: 19, QoS: 1, Topic: "legacy", Payload: []byte("body")}).WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "client-19.pkt"), raw.Bytes(), 0600))
	metadata, err := s.GetMetadata(19)
	require.NoError(t, err)
	assert.Empty(t, metadata)
	reader, err := s.Get(19)
	require.NoError(t, err)
	packet, err := packets.ReadPacket(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "body", string(packet.Content.(*packets.Publish).Payload))
}

func TestFileStoreIgnoresIncompleteRecordsWithoutExtension(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "client-7-partial.tmp"), []byte{0}, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "client-8-invalid.CORRUPT"), []byte{0}, 0600))
	s, err := filestore.New(dir, "client-", "")
	require.NoError(t, err)
	ids, err := s.List()
	require.NoError(t, err)
	assert.Empty(t, ids)
}
