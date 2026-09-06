/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
 */

package paho

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eclipse/paho.golang/internal/basictestserver"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/log"
	"github.com/eclipse/paho.golang/paho/session"
	"github.com/eclipse/paho.golang/paho/session/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type handlerDisconnectSession struct {
	session.SessionManager
	ackCount int
}

func (s *handlerDisconnectSession) Ack(p *packets.Publish) error {
	s.ackCount++
	return s.SessionManager.Ack(p)
}

// A handler-requested disconnect must release incoming even when it is blocked
// passing a QoS 1/2 PUBLISH to the full delivery queue. Discarded messages must
// neither reach application handlers nor be acknowledged.
func TestClientHandlerDisconnectDrainsPublishQueue(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		for _, manualAck := range []bool{false, true} {
			t.Run(fmt.Sprintf("QoS%d/manualAck=%t", qos, manualAck), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ts := basictestserver.New(log.NOOPLogger{})
					ts.SetResponse(packets.CONNACK, &packets.Connack{Properties: &packets.Properties{}})
					go ts.Run()
					defer ts.Stop()

					s := &handlerDisconnectSession{SessionManager: state.NewInMemory()}
					defer s.Close()
					handlerErr := &testHandlerDisconnector{
						packet: &Disconnect{ReasonCode: packets.DisconnectImplementationSpecificError},
						err:    errors.New("handler requested shutdown"),
					}
					entered := make(chan struct{})
					release := make(chan struct{})
					var releaseOnce sync.Once
					releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
					errorReceived := make(chan error, 1)
					handlerCalls, laterHandlerCalls := 0, 0
					c := NewClient(ClientConfig{
						Conn:                       ts.ClientConn(),
						Session:                    s,
						EnableManualAcknowledgment: manualAck,
						OnPublishReceived: []func(PublishReceived) (bool, error){
							func(PublishReceived) (bool, error) {
								handlerCalls++
								if handlerCalls == 1 {
									close(entered)
									<-release
									return false, handlerErr
								}
								return false, nil
							},
							func(PublishReceived) (bool, error) {
								laterHandlerCalls++
								return false, nil
							},
						},
						OnClientError: func(err error) {
							if errors.Is(err, handlerErr) {
								errorReceived <- err
							}
						},
					})
					_, err := c.Connect(context.Background(), &Connect{
						ClientID:   "handler-disconnect",
						Properties: &ConnectProperties{ReceiveMaximum: Uint16(1)},
					})
					require.NoError(t, err)
					defer func() {
						releaseHandler()
						c.cancelFunc()
						// Also release the blocked sender if the regression reappears,
						// so a failed assertion does not leave the test deadlocked.
						go func() {
							for range c.publishPackets {
							}
						}()
						<-c.Done()
					}()

					// QoS 0 does not consume the broker's Receive Maximum quota.
					// Hold the first callback, fill the queue, then send one QoS 1/2
					// packet so incoming blocks in Session.PacketReceived.
					for _, topic := range []string{"first", "queued"} {
						require.NoError(t, ts.SendPacket(&packets.Publish{
							Topic: topic, Payload: []byte("payload"), Properties: &packets.Properties{},
						}))
						if topic == "first" {
							<-entered
						}
					}
					require.NoError(t, ts.SendPacket(&packets.Publish{
						Topic: "blocked", QoS: qos, PacketID: 1,
						Payload: []byte("payload"), Properties: &packets.Properties{},
					}))
					synctest.Wait()
					require.Len(t, c.publishPackets, 1)
					releaseHandler()

					select {
					case <-c.Done():
					case <-time.After(time.Second):
						t.Fatal("handler-requested shutdown blocked on queued PUBLISH")
					}
					select {
					case err := <-errorReceived:
						assert.ErrorIs(t, err, handlerErr)
					case <-time.After(time.Second):
						t.Fatal("handler-requested shutdown did not report its error")
					}
					assert.Equal(t, 1, handlerCalls)
					assert.Zero(t, laterHandlerCalls)
					assert.Zero(t, s.ackCount)
					assert.Empty(t, ts.ReceivedPubacks())
					assert.Empty(t, ts.ReceivedPubrecs())
				})
			})
		}
	}
}
