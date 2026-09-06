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
	"fmt"

	"github.com/eclipse/paho.golang/packets"
)

// inboundTopicAliases is owned by incoming for the lifetime of one connection.
// Resolve each packet before queuing it so later alias reassignments cannot
// change the topics of packets still waiting for application callbacks.
type inboundTopicAliases map[uint16]string

func (a inboundTopicAliases) resolve(p *packets.Publish, maximum uint16) *topicAliasError {
	if p.Properties == nil || p.Properties.TopicAlias == nil {
		if p.Topic == "" {
			return &topicAliasError{packets.DisconnectProtocolError, "topic name must not be empty when topic alias is absent"}
		}
		return nil
	}
	alias := *p.Properties.TopicAlias
	if alias == 0 { // MQTT-3.3.2-8
		return &topicAliasError{packets.DisconnectTopicAliasInvalid, "topic alias must be greater than zero"}
	}
	if alias > maximum { // MQTT-3.3.2-11
		return &topicAliasError{packets.DisconnectTopicAliasInvalid, fmt.Sprintf("topic alias %d exceeds maximum %d", alias, maximum)}
	}
	if p.Topic != "" {
		a[alias] = p.Topic
		return nil
	}
	if topic, ok := a[alias]; ok {
		p.Topic = topic
		return nil
	}
	// MQTT 5.0 section 3.3.4: an unknown alias without a topic is a Protocol Error.
	return &topicAliasError{packets.DisconnectProtocolError, fmt.Sprintf("topic alias %d not found", alias)}
}

type topicAliasError struct {
	reasonCode byte
	message    string
}

func (e *topicAliasError) Error() string { return e.message }

func (e *topicAliasError) Disconnect() *Disconnect {
	return &Disconnect{
		ReasonCode: e.reasonCode,
		Properties: &DisconnectProperties{ReasonString: e.message},
	}
}
