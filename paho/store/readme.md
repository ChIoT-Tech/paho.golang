<!--
Copyright (c) 2026 Contributors to the Eclipse Foundation.
This program and the accompanying materials are made available under the
Eclipse Public License v2.0 and Eclipse Distribution License v1.0:
https://www.eclipse.org/legal/epl-2.0/
http://www.eclipse.org/org/documents/edl-v10.php

AI Disclosure: This file was largely generated with OpenAI Codex (GPT-6).
AI-generated portions are made available under CC0-1.0.
SPDX-License-Identifier: (EPL-2.0 OR BSD-3-Clause) AND CC0-1.0
Assisted-by: OpenAI GPT-6
-->

store
=====

Contains implementations of `session.storer`. These are used to store session states; that is store packets,
packets indexed by a uint16, with the ability to retrieve in the order written.

The MQTT client will open two stores; one for PUBLISH packets (and related acknowledgements) originating from the server
(i.e. the packet identifier was assigned by the server) and the other for messages originating on the client
(these may well share the same basic backend).

as per section 4.1 in the spec; The Session State in the Client consists of:
> · QoS 1 and QoS 2 messages which have been sent to the Server, but have not been completely acknowledged.
> · QoS 2 messages which have been received from the Server, but have not been completely acknowledged.

This means that we only need to store PUBLISH (QOS1+) related outbound packets (if we have not responded, the packet
will be resent). So the only things needed are:
 `PUBLISH` - This is the only situation where the message body is needed (so we can resend it).
 `PUBREC` - response to inbound PUBLISH. The only packet originating on the server that will be stored.
 `PUBREL` - relating to a PUBLISH we initiated
We do need to store the entire packet because, when resending it, this should include any properties.

The vast majority of operations will be writes. The only time data should be read is:
   * At startup an inventory of packets held will be needed (need to know which identifiers are allocated)
   * When connection is established, we load the packets, so they can be retransmitted

Design goals:
  - Simplicity
  - Store agnostic (allow for memory, disk files, database, REDIS etc)
  - Minimise data transfers (only read when data is needed)
  - Allow for properties (we use `packets.ControlPacket` so that a whole packet is stored; when resending we
    need to include properties etc).

## Alternative delivery-tracking implementation: Issue-216a

This branch explores storing an application tracking ID separately from MQTT packet properties. It uses the
publication callbacks from [PR #361](https://github.com/eclipse-paho/paho.golang/pull/361), with additional local
metadata. Related to [#216](https://github.com/eclipse-paho/paho.golang/issues/216) and the queue-ID approach in
[#264](https://github.com/eclipse-paho/paho.golang/pull/264). This is an alternative for comparison, with intentional
interface changes; it is not a compatibility adapter for the property-based implementation.

### Public data and contracts

```go
// paho.Publish: local application field, excluded by Publish.Packet().
TrackingID string

// paho/store.Metadata: copied by value, never encoded as MQTT properties.
type Metadata struct {
    ID  string
    QoS byte // Original publication QoS, also retained with PUBREL.
}
```

An empty ID means untracked. Nonempty IDs are opaque strings of at most 65,535 bytes. Applications may reuse an
existing message ID or generate one with `delivery.Tracker.Track`. IDs need not be UUIDs or MQTT UTF-8 strings.
The library does not enforce uniqueness or deduplicate IDs. MQTT packet identifiers remain separate, reusable
uint16 protocol identifiers.

Metadata preserves the submitted QoS byte. Queues continue to accept locally invalid publications and let the
client's existing validation reject them when delivery is attempted; metadata storage does not add protocol validation.

The changed contracts are:

```go
// Session store
Put(packetID uint16, packetType byte, packet io.WriterTo, metadata store.Metadata) error
Get(packetID uint16) (io.ReadCloser, error) // MQTT packet bytes only
GetMetadata(packetID uint16) (store.Metadata, error)

// Queue
Enqueue(packet io.Reader, metadata store.Metadata) error
// Queue entry
Metadata() (store.Metadata, error)
Reader() (io.Reader, error) // MQTT packet bytes only

// SessionManager
AddToSession(ctx context.Context, packet session.Packet, metadata store.Metadata) (<-chan packets.ControlPacket, error)
```

`Put` replaces packet and metadata together. A failed write must leave the previous record intact. Every write
supplies metadata explicitly: a reused MQTT packet identifier must never inherit an earlier publication's ID.
The same atomic visibility requirement applies to `Enqueue`. Metadata values contain no borrowed packet references.
Inbound session records and non-publication requests use empty metadata. `State` obtains QoS directly from ordinary
`*packets.Publish` inputs; custom packet wrappers must supply the original QoS themselves.

`PublishEvent`, `PublishSend`, and `PubrelPreparation` carry metadata separately from their packet references.
Metadata remains usable after the callback returns, including after record deletion or packet-ID reuse.
`ReadStoredPacket` controls only the optional source packet; it does not suppress metadata.

### Publication lifecycle

1. `PublishViaQueue` stores the encoded PUBLISH and `{TrackingID, QoS}` in one queue entry. Direct publication starts
   at the next step. Setting an ID never modifies MQTT properties or payload bytes.
2. The queue worker reads metadata and the packet, restores `Publish.TrackingID`, and submits the publication.
3. `AddToSession` allocates an MQTT packet ID, stores packet and metadata together, then caches metadata in the
   active transaction. On failure, it releases the allocation without publishing metadata in the cache.
4. On successful PUBREC, the session writes PUBREL with the same metadata. It changes the transaction stage only
   after that write succeeds, before sending PUBREL. The tracker needs no preparation callback or PUBLISH read.
5. On PUBACK, rejection PUBREC, or PUBCOMP, the event receives cached metadata before the transaction is removed.
   User callbacks still run outside session/store locks. The callback does not retain a reader or keep an entry alive.
6. On restart with a resumable session, metadata is loaded before retransmission and cached again. The packet is
   still decoded for protocol retransmission. On restart without a resumable session, discarded publications are
   identified by metadata reads alone when callbacks use `ReadStoredNone`.

The MQTT acknowledgement rules are unchanged. In particular, PUBREL is persisted before transmission and a
successful PUBREC does not complete QoS 2. See MQTT 5.0 sections 4.3.3 and 4.4, including
[MQTT-4.3.3-6](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html): PUBLISH must not be resent after PUBREL
has been sent. Repeated PUBREC still loads the stored PUBREL so optional application-supplied properties are preserved.

### File format and recovery

The built-in file queue and store write one record per entry:

| Bytes | Meaning |
| --- | --- |
| 0–4 | `00 50 41 48 4f` (zero byte followed by `PAHO`) |
| 5 | Format version, currently 1 |
| 6 | Original publication QoS |
| 7–8 | ID length, unsigned big-endian uint16 |
| Next ID-length bytes | Opaque application ID |
| Remaining bytes | Unmodified encoded MQTT packet |

Metadata is bounded by the ID limit and can be read without reading any packet bytes. Records are written to a
temporary file, synced, closed, and renamed into place. Partial writes are not visible as committed entries;
temporary and quarantined files are excluded from enumeration, including when no filename extension is configured.
There is no second metadata file to reconcile with the packet file. The in-memory implementations store metadata
beside their byte slice and replace both under their mutex.

Existing valid raw MQTT packet files are detected by their nonzero first byte and remain readable as **untracked**
records. This does not recover tracking properties from the other experimental branch. New writes use the new
format even for untracked publications. Old binaries cannot read these new records: use separate directories when
comparing branches, and drain or explicitly convert records before downgrading. There is no automatic conversion
between this format and PR #361's property-based tracking format.

Unreadable metadata stops session resumption rather than silently sending a tracked message without its ID.
Session-loss enumeration reports metadata read errors through the event handler. A queued packet whose metadata
is readable can report its ID even if the packet body is corrupt and quarantined. If the metadata itself is corrupt,
only an unassociated tracking error can be reported.
Queue backends wrap `store.ErrInvalidMetadata` for malformed records; other metadata read errors leave the entry
queued for retry. A temporary storage outage must not be treated as a corrupt publication.

Rename gives packet/metadata atomic visibility within the existing single-process store model. It is not a
transaction between queue and session, or between disk and application callbacks. Directory entries are not fsynced;
this implementation does not establish additional guarantees for sudden power loss. The existing filesystem
modification-time ordering limitation also remains.

### What becomes simpler, and what remains

| Concern | Property-based PR #361 | This alternative |
| --- | --- | --- |
| Attach application ID | Add a reserved MQTT User Property | Set `Publish.TrackingID` |
| Ordinary completion notification | Read/decode stored packet in current Header implementation | Copy active transaction metadata; no store read |
| First successful PUBREC | Tracker reads PUBLISH and copies ID into PUBREL | Session passes cached metadata to the PUBREL write |
| Before transmission | Tracker filters the reserved property | No ID-related packet changes |
| Session lost after restart | Read stored packet/header to find ID | Read bounded metadata prefix |
| Session resumed after restart | Read packet for retransmission | Read metadata and packet; cold recovery currently uses two store operations |
| Custom queue/store implementations | Existing persistence interfaces | Revised methods and atomic packet/metadata contract |
| File migration | MQTT packet format remains usable | Versioned local records; older readers cannot consume new writes |

`delivery.Tracker` no longer needs property cloning/filtering, packet-type-dependent identity extraction, or
`PreparePubrel`. Its `BeforeSend` hook only reports QoS 0 attempts, and its `OnEvent` hook works with `Source == nil`.
The general hooks and None/Header/Full source options remain available for unrelated packet customisation.

There is still an explicit metadata copy when PUBLISH becomes PUBREL. The difference is that the session owns
that copy and does not need to parse the old packet or call application code to obtain the ID. This simplifies the
tracker and removes its packet-read cost; it adds format handling, metadata caching, and interface obligations to
persistence. It is not evidence that the overall implementation is smaller or universally preferable.

An active session keeps one ID string per in-flight publication; the packet payload is not retained by this cache.
No throughput or allocation benchmark is claimed. The packet-read savings are verified by instrumented tests.
Caching is also possible in the property-based design: a header cache there could avoid most of its normal reads.
The separate-metadata approach makes the cached identity explicit, but is not the only route to that optimisation.

### Remaining delivery limits

- Queue removal and session insertion are separate operations. A crash between them can leave both records,
  leading to another transmission and repeated notifications with the same ID. Metadata does not add a transactional handoff.
- Notifications are not a durable completion journal. Crashes can lose or repeat callbacks; applications still need
  durable pending records, idempotent completion handling, and a policy for unresolved outcomes.
- A lost session or discarded queue entry reports `Unknown`, because an earlier transmission may have succeeded.
  PUBCOMP `0x92` also remains unconfirmed; QoS 0 reports an attempt, not broker receipt.
- The general callback lifecycle, reconnect coordination, store-reset gates, and packet-ID ownership rules remain.
  No callback runs under the session mutex, and no lazy packet reader is kept alive for tracking.

### Compatibility and validation

This experiment deliberately changes `Queue.Enqueue`, `Entry`, session-store `Put`/`GetMetadata`, and
`SessionManager.AddToSession`; custom implementations must be updated. The publication-event setter from PR #361
is also required. Ordinary callers of `Publish`/`PublishWithOptions`/`PublishViaQueue` keep their existing method
signatures. Direct users of queues, stores, and sessions pass metadata explicitly. Silently discarding metadata in
an adapter would break restart tracking and is not supported.

Tests cover metadata-only reads, opaque IDs, legacy raw records, memory/file queue recovery, session recovery at
both PUBLISH and PUBREL stages, failure of a packet replacement, packet-ID reuse, session-loss identification
without reading packet bodies, wire data without tracking properties, and the existing acknowledgement outcomes.
Validation of this working implementation:

| Command | Result |
| --- | --- |
| `go test -race ./...` | No data races reported. All packages passed except the existing `autopaho/queue/file` `TestFileQueue` ordering failure. |
| `go vet ./...` | Existing unused-result, cancellation-path, and unreachable-code diagnostics; the diagnostic set matches the comparison snapshot without metadata changes. |
| `git diff --check` | Passed. Modified Go files are formatted with `gofmt`. |

The file-queue ordering failure was also reproduced in the unchanged comparison snapshot. `TestFileStore` showed an
intermittent modification-time ordering failure in both versions during validation, but passed the final race run.
The metadata tests cover the file implementations directly; these existing ordering limitations have not been fixed
as part of this alternative. Validation used the repository's Go toolchain and a writable cache under `/tmp`.
