# Native contract: Rust ↔ Kotlin BLE shim

Status: proposed for review by the Kotlin `:ble` owner (AND-04..09).

This document is normative. `NativeSink`, `NativeEvents` and the mapping tables
in `ao_network` (NET-14) must match it exactly; a change to either side changes
this file in the same PR. The FFI binding (UniFFI) lives in `aoheyra`, not here.

Kotlin is a thin radio shim. It moves raw bytes and raw status codes. All
framing, fragmentation, beacon encoding, timeouts, retries, degree cap,
duplicate resolution and status mapping live in Rust.

## 1. GATT layout

| Item | UUID | Properties | Permissions |
|---|---|---|---|
| Service (primary) | `4c5b0001-108f-4105-a600-5a82e0fd7470` | | |
| RX characteristic | `4c5b0002-108f-4105-a600-5a82e0fd7470` | `WRITE_NO_RESPONSE` | `WRITE` |
| TX characteristic | `4c5b0003-108f-4105-a600-5a82e0fd7470` | `NOTIFY` | none |
| TX CCCD | `00002902-0000-1000-8000-00805f9b34fb` | | `READ`, `WRITE` |

- RX and TX are named from the peripheral's side: the central writes RX, the
  peripheral notifies TX. Every link uses exactly these two characteristics.
- No pairing, bonding or encryption is requested; characteristics need none.
  Authentication happens above this layer. Kotlin never calls `createBond`.
- The UUIDs never change. Protocol evolution is negotiated in `Hello`
  (`proto_min`/`proto_max`) and the beacon version byte.
- One ATT value (one write or one notification) carries exactly one chunk.
  Its length is at most `mtu − 3`.

## 2. Advertising and scanning

Legacy (non-extended) advertising, connectable, no device name, no TX power.

| Packet | Content | Bytes |
|---|---|---|
| Advertising data | Flags (added by Android) + complete list of 128-bit service UUIDs: the service UUID | 3 + 18 = 21 |
| Scan response | Manufacturer specific data, company ID `0xFFFF`, payload = the 16-byte beacon from Rust | 4 + 16 = 20 |

- Kotlin never builds or parses beacon bytes. It advertises exactly the bytes
  given and forwards exactly the bytes received.
- Scanning uses a `ScanFilter` on the service UUID and active scanning, so the
  scan response is merged into the `ScanResult`.
- `0xFFFF` is the Bluetooth SIG value reserved for unregistered use. Replace it
  only if aoheyra registers a company identifier; that is a wire change.

## 3. Identifiers

- **`LinkId`** (`u64`) names one connection on this node. Rust allocates every
  id: for outbound links in `connect`, for inbound links as the return value of
  `on_accepted`. Kotlin never invents one and never reuses one after
  `on_closed`.
- **`PeerKey`** (16 bytes) names a radio peer: bytes 0..6 are the 6-byte BLE
  address, most significant byte first (`AA:BB:..` → `AA BB ..`); bytes 6..16
  are zero. It is a radio address, not an identity: it rotates, and the same
  phone can show different addresses as central and as peripheral. Kotlin does
  not deduplicate by it.
- **`mtu`** (`u16`) is the negotiated ATT MTU, 23..=517. Advisory link hints
  from the layer above are keyed by `PeerKey` and stay inside Rust.

## 4. Rust → Kotlin: `NativeSink`

Implemented by Kotlin. Called from the Rust runtime.

```rust
pub trait NativeSink: Send + Sync {
    fn set_advertising(&self, beacon: Option<[u8; 16]>, mode: AdvertiseMode);
    fn set_scanning(&self, mode: Option<ScanMode>);
    fn connect(&self, link: LinkId, peer: PeerKey);
    fn write(&self, link: LinkId, bytes: Vec<u8>);
    fn disconnect(&self, link: LinkId);
}

pub enum AdvertiseMode { LowPower, Balanced, LowLatency }
pub enum ScanMode { LowPower, Balanced, LowLatency }
```

| Call | Kotlin does |
|---|---|
| `set_advertising(Some(b), m)` | Start advertising (§2) with beacon `b` and mode `m`, or replace beacon/mode of the running advertisement. Latest call wins; Kotlin may coalesce updates still pending. |
| `set_advertising(None, _)` | Stop advertising. |
| `set_scanning(Some(m))` / `(None)` | Start or change / stop scanning (§2). Rust starts, stops or changes the mode of scanning, and changes the advertising mode, at most once per `discovery_interval` (default 10 s), so Kotlin needs no throttling of its own. |
| `connect(link, peer)` | Central setup (§6.1) to the `BluetoothDevice` last seen in a scan result for `peer`. Unknown peer → `on_closed(link, PeerUnknown)`. |
| `write(link, bytes)` | Send `bytes` as one ATT value: RX write without response if this side is central, TX notification if peripheral. |
| `disconnect(link)` | Tear down (§6.3), then `on_closed(link, Requested)`. Valid in any state, including while connecting. |

Rules:

1. **Every call returns immediately.** Kotlin posts the work to its BLE thread
   and returns. No BLE API call, lock wait, I/O or sleep inside a `NativeSink`
   method.
2. **No re-entrancy.** Kotlin never calls a `NativeEvents` method from inside a
   `NativeSink` call; results are posted and delivered later.
3. **One write in flight per link.** Rust calls `write` only when the link holds
   its single write credit (§5). Kotlin never queues more than one Rust write
   per link and never drops one: if it cannot be sent, the link is closed.
4. Calls for a `LinkId` that is already closed are ignored.
5. Kotlin does not retry, reconnect (`autoConnect = false`), time out, or
   enforce a connection limit. Rust decides all of these.

## 5. Kotlin → Rust: `NativeEvents`

Implemented by Rust (`NativeTransport`). Called by Kotlin on its BLE thread.

```rust
pub trait NativeEvents: Send + Sync {
    fn on_radio_state(&self, state: RadioState);
    fn on_discovery_failed(&self, kind: DiscoveryKind, code: i32);
    fn on_scan_result(&self, peer: PeerKey, rssi: i16, beacon: Vec<u8>);
    fn on_connected(&self, link: LinkId, mtu: u16);
    fn on_accepted(&self, peer: PeerKey, mtu: u16) -> LinkId;
    fn on_mtu_changed(&self, link: LinkId, mtu: u16);
    fn on_bytes(&self, link: LinkId, bytes: Vec<u8>);
    fn on_write_ready(&self, link: LinkId);
    fn on_closed(&self, link: LinkId, status: NativeStatus);
}

pub enum RadioState { Ready, Off, PermissionDenied }
pub enum DiscoveryKind { Advertise, Scan }
```

| Event | When |
|---|---|
| `on_radio_state` | At start and on every change of adapter state or BLE permissions. On `Off` / `PermissionDenied`, Kotlin first closes every link with `RadioOff` / `PermissionDenied`. |
| `on_discovery_failed` | `AdvertiseCallback.onStartFailure` / `ScanCallback.onScanFailed`; `code` is the raw Android error code. |
| `on_scan_result` | Every scan result matching the filter. `beacon` is the raw manufacturer payload for `0xFFFF`, empty if absent. |
| `on_connected` | Outbound link finished central setup (§6.1). |
| `on_accepted` | Inbound link finished peripheral setup (§6.2). Returns the `LinkId` to use from now on. |
| `on_mtu_changed` | MTU changed after `on_connected` / `on_accepted`. |
| `on_bytes` | One ATT value received: RX write (peripheral) or TX notification (central), unmodified. |
| `on_write_ready` | The stack accepted the previous `write` (§5.1). |
| `on_closed` | The link is gone and its `BluetoothGatt` is closed. |

Rust guarantees every `NativeEvents` method returns immediately: it takes a
short lock that is never held across a `NativeSink` call or any I/O, appends to
a bounded queue (for `on_accepted`, also allocates the id), and never calls back
into `NativeSink` from inside it. If the queue for link events is full
(`NativeConfig::max_pending_events`), Rust closes that link with
`TransportLost`; link events are never dropped silently. It does the same for
an `on_bytes` value longer than 514 bytes, which no ATT MTU allows. Scan results are
latest-wins per peer, held for at most `NativeConfig::max_pending_scans` peers,
and may be dropped under load; a `beacon` longer than 31 bytes is dropped.

Before the transport starts and after it stops, scan results are dropped and an
`on_accepted` link is disconnected without being reported. Rust makes those
disconnects, and any it decides on inside a callback, on its next
`NativeTransport` call or poll. Callbacks for a `LinkId` that is unknown or
already closed are ignored. After the transport is dropped, every callback is a
no-op and `on_accepted` returns `LinkId` 0, which Rust never allocates.

### 5.1 Write credit

Each link holds one write credit. It is granted by `on_connected` /
`on_accepted`, consumed by `write`, and returned by `on_write_ready`, which
Kotlin sends exactly once per successful `write`:

- central: `onCharacteristicWrite(…, RX, GATT_SUCCESS)`;
- peripheral: `onNotificationSent(device, GATT_SUCCESS)`.

`on_write_ready` means "accepted by the local stack", not "delivered". BLE's
link layer delivers in order while the connection lasts. A write that fails
(non-success callback status, or the API call is refused) closes the link with
`Gatt { stage: Write, .. }` or `Refused { stage: Write, .. }`.

### 5.2 Ordering

Per link, Kotlin delivers events in this order:

1. `on_connected` / `on_accepted` comes before any other event for the link.
2. `on_bytes` calls arrive in stack order, one call per ATT value, never split,
   merged, reordered or dropped.
3. `on_bytes`, `on_write_ready` and `on_mtu_changed` keep the relative order in
   which the stack callbacks arrived.
4. `on_closed` comes exactly once per `LinkId` (including failed connects and
   after `disconnect`) and is the last event for it.

Across links nothing is ordered. The simplest way to keep the per-link rules is
one `HandlerThread` for all GATT callbacks (pass its `Handler` to `connectGatt`
and `openGattServer`) forwarding to Rust in callback order.

## 6. Link procedures

Kotlin serialises GATT operations per `BluetoothGatt` (one outstanding
operation at a time); Rust writes queue behind setup operations.

### 6.1 Central (outbound, `LinkRole::Initiator`)

1. `connectGatt(ctx, autoConnect = false, cb, TRANSPORT_LE, …, handler)`.
   `null` → `on_closed(link, Refused { stage: Connect, code: -1 })`.
2. `onConnectionStateChange(CONNECTED, GATT_SUCCESS)` → `discoverServices()`.
3. Service or RX/TX missing → close with `ServiceMissing`.
4. `requestMtu(517)`. A failed MTU request is not fatal: continue with 23.
5. `setCharacteristicNotification(TX, true)` and write `0x0001` to TX's CCCD.
6. `onDescriptorWrite(GATT_SUCCESS)` → `on_connected(link, mtu)`.

Any non-success status in steps 2–6 closes the link with
`Gatt { stage, code }`.

### 6.2 Peripheral (inbound, `LinkRole::Responder`)

1. `openGattServer` once and add the service (§1) before advertising.
2. A central connects. Record its MTU from `onMtuChanged` (default 23).
3. When it writes `0x0001` to the TX CCCD, respond if a response is needed, then
   call `on_accepted(peer, mtu)` and keep the returned `LinkId`.
4. RX writes with `preparedWrite == false` and `offset == 0` go to `on_bytes`.
   Any other write to RX, or an RX write before step 3, means the peer is
   misbehaving: respond `GATT_REQUEST_NOT_SUPPORTED` if a response is needed and
   `cancelConnection` (with `on_closed(link, PeerMisbehaved)` if a link exists).
5. A central writing `0x0000` to the CCCD after step 3 closes the link with
   `PeerMisbehaved`.

Kotlin accepts every inbound central; Rust disconnects any it does not want.

### 6.3 Teardown

Central: `disconnect()`, wait for `onConnectionStateChange(DISCONNECTED)` (or
the connect attempt to end), then `close()`. Peripheral: `cancelConnection`
and wait for the disconnect callback. Then `on_closed`. Kotlin does not call
the hidden `BluetoothGatt.refresh()`.

## 7. `NativeStatus`

Why Kotlin closed a link. Kotlin passes raw values; it does not interpret them.

```rust
pub enum NativeStatus {
    /// Rust called `disconnect`.
    Requested,
    /// Non-success status from an async GATT callback (BluetoothGatt.GATT_* / HCI).
    Gatt { stage: Stage, code: i32 },
    /// A GATT API call was refused synchronously (BluetoothStatusCodes; -1 for false/null).
    Refused { stage: Stage, code: i32 },
    /// `connect` named a peer with no known `BluetoothDevice`.
    PeerUnknown,
    /// Service, RX or TX not found after discovery.
    ServiceMissing,
    /// The peer broke §6.2.
    PeerMisbehaved,
    /// Adapter turned off or BLE permission revoked.
    RadioOff,
    PermissionDenied,
}

pub enum Stage { Connect, Discover, Subscribe, Write }
```

A link that is connected and then drops with a status from
`onConnectionStateChange` uses `Gatt { stage: Connect, code }`.

## 8. Status mapping (Rust)

Rust applies these tables. Before `on_connected` / `on_accepted` the result is
a `ConnectError`; afterwards a `DisconnectReason`. The raw `NativeStatus` is not
kept.

### 8.1 Before the link is up → `ConnectError`

```rust
#[non_exhaustive]
pub enum ConnectError {
    Timeout, Cancelled, Rejected, Unreachable,
    SetupFailed, ServiceMissing, PeerUnknown, RadioUnavailable,
}
```

| `NativeStatus` | `ConnectError` |
|---|---|
| `Requested` after Rust called `disconnect` | `Cancelled` (any status; Rust has no connect timeout of its own yet) |
| `Requested` without a `disconnect` | `Unreachable` |
| `Gatt { Connect, 0x08 }` (connection timeout), `Gatt { Connect, 0x22 }` (LMP response timeout) | `Timeout` |
| `Gatt { Connect, 0x09 }` (connection limit), `0x0D` (limited resources), `0x13` (peer terminated), `0x14` (peer low resources), `0x15` (peer power off) | `Rejected` |
| `Gatt { Connect, 0x3E }` (failed to establish), `0x85` (`GATT_ERROR`), any other code | `Unreachable` |
| `Gatt { Discover \| Subscribe, _ }`, `Refused { _, _ }` | `SetupFailed` |
| `ServiceMissing` | `ServiceMissing` |
| `PeerUnknown` | `PeerUnknown` |
| `RadioOff`, `PermissionDenied` | `RadioUnavailable` |
| `PeerMisbehaved` | `Unreachable` (not possible: no inbound link exists yet) |

### 8.2 After the link is up → `DisconnectReason`

| `NativeStatus` | `DisconnectReason` |
|---|---|
| any status after Rust called `disconnect` or `stop` | `Closed` |
| any status after Rust closed an overflowing link, or one given an oversize value | `TransportLost` |
| `PeerMisbehaved` | `ProtocolViolation` |
| everything else | `TransportLost` |

A clean close by the peer arrives as a `Bye` frame before the radio drops, so
`DisconnectReason::Bye` never comes from a status code.

## 9. Mapping to `Transport` (Rust)

`NativeTransport` implements the `Transport` trait over this contract (feature
`native`). `NativeTransport::new(sink, NativeConfig)` returns the transport and
the `NativeHandle` that implements `NativeEvents`; aoheyra hands the handle to
Kotlin. A `write` without the link's write credit is not passed on.

| `Transport` | `NativeSink` |
|---|---|
| `set_advertising(Some(AdvProfile { beacon, mode }))` / `None` | `set_advertising(Some(beacon), mode)` / `None` |
| `set_scanning(Some(ScanProfile { mode }))` / `None` | `set_scanning(Some(mode))` / `None` |
| `connect(peer)` | allocates a `LinkId`, then `connect(link, peer)` |
| `write` / `disconnect` | `write` / `disconnect` |

`PowerMode` maps one-to-one onto `AdvertiseMode` and `ScanMode`.

| `NativeEvents` | `TransportEvent` |
|---|---|
| `on_radio_state` | `RadioState` |
| `on_scan_result` | `Discovered` |
| `on_connected` | `Connected { role: Initiator }` |
| `on_accepted` | `Connected { role: Responder }` with a newly allocated `LinkId` |
| `on_mtu_changed`, `on_bytes`, `on_write_ready` | `MtuChanged`, `Received`, `WriteReady` |
| `on_closed` before / after the link was up | `ConnectFailed` / `Disconnected`, per §8 (`Requested` gives `Cancelled` / `Closed`) |

`on_discovery_failed` is ignored; discovery restarts on the next profile change.
The native shim reports neither `Lost` nor `Rssi`.
