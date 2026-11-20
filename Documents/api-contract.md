# RoomScanner API Contract (Provider Portal)

| Version | Date | PP owner | CP owner | Status |
|---|---|---|---|---|
| v0.2 | 2026-11-20 | TBD | TBD | Ultimo draft, pending CP review |

This contract defines the Kafka exchanges between Provider Portal (PP) and Customer Portal (CP) for Ultimo. It addresses Common Requirements §4 (TW-S06, TW-M01, TW-U04) and §7.3 (INT-U01–U03). Sections tagged **CP decision** state proposed rules for joint review. Topic names, message types, and payload shapes are proposed until both teams approve v0.2.

## 1. Kafka conventions

All messages are JSON objects in UTF-8. Dates use `YYYY-MM-DD` and describe the property's local calendar where applicable. Timestamps use UTC ISO 8601. IDs are opaque strings. Fields are required unless marked optional (`?`). A nullable field is present with `null`. `Money` contains `amount: integer` in minor currency units and `currency: string` as a three-letter currency code.

| Envelope field | Type | Rule |
|---|---|---|
| `messageId` | string | Identifies one published message. Each intentional resend uses a new ID. Broker redelivery retains the original ID. |
| `correlationId` | string | Identifies a logical request and stays constant across resends. A reply copies it. Omit on `booking.outcome.changed`, which uses `orderId` and `bookingId` in its payload. |
| `type` | string | Exact operation identifier in the index. |
| `timestamp` | timestamp | Time this message was published. |
| `attempt` | integer | Starts at 1. An intentional resend increments it. Replies echo the request attempt. Omit on outcome and notification events. |
| `payload` | object | Operation data for requests and events. A reply contains either `{data: object}` or `{error: Error}`. |

`Error` contains `code: string`, `message: string`, and `retryable: boolean`. An unknown message type returns `UNSUPPORTED_MESSAGE_TYPE` when a reply can be routed. Use `VALIDATION_ERROR` for invalid input and `FORBIDDEN` for access outside the operation's scope. These are final errors for the submitted request.

**CP decision D1.** The proposed policy is an initial send plus up to three resends, with a five-second response timeout per attempt. Resend on timeout or a retryable error such as `TEMPORARILY_UNAVAILABLE`. Keep the same logical correlation ID and business identifiers. Accept a definitive reply for any attempt of the outstanding request and ignore later duplicates. Once the retry budget is exhausted, a write has an unknown outcome until reconciled. Transport timeouts never authorize a second capture, a new order ID for the same purchase, or a refund based solely on lack of a reply. Event notifications and booking outcomes have separate delivery rules below.

**CP decision D2.** Give each portal a distinct broker identity. Grant publish and consume permissions according to the directions in the index. CP validates the customer session and acts only for that customer. PP verifies booking ownership and binds the quote, the capture, and the customer to the same order. PP validates provider ownership for receptionist actions. Agree broker authentication, encrypted transport, topic provisioning, and which customer data PP needs. Examples use fictional data.

### Shared values

| Type | Values |
|---|---|
| `ProviderCategory` | `RESORT`, `HOTEL`, `MOTEL_HOSTEL`, `HOMESTAY` |
| `RoomType` | `BUNK_BED`, `SINGLE_TWIN_BED`, `DOUBLE_BED` |
| `Amenity` | `FREE_WIFI`, `PRIVATE_BATHROOM`, `AIR_CONDITIONING`, `DISABLED_FACILITIES`, `KITCHEN`, `WASHING_MACHINE`, `PETS_ALLOWED` |
| `Membership` | `STANDARD`, `PREMIUM` |
| `BookingStatus` | `PENDING`, `APPROVED`, `CANCELLED_BY_CUSTOMER`, `CANCELLED_BY_PROVIDER`, `COMPLETED` |
| `PaymentStatus` | `CAPTURED_HELD`, `RELEASE_PENDING`, `RELEASED`, `RELEASE_FAILED`, `REFUND_PENDING`, `REFUNDED`, `REFUND_FAILED` |

`Payment` contains `status: PaymentStatus`, `captureReference: string`, `releaseReference: string|null`, `refundReference: string|null`, and `failureCode?: string`. A pending operation may have a null reference while the gateway outcome is being established. Keep gateway credentials and payment-method data outside Kafka.

`PageInput` contains `cursor?: string` and `limit?: integer`, with default 10 and maximum 50. `nextCursor: string|null` is null on the last page. Cursors are opaque and scoped to the same filters and ordering.

## 2. Operation index

The arrow denotes the sender and receiver of the first message. Replies travel in the reverse direction. The event trigger has a prompt acknowledgement and a separate, potentially delayed notification.

| ID | Operation | Direction | Request or event topic and type | Reply topic and type |
|---|---|---|---|---|
| PP-01 | Search providers | CP → PP | `rs.pp.search.request` · `provider.search.request` | `rs.pp.search.response` · `provider.search.response` |
| PP-02 | List a provider's available rooms | CP → PP | `rs.pp.rooms.request` · `room.list.request` | `rs.pp.rooms.response` · `room.list.response` |
| PP-03 | Get room details | CP → PP | `rs.pp.rooms.request` · `room.get.request` | `rs.pp.rooms.response` · `room.get.response` |
| PP-04 | Create booking | CP → PP | `rs.pp.booking.request` · `booking.create.request` | `rs.pp.booking.response` · `booking.create.response` |
| PP-05 | Cancel booking | CP → PP | `rs.pp.booking.request` · `booking.cancel.request` | `rs.pp.booking.response` · `booking.cancel.response` |
| PP-06 | List or look up a customer's bookings | CP → PP | `rs.pp.booking.request` · `booking.list.request` | `rs.pp.booking.response` · `booking.list.response` |
| PP-07a | Register an event favorite for suggestions | CP → PP | `rs.cp.event.favorite.request` · `event.favorite.request` | `rs.cp.event.favorite.response` · `event.favorite.response` |
| PP-07b | Publish event suggestions | PP → CP | `rs.cp.event.notification` · `event.suggestions.published` | — |
| PP-08 | Quote a booking before capture | CP → PP | `rs.pp.booking.request` · `booking.quote.request` | `rs.pp.booking.response` · `booking.quote.response` |
| CP-01 | Publish booking or payment changes | PP → CP | `rs.pp.booking.outcome` · `booking.outcome.changed` | — |
| CP-02 | Get provider rating statistics | PP → CP | `rs.cp.rating.request` · `rating.stats.request` | `rs.cp.rating.response` · `rating.stats.response` |
| CP-03 | List provider reviews | PP → CP | `rs.cp.rating.request` · `review.list.request` | `rs.cp.rating.response` · `review.list.response` |
| CP-04 | Get customer membership | PP → CP | `rs.cp.customer.request` · `membership.get.request` | `rs.cp.customer.response` · `membership.get.response` |

## 3. Catalogue and search

### PP-01 · Search providers

Covers `CP-SEARCH-S01/S02/M01/M02` and `PP-SEARCH-S01/S02/M01/M02/U01/U02`.

`SearchCriteria` contains `country: string` (two-letter country code), `city: string` (agreed city identifier), `checkIn: date`, `checkOut: date`, `q?: string`, `roomTypes?: RoomType[]`, `categories?: ProviderCategory[]`, and `amenities?: Amenity[]`. Require `checkOut > checkIn`. PP uses the location to route to relevant shards and limits results to rooms available for the stay. Request payload is `SearchCriteria & PageInput`.

`ProviderSummary` contains `providerId: string`, `name: string`, `logoUrl: string|null`, `city: string`, `country: string`, `category: ProviderCategory`, `roomTypes: RoomType[]`, `amenities: Amenity[]`, `availableQuantity: integer`, `minPricePerNight: Money`, `overallRating: number|null`, and `ratingCount: integer`. CP owns rating statistics. Response data contains `items: ProviderSummary[]` and `nextCursor: string|null`.

**CP decision D3.** Proposed matching keeps location and availability mandatory. Within a supplied filter group, any selected value can match. Across supplied groups and text criteria, require every supplied group to match the same eligible listing and its provider. The SRS describes OR semantics and gives an example that combines criteria. Confirm which groups should combine by OR, the searchable fields, and these expected results before implementation. For the example `Single Bed Free WiFi Homestay`, the proposed expected set is Homestay providers with an available Single/Twin room offering Free Wi-Fi. A Homestay offering only Double Bed fails that example. Agree text parsing, the seven-value amenity vocabulary, city identifiers, availability aggregation, and deterministic pagination ordering.

```json
{
  "messageId": "m-01",
  "correlationId": "search-1",
  "type": "provider.search.request",
  "timestamp": "2026-11-20T04:00:00Z",
  "attempt": 1,
  "payload": {
    "country": "VN",
    "city": "hcmc",
    "checkIn": "2027-01-10",
    "checkOut": "2027-01-12",
    "q": "Example",
    "roomTypes": [
      "SINGLE_TWIN_BED"
    ],
    "amenities": [
      "FREE_WIFI"
    ],
    "limit": 10
  }
}
```

```json
{
  "messageId": "m-02",
  "correlationId": "search-1",
  "type": "provider.search.response",
  "timestamp": "2026-11-20T04:00:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "items": [
        {
          "providerId": "p-1",
          "name": "Example Hotel",
          "logoUrl": "https://example.com/p-1.webp",
          "city": "hcmc",
          "country": "VN",
          "category": "HOTEL",
          "roomTypes": [
            "SINGLE_TWIN_BED"
          ],
          "amenities": [
            "FREE_WIFI"
          ],
          "availableQuantity": 5,
          "minPricePerNight": {
            "amount": 8500,
            "currency": "USD"
          },
          "overallRating": 4.6,
          "ratingCount": 10
        }
      ],
      "nextCursor": null
    }
  }
}
```

### PP-02 · List a provider's available rooms

Covers `CP-SEARCH-S03` and `PP-SEARCH-S03`. Request contains `providerId: string` and `criteria: SearchCriteria`. CP passes the selected search criteria without the cursor. PP applies the same matching rules so that branches outside the selected city and unrelated rooms do not enter the result. Error `PROVIDER_NOT_FOUND` is final.

`RoomListing` contains `roomId: string`, `branchId: string`, `name: string`, `roomType: RoomType`, `availableQuantity: integer`, `pricePerNight: Money`, `images: string[]`, and `amenities: Amenity[]`. The price is the base nightly rate. The customer-specific payable price comes from PP-08. Response data contains `provider: ProviderSummary` and `rooms: RoomListing[]`.

```json
{
  "messageId": "m-03",
  "correlationId": "rooms-1",
  "type": "room.list.request",
  "timestamp": "2026-11-20T04:01:00Z",
  "attempt": 1,
  "payload": {
    "providerId": "p-1",
    "criteria": {
      "country": "VN",
      "city": "hcmc",
      "checkIn": "2027-01-10",
      "checkOut": "2027-01-12",
      "q": "Example",
      "roomTypes": [
        "SINGLE_TWIN_BED"
      ],
      "amenities": [
        "FREE_WIFI"
      ]
    }
  }
}
```

```json
{
  "messageId": "m-04",
  "correlationId": "rooms-1",
  "type": "room.list.response",
  "timestamp": "2026-11-20T04:01:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "provider": {
        "providerId": "p-1",
        "name": "Example Hotel",
        "logoUrl": "https://example.com/p-1.webp",
        "city": "hcmc",
        "country": "VN",
        "category": "HOTEL",
        "roomTypes": [
          "SINGLE_TWIN_BED"
        ],
        "amenities": [
          "FREE_WIFI"
        ],
        "availableQuantity": 5,
        "minPricePerNight": {
          "amount": 8500,
          "currency": "USD"
        },
        "overallRating": 4.6,
        "ratingCount": 10
      },
      "rooms": [
        {
          "roomId": "r-3",
          "branchId": "br-1",
          "name": "Twin Room",
          "roomType": "SINGLE_TWIN_BED",
          "availableQuantity": 5,
          "pricePerNight": {
            "amount": 8500,
            "currency": "USD"
          },
          "images": [
            "https://example.com/r-3.webp"
          ],
          "amenities": [
            "FREE_WIFI"
          ]
        }
      ]
    }
  }
}
```

### PP-03 · Get room details

Covers `CP-SEARCH-S03` and `PP-SEARCH-S02/S03`. Request contains `roomId: string`, `checkIn: date`, and `checkOut: date`. Response data is `RoomListing` with `description: string`, `capacity: integer`, and `branchAddress: string`. Missing or archived rooms return final `ROOM_NOT_FOUND`. A live listing with no inventory for the dates returns `availableQuantity: 0`.

```json
{
  "messageId": "m-05",
  "correlationId": "room-1",
  "type": "room.get.request",
  "timestamp": "2026-11-20T04:02:00Z",
  "attempt": 1,
  "payload": {
    "roomId": "r-3",
    "checkIn": "2027-01-10",
    "checkOut": "2027-01-12"
  }
}
```

```json
{
  "messageId": "m-06",
  "correlationId": "room-1",
  "type": "room.get.response",
  "timestamp": "2026-11-20T04:02:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "roomId": "r-3",
      "branchId": "br-1",
      "name": "Twin Room",
      "roomType": "SINGLE_TWIN_BED",
      "availableQuantity": 5,
      "pricePerNight": {
        "amount": 8500,
        "currency": "USD"
      },
      "images": [
        "https://example.com/r-3.webp"
      ],
      "amenities": [
        "FREE_WIFI"
      ],
      "description": "Twin beds with Wi-Fi",
      "capacity": 2,
      "branchAddress": "Example Street, Ho Chi Minh City"
    }
  }
}
```

## 4. Booking and escrow

Covers `PP-BOOK-S01/S02/U01`, `PP-MONEY-U01` and `CP-MONEY-U01–U04`. CP authorizes and captures the agreed amount in the sandbox before PP accepts the booking for receptionist review. Captured funds are held until release or return. Approval releases those funds without a second charge. PP owns the reservation, approval and cancellation decisions, and the reported beneficiary-side payment outcome.

**CP decision D4.** Confirm sandbox capabilities and credential ownership for capture, held funds, release, and refund, including returns after funds have been released. The Provider SRS describes escrow both as an Ultimo replacement and as optional in one passage. This draft uses escrow because the Customer SRS explicitly requires it. Validate the selected sandbox before treating this flow as agreed.

| Step | Contract behavior |
|---|---|
| Quote and capture | CP obtains PP-08, then authorizes and captures the quoted amount. Failed or incomplete capture never opens a booking for review. |
| Reserve | CP submits PP-04. PP verifies the capture and atomically records the pending booking, allocates inventory for the dates, and stores the order decision. Pending bookings have no automatic expiry. |
| Approve | The receptionist approves the provider's own pending booking. PP moves it to `APPROVED` with `RELEASE_PENDING` and initiates one release. PP reports `RELEASED` or `RELEASE_FAILED`. Proposed confirmation email trigger is the first outcome showing approval or completion with `RELEASED`. |
| Cancel | An authorized customer uses PP-05 or the receptionist cancels in PP. PP releases inventory once and reports cancellation with `REFUND_PENDING`, then `REFUNDED` or `REFUND_FAILED`. If release is in flight, PP reconciles it before executing the return. If release already succeeded, the return follows the agreed sandbox recovery path. |
| Reject after capture | For a definitive PP-04 rejection with no accepted booking, CP verifies that the capture belongs to the rejected order and is unbound to any accepted booking. CP owns returning that capture and reporting the result to its customer. PP owns refunds for accepted bookings. |
| Timeout after capture | CP keeps the same order and capture identifiers and resends the same command. CP may use PP-06 with `orderId` to reconcile. An empty lookup alone does not establish rejection. CP uses the recorded order decision or keeps the order pending reconciliation. |

### Booking retry and concurrency rules

**CP decision D5.** CP generates `orderId` for one purchase and `cancellationId` for one cancellation command. PP scopes these IDs to the CP partner and binds them to immutable request data. A matching replay returns the stored command result. A different payload under the same ID returns final `IDEMPOTENCY_CONFLICT`. A capture reference already bound to another order returns final `CAPTURE_ALREADY_USED`. That error requires reconciliation with the existing order before any refund. Before evaluating current quote expiry or stock on a replay, PP checks the stored decision. A retry of a successful booking can therefore return the original acceptance after the quote expires. PP-06 and CP-01 expose subsequent state.

All PP booking instances use the same authoritative inventory and order decisions. For create, reserve inventory and record the accepted order as one atomic change. Persist definitive rejections for valid identified commands so the same order cannot later succeed after CP starts a refund. A conflicting replay leaves the original order decision intact. Serialize approval and cancellation for a given booking. Use a stable payment operation ID for each release or refund and reconcile its outcome before retrying an uncertain gateway call. Retain order decisions through the agreed reconciliation period. Confirm the retry deadline, decision retention, and ownership of unresolved outcomes.

### PP-08 · Quote a booking before capture

Covers `PP-DISCOUNT-S01/S02` and supports `PP-MONEY-U01`. Request contains `customerId: string`, `roomId: string`, `checkIn: date`, `checkOut: date`, `quantity: integer`, `adults: integer`, and `children: integer`. Require a positive quantity, non-negative guest counts, and `quantity <= adults + children`. PP checks membership through CP-04 and verifies availability. Response data contains `quoteId: string`, `expiresAt: timestamp`, `membership: Membership`, `discountPolicyId: string|null`, and `totalAmount: Money`.

**CP decision D6.** The proposed quote locks pricing until expiry and does not reserve inventory. PP applies the assigned percentage discount to the stay subtotal for customers with active Premium at quote time. CP captures the quoted amount. PP verifies that an initial acceptance uses an unexpired quote bound to the submitted customer, room, dates, quantity, and guest counts. An accepted booking retains the quoted amount if prices or discount policies later change. Confirm quote lifetime, rounding, membership cutoff, and the effect of policy cancellation on open quotes. A definitive `QUOTE_EXPIRED` or `ROOM_NOT_AVAILABLE` after capture follows the recovery rule above.

```json
{
  "messageId": "m-07",
  "correlationId": "quote-1",
  "type": "booking.quote.request",
  "timestamp": "2026-11-20T04:03:00Z",
  "attempt": 1,
  "payload": {
    "customerId": "c-9",
    "roomId": "r-3",
    "checkIn": "2027-01-10",
    "checkOut": "2027-01-12",
    "quantity": 1,
    "adults": 2,
    "children": 0
  }
}
```

```json
{
  "messageId": "m-08",
  "correlationId": "quote-1",
  "type": "booking.quote.response",
  "timestamp": "2026-11-20T04:03:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "quoteId": "q-1",
      "expiresAt": "2026-11-20T04:13:01Z",
      "membership": "PREMIUM",
      "discountPolicyId": "d-1",
      "totalAmount": {
        "amount": 15300,
        "currency": "USD"
      }
    }
  }
}
```

### PP-04 · Create booking

Request contains `orderId: string`, `quoteId: string`, `customerId: string`, `customerName: string`, `customerEmail: string`, `membership: Membership`, `roomId: string`, `checkIn: date`, `checkOut: date`, `quantity: integer`, `adults: integer`, `children: integer`, `totalAmount: Money`, and `captureReference: string`. PP validates the supplied membership and amount against the quote. Through the sandbox integration, PP verifies the capture's held status, amount, currency, and binding to the submitted order. Final errors include `QUANTITY_EXCEEDS_GUESTS`, `ROOM_NOT_AVAILABLE`, `QUOTE_EXPIRED`, and `PAYMENT_MISMATCH`. If verification is temporarily unavailable, keep the result retryable until PP establishes the capture status.

Response data contains `orderId: string`, `bookingId: string`, `providerId: string`, `version: integer`, `status: BookingStatus`, `checkIn: date`, `checkOut: date`, and `payment: Payment`. The initial accepted result has version 1 and uses `PENDING` and `CAPTURED_HELD`.

```json
{
  "messageId": "m-09",
  "correlationId": "book-1",
  "type": "booking.create.request",
  "timestamp": "2026-11-20T04:04:00Z",
  "attempt": 1,
  "payload": {
    "orderId": "o-1",
    "quoteId": "q-1",
    "customerId": "c-9",
    "customerName": "Example Customer",
    "customerEmail": "customer@example.com",
    "membership": "PREMIUM",
    "roomId": "r-3",
    "checkIn": "2027-01-10",
    "checkOut": "2027-01-12",
    "quantity": 1,
    "adults": 2,
    "children": 0,
    "totalAmount": {
      "amount": 15300,
      "currency": "USD"
    },
    "captureReference": "cap-1"
  }
}
```

After a timeout, CP resends the same payload and correlation ID. This example retrieves the existing acceptance from another PP booking instance.

```json
{
  "messageId": "m-10",
  "correlationId": "book-1",
  "type": "booking.create.request",
  "timestamp": "2026-11-20T04:04:05Z",
  "attempt": 2,
  "payload": {
    "orderId": "o-1",
    "quoteId": "q-1",
    "customerId": "c-9",
    "customerName": "Example Customer",
    "customerEmail": "customer@example.com",
    "membership": "PREMIUM",
    "roomId": "r-3",
    "checkIn": "2027-01-10",
    "checkOut": "2027-01-12",
    "quantity": 1,
    "adults": 2,
    "children": 0,
    "totalAmount": {
      "amount": 15300,
      "currency": "USD"
    },
    "captureReference": "cap-1"
  }
}
```

```json
{
  "messageId": "m-11",
  "correlationId": "book-1",
  "type": "booking.create.response",
  "timestamp": "2026-11-20T04:04:06Z",
  "attempt": 2,
  "payload": {
    "data": {
      "orderId": "o-1",
      "bookingId": "b-1",
      "providerId": "p-1",
      "version": 1,
      "status": "PENDING",
      "checkIn": "2027-01-10",
      "checkOut": "2027-01-12",
      "payment": {
        "status": "CAPTURED_HELD",
        "captureReference": "cap-1",
        "releaseReference": null,
        "refundReference": null
      }
    }
  }
}
```

Alternative outcome for the original request when inventory is unavailable at the time of PP's decision. This example replaces the successful branch above.

```json
{
  "messageId": "m-12",
  "correlationId": "book-1",
  "type": "booking.create.response",
  "timestamp": "2026-11-20T04:04:01Z",
  "attempt": 1,
  "payload": {
    "error": {
      "code": "ROOM_NOT_AVAILABLE",
      "message": "The requested inventory is no longer available.",
      "retryable": false
    }
  }
}
```

### PP-05 · Cancel booking (customer)

Request contains `cancellationId: string`, `bookingId: string`, `customerId: string`, and `reason?: string`. Response data contains `bookingId: string`, `version: integer`, `status: BookingStatus`, and `payment: Payment`. PP checks ownership and cancellability, changes the reservation once, and reports the current return process through CP-01. Final errors include `BOOKING_NOT_FOUND`, `NOT_BOOKING_OWNER`, and `BOOKING_NOT_CANCELLABLE`. **CP decision D7.** Confirm the cancellable states, stay cutoff, scope of full versus partial refunds, and handling of already released funds.

```json
{
  "messageId": "m-13",
  "correlationId": "cancel-1",
  "type": "booking.cancel.request",
  "timestamp": "2026-11-21T04:05:00Z",
  "attempt": 1,
  "payload": {
    "cancellationId": "can-1",
    "bookingId": "b-1",
    "customerId": "c-9",
    "reason": "Travel plans changed"
  }
}
```

```json
{
  "messageId": "m-14",
  "correlationId": "cancel-1",
  "type": "booking.cancel.response",
  "timestamp": "2026-11-21T04:05:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "bookingId": "b-1",
      "version": 4,
      "status": "CANCELLED_BY_CUSTOMER",
      "payment": {
        "status": "REFUND_PENDING",
        "captureReference": "cap-1",
        "releaseReference": "rel-1",
        "refundReference": null
      }
    }
  }
}
```

### PP-06 · List or look up a customer's bookings

Request contains `customerId: string`, `orderId?: string`, and `PageInput`. If `orderId` is supplied, omit pagination inputs. Response data contains `items: BookingSummary[]`, `nextCursor: string|null`, and `orderDecision?: OrderDecision`. Return `orderDecision` only on an order lookup. Its fields are `status: ACCEPTED|REJECTED|UNKNOWN` and `error?: Error` (required on `REJECTED`). A lookup is scoped to the supplied customer. `UNKNOWN` means PP has no definitive visible decision for that customer and order. A mismatched customer must not be shown the existence or state of someone else's booking.

`BookingSummary` contains `bookingId: string`, `orderId: string`, `providerId: string`, `providerName: string`, `roomId: string`, `roomName: string`, `checkIn: date`, `checkOut: date`, `quantity: integer`, `version: integer`, `status: BookingStatus`, `totalAmount: Money`, `payment: Payment`, and `reviewEligibleFrom: timestamp|null`. `COMPLETED` represents a completed stay. Payment release is represented independently.

**CP decision D8.** Proposed review eligibility begins on the first local day of a qualifying approved or completed stay, after successful escrow release. Pending or cancelled bookings return null. PP supplies evidence for the accommodation provider review. CP checks the current time and owns review state and the Pending & Active, Not Reviewed, and Past groupings. Confirm the booking states, treatment of cancellation after check-in, and which property timezone determines the first day.

```json
{
  "messageId": "m-15",
  "correlationId": "lookup-1",
  "type": "booking.list.request",
  "timestamp": "2026-11-20T04:04:06Z",
  "attempt": 1,
  "payload": {
    "customerId": "c-9",
    "orderId": "o-1"
  }
}
```

```json
{
  "messageId": "m-16",
  "correlationId": "lookup-1",
  "type": "booking.list.response",
  "timestamp": "2026-11-20T04:04:07Z",
  "attempt": 1,
  "payload": {
    "data": {
      "items": [
        {
          "bookingId": "b-1",
          "orderId": "o-1",
          "providerId": "p-1",
          "providerName": "Example Hotel",
          "roomId": "r-3",
          "roomName": "Twin Room",
          "checkIn": "2027-01-10",
          "checkOut": "2027-01-12",
          "quantity": 1,
          "version": 1,
          "status": "PENDING",
          "totalAmount": {
            "amount": 15300,
            "currency": "USD"
          },
          "payment": {
            "status": "CAPTURED_HELD",
            "captureReference": "cap-1",
            "releaseReference": null,
            "refundReference": null
          },
          "reviewEligibleFrom": null
        }
      ],
      "nextCursor": null,
      "orderDecision": {
        "status": "ACCEPTED"
      }
    }
  }
}
```

### CP-01 · Booking outcome event

PP publishes after committing each subsequent booking or payment change. Payload contains `bookingId: string`, `orderId: string`, `customerId: string`, `version: integer`, `status: BookingStatus`, `payment: Payment`, and `updatedAt: timestamp`. Each event is a snapshot. PP uses `bookingId` as the Kafka key and increments `version` per committed change. CP applies only a snapshot newer than the last applied version for that booking, regardless of whether it came from a reply or an event. Process confirmation email eligibility once per booking from a qualifying outcome, even if a reply with the same version arrived first. Suppress an obsolete confirmation if CP already knows the booking is cancelled. CP uses PP-06 for reconciliation. **CP decision D5** covers reliable publication after commit and idempotent consumer processing.

Approval and successful release, followed later by successful cancellation and refund, are shown below. Intermediate pending outcomes use the same schema.

```json
{
  "messageId": "m-17",
  "type": "booking.outcome.changed",
  "timestamp": "2026-11-20T04:10:00Z",
  "payload": {
    "bookingId": "b-1",
    "orderId": "o-1",
    "customerId": "c-9",
    "version": 3,
    "status": "APPROVED",
    "payment": {
      "status": "RELEASED",
      "captureReference": "cap-1",
      "releaseReference": "rel-1",
      "refundReference": null
    },
    "updatedAt": "2026-11-20T04:10:00Z"
  }
}
```

```json
{
  "messageId": "m-18",
  "type": "booking.outcome.changed",
  "timestamp": "2026-11-21T04:06:00Z",
  "payload": {
    "bookingId": "b-1",
    "orderId": "o-1",
    "customerId": "c-9",
    "version": 5,
    "status": "CANCELLED_BY_CUSTOMER",
    "payment": {
      "status": "REFUNDED",
      "captureReference": "cap-1",
      "releaseReference": "rel-1",
      "refundReference": "ref-1"
    },
    "updatedAt": "2026-11-21T04:06:00Z"
  }
}
```

## 5. Regional-event suggestions

### PP-07a · Register an event favorite

Covers `CP-EVENT-U01/U02/U03` and `PP-EVENT-U01/U02`. CP owns event administration, favorites, and membership eligibility. CP creates the Kafka topics for this exchange and sends the current event data. Request contains `favoriteId: string`, `eventId: string`, `eventName: string`, `city: string`, `country: string`, `startsAt: timestamp`, `endsAt: timestamp`, `notifyFrom: timestamp`, and `searchLink: string` (the CP city-search route). CP maps the opaque `favoriteId` to a customer internally.

PP durably accepts the work before replying. Response data contains `favoriteId: string`, `scheduledFor: timestamp`, and `status: SCHEDULED|QUEUED`. Use `QUEUED` when the favorite is handled from `notifyFrom` through the ongoing event, up to but excluding `endsAt`. Before `notifyFrom`, use `SCHEDULED` and publish at that timestamp. If the event has ended when first received, return final `EVENT_ENDED`. PP validates that `notifyFrom` is on or before `startsAt` and that `endsAt` follows `startsAt`. A replay of the same favorite and trigger returns the stored acknowledgement without creating another delivery.

| When PP receives the favorite | Expected behavior |
|---|---|
| Before the agreed two-month cutoff | Acknowledge with `SCHEDULED` and publish at the cutoff. |
| From the cutoff until the event starts | Acknowledge with `QUEUED` and publish promptly. |
| During the event | Acknowledge with `QUEUED` and publish promptly. |
| At or after the event ends | Return `EVENT_ENDED`. |

**CP decision D9.** Proposed timing has CP supply the authoritative cutoff and PP own scheduling. Confirm the event timezone and two-month boundary. The CP example gives 11 February for a 12 April start, which differs from subtracting two calendar months on the local date. CP rechecks that the event is still in its eligible window, the same favorite is active, and the customer is still Premium at display time. Agree how scheduled work is replaced when event details change and which dates determine recommendation availability and pricing.

```json
{
  "messageId": "m-19",
  "correlationId": "favorite-1",
  "type": "event.favorite.request",
  "timestamp": "2026-11-20T04:15:00Z",
  "attempt": 1,
  "payload": {
    "favoriteId": "f-1",
    "eventId": "e-1",
    "eventName": "Example City Festival",
    "city": "hcmc",
    "country": "VN",
    "startsAt": "2027-03-20T01:00:00Z",
    "endsAt": "2027-03-21T15:00:00Z",
    "notifyFrom": "2027-01-20T01:00:00Z",
    "searchLink": "/search?country=VN&city=hcmc"
  }
}
```

```json
{
  "messageId": "m-20",
  "correlationId": "favorite-1",
  "type": "event.favorite.response",
  "timestamp": "2026-11-20T04:15:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "favoriteId": "f-1",
      "scheduledFor": "2027-01-20T01:00:00Z",
      "status": "SCHEDULED"
    }
  }
}
```

### PP-07b · Publish event suggestions

PP finds the two highest-rated providers in the event city with room availability for the agreed dates. Payload contains `favoriteId: string`, `eventId: string`, `eventName: string`, `city: string`, `country: string`, `startsAt: timestamp`, `endsAt: timestamp`, `searchLink: string`, and `providers: EventProvider[]`. `EventProvider` contains `providerId: string`, `name: string`, `category: ProviderCategory`, `logoUrl: string|null`, `pricePerNight: Money`, and `overallRating: number`. Here `name` is the accommodation provider's display name. The event copies the trigger's `correlationId`. The acknowledgement in PP-07a satisfies the request timeout. This notification can arrive months later.

**CP decision D9.** Proposal when fewer than two qualify is to send the available ranked results, with `[]` for none. Break rating ties by `providerId`. CP displays an eligible favorite's notification at most once, even on redelivery. Agree the handling of empty results and delayed publishing failures. CP suppresses delivery after event end.

```json
{
  "messageId": "m-21",
  "correlationId": "favorite-1",
  "type": "event.suggestions.published",
  "timestamp": "2027-01-20T01:00:00Z",
  "payload": {
    "favoriteId": "f-1",
    "eventId": "e-1",
    "eventName": "Example City Festival",
    "city": "hcmc",
    "country": "VN",
    "startsAt": "2027-03-20T01:00:00Z",
    "endsAt": "2027-03-21T15:00:00Z",
    "searchLink": "/search?country=VN&city=hcmc",
    "providers": [
      {
        "providerId": "p-1",
        "name": "Example Hotel",
        "category": "HOTEL",
        "logoUrl": "https://example.com/p-1.webp",
        "pricePerNight": {
          "amount": 8500,
          "currency": "USD"
        },
        "overallRating": 4.6
      },
      {
        "providerId": "p-2",
        "name": "Example Homestay",
        "category": "HOMESTAY",
        "logoUrl": null,
        "pricePerNight": {
          "amount": 7000,
          "currency": "USD"
        },
        "overallRating": 4.4
      }
    ]
  }
}
```

## 6. Customer Portal data services

### CP-02 · Get provider rating statistics

Covers `PP-RATING-S01/M01/U01/U02` and `CP-RATING-U01`. Request contains `providerId: string` and `metrics: RatingMetric[]`. `RatingMetric` is `OVERALL`, `SERVICE`, `FRIENDLINESS`, or `ROOM`. Response data contains `providerId: string`, `overall?: RatingSummary` (required when requested), and `subcategories: object` keyed by the requested non-Overall metrics. `RatingSummary` contains `average: number|null` and `count: integer`. Subcategory entries add `presetOptions: PresetOption[]` with `optionId: string`, `label: string`, `starGroup: 1-2|3|4-5`, `count: integer`, and `percentage: number`. A requested subcategory includes all its two-per-group options, even when their count is zero. PP displays CP's computed figures.

**CP decision D10.** Proposed percentage denominator is all active reviews with a rating in the option's star group for that subcategory, including those that chose no option. A zero denominator returns `0`. A metric with zero ratings returns `count: 0` and `average: null`. Confirm denominator, rounding, and the stable option IDs and labels.

```json
{
  "messageId": "m-22",
  "correlationId": "rating-1",
  "type": "rating.stats.request",
  "timestamp": "2026-11-20T04:20:00Z",
  "attempt": 1,
  "payload": {
    "providerId": "p-1",
    "metrics": [
      "OVERALL",
      "SERVICE"
    ]
  }
}
```

```json
{
  "messageId": "m-23",
  "correlationId": "rating-1",
  "type": "rating.stats.response",
  "timestamp": "2026-11-20T04:20:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "providerId": "p-1",
      "overall": {
        "average": 4.6,
        "count": 10
      },
      "subcategories": {
        "SERVICE": {
          "average": 4.2,
          "count": 10,
          "presetOptions": [
            {
              "optionId": "svc-low-1",
              "label": "Slow service",
              "starGroup": "1-2",
              "count": 0,
              "percentage": 0
            },
            {
              "optionId": "svc-low-2",
              "label": "Service unavailable",
              "starGroup": "1-2",
              "count": 0,
              "percentage": 0
            },
            {
              "optionId": "svc-mid-1",
              "label": "Acceptable wait",
              "starGroup": "3",
              "count": 1,
              "percentage": 50
            },
            {
              "optionId": "svc-mid-2",
              "label": "Adequate service",
              "starGroup": "3",
              "count": 0,
              "percentage": 0
            },
            {
              "optionId": "svc-high-1",
              "label": "Quick service",
              "starGroup": "4-5",
              "count": 4,
              "percentage": 50
            },
            {
              "optionId": "svc-high-2",
              "label": "Convenient service",
              "starGroup": "4-5",
              "count": 2,
              "percentage": 25
            }
          ]
        }
      }
    }
  }
}
```

### CP-03 · List provider reviews

Request contains `providerId: string` and `PageInput`. Response data contains `items: ProviderReview[]` and `nextCursor: string|null`. `ProviderReview` contains `reviewId: string`, `customerName: string` (CP-approved display name), `overall: integer` (1–5), `comment: string|null`, and `createdAt: timestamp`. CP excludes archived reviews. **CP decision D10.** Confirm display-name access and the display path for comments where required by `PP-SEARCH-S02`.

```json
{
  "messageId": "m-24",
  "correlationId": "reviews-1",
  "type": "review.list.request",
  "timestamp": "2026-11-20T04:21:00Z",
  "attempt": 1,
  "payload": {
    "providerId": "p-1",
    "limit": 10
  }
}
```

```json
{
  "messageId": "m-25",
  "correlationId": "reviews-1",
  "type": "review.list.response",
  "timestamp": "2026-11-20T04:21:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "items": [
        {
          "reviewId": "rev-1",
          "customerName": "Guest A",
          "overall": 5,
          "comment": "Easy check-in.",
          "createdAt": "2026-11-18T09:00:00Z"
        }
      ],
      "nextCursor": null
    }
  }
}
```

### CP-04 · Get customer membership

Request contains `customerId: string`. Response data contains `customerId: string`, `membership: Membership`, and `premiumUntil: timestamp|null`. CP returns membership as of evaluation time. The proposed expiry boundary is exclusive. Unknown customers return final `CUSTOMER_NOT_FOUND`. PP uses this exchange for quote eligibility under **CP decision D6**.

```json
{
  "messageId": "m-26",
  "correlationId": "membership-1",
  "type": "membership.get.request",
  "timestamp": "2026-11-20T04:03:00Z",
  "attempt": 1,
  "payload": {
    "customerId": "c-9"
  }
}
```

```json
{
  "messageId": "m-27",
  "correlationId": "membership-1",
  "type": "membership.get.response",
  "timestamp": "2026-11-20T04:03:01Z",
  "attempt": 1,
  "payload": {
    "data": {
      "customerId": "c-9",
      "membership": "PREMIUM",
      "premiumUntil": "2027-06-01T00:00:00Z"
    }
  }
}
```

## 7. Decisions pending joint review

| ID | Agreement needed |
|---|---|
| D1 | Final wire names, field shapes, retry timeouts, attempt limits, and retryable errors. |
| D2 | Topic provisioning, broker identities and permissions, and customer data scope. Name an owner for each portal. |
| D3 | Search OR semantics, expected match fixtures, classifications, stable locations, and pagination. |
| D4 | Escrow sandbox, capture verification, release and refund capabilities, credential ownership, and confirmation timing. |
| D5 | Stable IDs, retention, recovery after retry exhaustion, reliable event publication, and duplicate handling across failover. |
| D6 | Acceptance of PP-08, quote lifetime and rounding, membership eligibility, and refund ownership when a capture precedes a rejected booking. |
| D7 | Cancellation cutoff, eligible states, and the return path after release. |
| D8 | Booking states and local time rules for review eligibility and CP's booking groupings. |
| D9 | Event trigger ownership and topics, two-month cutoff, scheduling updates, recipient recheck, and ranking, availability, and empty-result rules. |
| D10 | Rating aggregation, stable feedback options, comment access, and missing-data behavior. |
| D11 | Link the cross-squad PMS contract when both squads agree on shared identifiers, authorized operations, entitlement checks, data scope, and implementation ownership (`PP-PMS-S01/S02`). |

## 8. Change log

| Version | Date | Change |
|---|---|---|
| v0.1 | 2026-11-20 | First PP draft. |
| v0.2 | 2026-11-20 | Defined Ultimo Kafka messages, added a pre-capture quote, specified escrow and booking retries, and documented the event trigger and notification. |
