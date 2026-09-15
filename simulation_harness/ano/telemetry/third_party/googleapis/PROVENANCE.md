# Vendored third-party schema text — PROVENANCE

## What is here

Three `.proto` files, copied **verbatim and unmodified**, from the public
`googleapis` distribution:

| File | Upstream path |
|---|---|
| `google/logging/v2/log_entry.proto` | `//depot/google3/third_party/googleapis/stable/google/logging/v2/log_entry.proto` |
| `google/logging/type/log_severity.proto` | `//depot/google3/third_party/googleapis/stable/google/logging/type/log_severity.proto` |
| `google/logging/type/http_request.proto` | `//depot/google3/third_party/googleapis/stable/google/logging/type/http_request.proto` |

Copied 2026-09-13. Each retains its original header:

```
// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
...
```

**Licence: Apache License 2.0.** These are the published, externally visible
definitions — the same text any Google Cloud customer can read.

## Why they are here

They are **not compiled and not executed**. No `protobuf` runtime exists in
this environment and none is required. They serve exactly two purposes:

1. **Auditable provenance.** A reviewer can diff
   `ano/telemetry/schemas/logentry_schema.json` against the real published
   proto, in this repository, offline, without trusting anyone's summary of it.

2. **A drift test.** `tests/m2_generator/test_logentry_drift.py` parses this
   proto text with a regex and asserts that every field name and field number
   in our declarative schema agrees with it, that no proto field is
   unaccounted for, and that every field on our "must not emit" list is
   genuinely absent from the public schema. If Google adds a field, or if we
   typo a field number, that test fails — offline, with no network.

This converts *"we believe our schema is right"* into *"our schema is
mechanically pinned to the published proto"*.

## What is deliberately NOT vendored

* Google's **internal** `//depot/google3/google/logging/v2/log_entry.proto`.
  It is Google-internal, and it carries restricted fields
  (`internalId`, `writerEmailAddress`, `errorGroup`, `transformRevisions`,
  `receiveLocation`) plus internal annotations. Those restricted fields are
  precisely the ones our generator must **not** emit, so including them would
  actively mislead a reader about what the public schema contains.

* The monitored-resource descriptor registry
  (`nexus/platform/logging/monitored_resources/cloud.json`) — Google-internal
  config. The *facts* it contains (resource type → label key set) are
  published at `cloud.google.com/logging/docs/api/v2/resource-list`, and are
  restated in `ano/telemetry/schemas/monitored_resources.json` rather than
  copied.

* Any captured real `/metrics` payload. The node_exporter capture used as a
  shape reference during development is a Google-internal experimental file;
  the repository instead carries small fixtures authored here, plus tests
  asserting the parser accepts the constructs a real capture exhibits.

## Note on `google/api/monitored_resource.proto`

`LogEntry.resource` is a `google.api.MonitoredResource`, whose definition lives
in a different proto that is **not** vendored here. Its shape (`type` plus a
`map<string,string> labels`) is trivial and stable, and it is validated
structurally rather than by drift test. This is recorded as a known gap rather
than silently glossed over.
