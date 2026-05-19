# Object-Store External Sources (OBJ slice 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `CREATE EXTERNAL SOURCE … FROM 's3://…' | 'az://…'` read JSON/CSV/NDJSON straight from S3-compatible or Azure Blob object storage, by resolving the object to a signed HTTPS GET at the router and feeding it through the shipped EXT fetch→decode→capture-once→atomic-Txn→replicate pipeline.

**Architecture:** A new pure-Rust optional crate `kessel-objstore` implements AWS SigV4 and Azure Shared-Key GET signing (deterministic given an injected `now`, reusing in-tree zero-dep `kessel-crypto` for SHA-256/HMAC). `kessel-fetch` gains an opt-in `object-store` feature (implies `tls`) and a thin `fetch_rows_signed` that reuses the existing rustls transport + `exchange` + `rows_from_body`. Recipe/proto/SQL gain backward-compatible additive object-store fields (SP98/SP86 discipline). The router's `do_refresh` gains a one-branch scheme dispatch that resolves credentials router-side by env-var **name** (never values in op/WAL/log/digest), signs, and fetches. Everything else (deterministic ObjectId, atomic `Op::Txn`, fail-closed) is the unchanged EXT path.

**Tech Stack:** Rust; `kessel-crypto` (in-tree, zero-dep) for SHA-256/HMAC-SHA256/hex; `rustls 0.23`+`webpki-roots` (already vendored via the `tls` feature); no new external dependency anywhere.

**Spec:** `docs/superpowers/specs/2026-05-19-object-store-sources-design.md`
**Internal record (write in T12):** `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`

**Conventions (every task):** repo `C:\Users\ihass\KesselDB`; commit straight to `main` (single-branch, user-authorized via the standing autonomous mandate); **no** `Co-Authored-By`, **no** signing; match `git log -3 --format='%s'` style (`area: summary`, `(review polish)` for follow-ups). After each task's final commit, `git push`. Use the Bash tool (git-bash); forward-slash paths work.

**Determinism gate (run after every kernel-adjacent task — T6/T7/T8/T10/T11/T12):** `cargo test --workspace --release` ⇒ every `test result:` line `0 failed` AND `large_seed_corpus_is_deterministic_and_converges` present + passing. Baseline (recorded in Task 0) = **247**. **Target default-build delta = 0**: every new test is `#[cfg(feature=...)]`-gated or lives in `kessel-objstore` (which the default workspace build does not compile — nothing depends on it without a feature). If any new default-build test is unavoidable, the task MUST state it explicitly and T12 reconciles README/STATUS to the true number. Do NOT fake known-answer crypto vectors — use the real documented AWS/Azure expected values; if a real vector cannot be obtained, report BLOCKED.

---

## File Structure

- `crates/kessel-objstore/Cargo.toml` — new crate; dep: `kessel-crypto` only.
- `crates/kessel-objstore/src/lib.rs` — public API: `ObjGetRequest`, `ObjCreds`, `SignedRequest`, `ObjError`, `DateTime`, `Provider`, `sign_get`.
- `crates/kessel-objstore/src/b64.rs` — minimal pure base64 encode/decode (std alphabet, padding).
- `crates/kessel-objstore/src/sigv4.rs` — AWS SigV4 GET signer + RFC-3986 URI encoding.
- `crates/kessel-objstore/src/azure.rs` — Azure Blob Shared-Key GET signer.
- `crates/kessel-fetch/Cargo.toml` — add optional `kessel-objstore` + `object-store` feature.
- `crates/kessel-fetch/src/http.rs` — add `build_request_with_headers` (extract; existing `build_request` delegates, no behavior change).
- `crates/kessel-fetch/src/lib.rs` — add `#[cfg(feature="object-store")] fetch_rows_signed`.
- `crates/kessel-fetch/tests/objstore_stub.rs` — feature-gated localhost rustls stub test.
- `crates/kessel-catalog/src/lib.rs` — v3 trailer: `ExternalAuth::ObjStoreEnv` + `ExternalRecipe.region/endpoint`.
- `crates/kessel-proto/src/lib.rs` — additive tolerant `Op::CreateExternalSource.objstore` field.
- `crates/kessel-sm/src/lib.rs` — apply maps auth_kind 3 + objstore tuple → recipe.
- `crates/kessel-sql/src/lib.rs` — `s3://`/`az://`, `REGION`, `ENDPOINT`, `AUTH OBJSTORE …`, CREATE-time rejections.
- `crates/kesseldb-server/src/router.rs` — `do_refresh` scheme dispatch (feature-gated).
- `crates/kesseldb-server/Cargo.toml` — `external-sources-objstore` composite feature.
- `crates/kesseldb-server/tests/external_source_objstore_oracle.rs` — feature-gated e2e.
- `docs/USAGE.md`, `docs/STATUS.md`, `README.md`, the internal subproject100 record.

---

### Task 0: Record the determinism baseline

**Files:** none.

- [ ] **Step 1: Capture the default-build total + seed-7**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus" | tee /tmp/kdb-obj-baseline.txt`
Expected: every line `0 failed`; `large_seed_corpus_is_deterministic_and_converges ... ok`. Sum the `N passed` across `test result:` lines; record as **BASELINE = 247** in working notes (it should equal 247 — if it differs, use the measured value and note it). No commit.

---

### Task 1: Scaffold `kessel-objstore` (types + base64) + workspace wiring

**Files:**
- Create: `crates/kessel-objstore/Cargo.toml`, `crates/kessel-objstore/src/lib.rs`, `crates/kessel-objstore/src/b64.rs`
- Modify: root `Cargo.toml` (workspace members)

- [ ] **Step 1: Add the crate to the workspace**

Read root `crates`/workspace list: `cd /c/Users/ihass/KesselDB && grep -n "members" -n Cargo.toml && sed -n '1,40p' Cargo.toml`. Add `"crates/kessel-objstore"` to the `members` array (match existing formatting exactly — alphabetical/positional as the file does).

- [ ] **Step 2: Create `crates/kessel-objstore/Cargo.toml`**

```toml
[package]
name = "kessel-objstore"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
# In-tree, zero external deps. Provides sha256 / hmac_sha256 / hex —
# exactly the AWS SigV4 + Azure Shared-Key primitives, so this crate
# adds NO new external dependency anywhere.
kessel-crypto = { path = "../kessel-crypto" }

[lib]
path = "src/lib.rs"
```

- [ ] **Step 3: Write the base64 failing test**

Create `crates/kessel-objstore/src/b64.rs`:

```rust
//! Minimal std-alphabet base64 (encode + decode) — no external dep.
//! Azure account keys are base64; the signature is base64-encoded.

const ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn encode(input: &[u8]) -> String {
    let mut out = String::with_capacity((input.len() + 2) / 3 * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

pub fn decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() % 4 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= if c == b'=' { 0 } else { val(c)? } << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64_round_trip_and_known_vectors() {
        // RFC 4648 §10 test vectors.
        assert_eq!(encode(b""), "");
        assert_eq!(encode(b"f"), "Zg==");
        assert_eq!(encode(b"fo"), "Zm8=");
        assert_eq!(encode(b"foo"), "Zm9v");
        assert_eq!(encode(b"foob"), "Zm9vYg==");
        assert_eq!(encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(encode(b"foobar"), "Zm9vYmFy");
        for v in [
            "Zg==", "Zm8=", "Zm9v", "Zm9vYg==", "Zm9vYmE=", "Zm9vYmFy",
        ] {
            assert_eq!(encode(&decode(v).unwrap()), v);
        }
        // A 32-byte key (Azure-style) round-trips.
        let k = [7u8; 32];
        assert_eq!(decode(&encode(&k)).unwrap(), k);
        assert!(decode("not*valid").is_none());
    }
}
```

- [ ] **Step 4: Write the public API skeleton in `crates/kessel-objstore/src/lib.rs`**

```rust
//! Object-store request signing (AWS SigV4 / Azure Shared Key) for
//! GET. Pure + deterministic given an injected `DateTime`; performs
//! NO I/O. Optional crate — never compiled by the default KesselDB
//! workspace build (nothing depends on it without a feature), so the
//! deterministic kernel and seed-7 corpus are untouched.
#![forbid(unsafe_code)]

mod b64;
mod sigv4;
mod azure;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provider {
    S3,
    Azure,
}

/// Wall clock, injected by the caller so the signer is unit-testable
/// against fixed vectors (no `std::time` call inside the signer).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DateTime {
    /// Seconds since the Unix epoch (UTC).
    pub secs_since_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjCreds {
    /// AWS / S3-compatible.
    S3 { key_id: String, secret: String },
    /// Azure Blob Shared Key (`key_b64` is the base64 account key).
    AzureSharedKey { account: String, key_b64: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjGetRequest {
    pub provider: Provider,
    /// S3 bucket OR Azure container.
    pub bucket_or_container: String,
    /// Object key / blob path (no leading '/').
    pub key: String,
    /// S3 region (required for AWS virtual-hosted; ignored for Azure).
    pub region: Option<String>,
    /// Custom endpoint base (S3-compatible path-style / custom Azure).
    /// MUST be `https://…` when set. None ⇒ provider default host.
    pub endpoint: Option<String>,
    pub creds: ObjCreds,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedRequest {
    /// Absolute `https://…` URL to GET.
    pub https_url: String,
    /// Signed request headers (name, value), incl. `host`,
    /// `Authorization`, and the provider date/content headers.
    pub headers: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjError {
    BadUrl(String),
    BadEndpoint(String),
    Cred(String),
    Encoding(String),
}

impl std::fmt::Display for ObjError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjError::BadUrl(s) => write!(f, "objstore url: {s}"),
            ObjError::BadEndpoint(s) => write!(f, "objstore endpoint: {s}"),
            ObjError::Cred(s) => write!(f, "objstore cred: {s}"),
            ObjError::Encoding(s) => write!(f, "objstore encoding: {s}"),
        }
    }
}

/// Sign a GET. Pure given `now`. Never touches the network.
pub fn sign_get(
    req: &ObjGetRequest,
    now: DateTime,
) -> Result<SignedRequest, ObjError> {
    match req.provider {
        Provider::S3 => sigv4::sign_get_s3(req, now),
        Provider::Azure => azure::sign_get_azure(req, now),
    }
}

/// `YYYYMMDD` and `YYYYMMDDTHHMMSSZ` from epoch seconds (UTC,
/// proleptic Gregorian). Pure — used by both signers and unit-tested
/// directly so the AWS/Azure known-answer vectors are reproducible.
pub(crate) fn ymd_hms(secs: u64) -> (String, String) {
    // days since epoch + seconds within day
    let days = (secs / 86_400) as i64;
    let sod = (secs % 86_400) as u32;
    let (h, mi, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    // civil-from-days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as i64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0,365]
    let mp = (5 * doy + 2) / 153; // [0,11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1,31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1,12]
    let year = (y + i64::from(m <= 2)) as u64;
    let date = format!("{year:04}{m:02}{d:02}");
    let dt = format!("{date}T{h:02}{mi:02}{s:02}Z");
    (date, dt)
}

#[cfg(test)]
mod time_tests {
    use super::ymd_hms;
    #[test]
    fn ymd_hms_known_instants() {
        // 2015-08-30T12:36:00Z = 1440938160 (AWS SigV4 suite epoch).
        assert_eq!(
            ymd_hms(1_440_938_160),
            ("20150830".into(), "20150830T123600Z".into())
        );
        // 2013-11-24T08:31:35Z (Azure doc example) = 1385281895.
        assert_eq!(
            ymd_hms(1_385_281_895),
            ("20131124".into(), "20131124T083135Z".into())
        );
        // epoch.
        assert_eq!(
            ymd_hms(0),
            ("19700101".into(), "19700101T000000Z".into())
        );
    }
}
```

- [ ] **Step 5: Stub the two signer modules so the crate compiles**

Create `crates/kessel-objstore/src/sigv4.rs`:

```rust
use crate::{ObjError, ObjGetRequest, SignedRequest};

pub(crate) fn sign_get_s3(
    _req: &ObjGetRequest,
    _now: crate::DateTime,
) -> Result<SignedRequest, ObjError> {
    Err(ObjError::Encoding("sigv4 not implemented (Task 2)".into()))
}
```

Create `crates/kessel-objstore/src/azure.rs`:

```rust
use crate::{ObjError, ObjGetRequest, SignedRequest};

pub(crate) fn sign_get_azure(
    _req: &ObjGetRequest,
    _now: crate::DateTime,
) -> Result<SignedRequest, ObjError> {
    Err(ObjError::Encoding("azure not implemented (Task 3)".into()))
}
```

- [ ] **Step 6: Build + run the crate's tests**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-objstore`
Expected: PASS — `b64_round_trip_and_known_vectors`, `ymd_hms_known_instants`. (`kessel-crypto` provides nothing yet-used; that's fine.)

- [ ] **Step 7: Confirm default workspace build does NOT compile this crate**

Run: `cd /c/Users/ihass/KesselDB && cargo build 2>&1 | tail -2 && cargo tree -p kessel-fetch -e normal | grep -i objstore || echo "OBJSTORE NOT IN DEFAULT GRAPH"`
Expected: workspace builds; prints `OBJSTORE NOT IN DEFAULT GRAPH` (nothing depends on it yet).

- [ ] **Step 8: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add Cargo.toml crates/kessel-objstore
git commit -m "objstore: scaffold kessel-objstore crate (api + base64 + epoch date)"
```

---

### Task 2: AWS SigV4 GET signer (`sigv4.rs`) — known-answer vector first

**Files:**
- Modify: `crates/kessel-objstore/src/sigv4.rs`

- [ ] **Step 1: Write the failing known-answer test**

Replace `crates/kessel-objstore/src/sigv4.rs` test section by appending this module (and the implementation in Step 3). The vector is the **canonical AWS Signature V4 example** (AWS docs "Examples of the complete Signature Version 4 signing process", `GET https://examplebucket.s3.amazonaws.com/test.txt` range example reduced to a no-range GET) — fixed credentials/date so the expected `Authorization` is reproducible:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DateTime, ObjCreds, ObjGetRequest, Provider};

    // AWS docs canonical creds (public example key, not a real secret):
    //   AKIAIOSFODNN7EXAMPLE /
    //   wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    // GET https://examplebucket.s3.amazonaws.com/test.txt at
    // 20130524T000000Z, region us-east-1, service s3, empty payload.
    // Expected from AWS "Example: GET Object" (no Range header) —
    // canonical request → string to sign → signature documented by AWS.
    #[test]
    fn sigv4_aws_doc_get_object_known_answer() {
        let req = ObjGetRequest {
            provider: Provider::S3,
            bucket_or_container: "examplebucket".into(),
            key: "test.txt".into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            creds: ObjCreds::S3 {
                key_id: "AKIAIOSFODNN7EXAMPLE".into(),
                secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            },
        };
        // 20130524T000000Z = 1369353600.
        let s = sign_get_s3(&req, DateTime { secs_since_epoch: 1_369_353_600 })
            .expect("sign");
        assert_eq!(
            s.https_url,
            "https://examplebucket.s3.us-east-1.amazonaws.com/test.txt"
        );
        let auth = s
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .map(|(_, v)| v.clone())
            .expect("authorization header");
        // AWS-documented expected signature for this exact request.
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 \
Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
Signature=98e3a4e1311c98a98f53b8c80f8e0fab1aa8a0a48d3c719c8e2a4f5b1b8c7d6e"
        );
    }
}
```

> **Implementer note (REQUIRED, do not skip):** the `Signature=` hex in the assertion above is a **placeholder digest and WILL be wrong**. Before implementing, obtain the **real** expected signature for this exact canonical request from the AWS documentation page *"Examples of the complete Signature Version 4 signing process (Python)"* / the `aws-sig-v4-test-suite`. Compute it independently (e.g. a one-off local script using the same documented inputs) and replace BOTH the asserted `Signature=` value here AND keep it as the known-answer. The canonical AWS doc example for `GET /test.txt` (empty payload, `x-amz-content-sha256` = SHA256("") = `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`) yields a fixed, documented signature; use that exact value. If you cannot obtain/derive the real value, STOP and report BLOCKED — do NOT ship a self-referential test that asserts whatever the code produces.

- [ ] **Step 2: Run it — expect failure**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-objstore sigv4_aws_doc_get_object_known_answer -- --nocapture`
Expected: FAIL (`sigv4 not implemented`).

- [ ] **Step 3: Implement the SigV4 GET signer**

Replace the top of `crates/kessel-objstore/src/sigv4.rs` (above the `#[cfg(test)]`) with:

```rust
use crate::{DateTime, ObjCreds, ObjError, ObjGetRequest, SignedRequest};
use kessel_crypto::{hex, hmac_sha256, sha256};

/// SHA-256 of the empty body — sent as `x-amz-content-sha256` so the
/// GET is fully signed (no UNSIGNED-PAYLOAD).
const EMPTY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// RFC-3986 encode one path segment (AWS: unreserved kept, '/' kept by
/// the caller joining segments; everything else %XX upper-hex).
fn enc_seg(seg: &str) -> String {
    let mut o = String::with_capacity(seg.len());
    for &b in seg.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_'
            | b'.' | b'~' => o.push(b as char),
            _ => o.push_str(&format!("%{b:02X}")),
        }
    }
    o
}

/// Canonical URI: each '/'-separated segment encoded, joined by '/',
/// always leading '/'.
fn canonical_uri(key: &str) -> String {
    let mut s = String::from("/");
    let parts: Vec<String> =
        key.split('/').map(enc_seg).collect();
    s.push_str(&parts.join("/"));
    s
}

pub(crate) fn sign_get_s3(
    req: &ObjGetRequest,
    now: DateTime,
) -> Result<SignedRequest, ObjError> {
    let (key_id, secret) = match &req.creds {
        ObjCreds::S3 { key_id, secret } => (key_id, secret),
        _ => return Err(ObjError::Cred("S3 creds required".into())),
    };
    let region = req
        .region
        .as_deref()
        .ok_or_else(|| ObjError::BadUrl("S3 REGION required".into()))?;
    let (date, amzdate) = crate::ymd_hms(now.secs_since_epoch);

    // Host + URL: virtual-hosted by default; path-style if endpoint set.
    let (host, https_url, canon_uri) = match &req.endpoint {
        None => {
            let h = format!(
                "{}.s3.{}.amazonaws.com",
                req.bucket_or_container, region
            );
            let cu = canonical_uri(&req.key);
            (h.clone(), format!("https://{h}{cu}"), cu)
        }
        Some(ep) => {
            let rest = ep.strip_prefix("https://").ok_or_else(|| {
                ObjError::BadEndpoint("endpoint must be https://".into())
            })?;
            let host = rest.split('/').next().unwrap_or(rest).to_string();
            let cu = format!(
                "/{}{}",
                enc_seg(&req.bucket_or_container),
                canonical_uri(&req.key)
            );
            (
                host.clone(),
                format!("https://{host}{cu}"),
                cu,
            )
        }
    };

    // Canonical request (no query; GET; the 3 signed headers, sorted).
    let canonical_headers = format!(
        "host:{host}\nx-amz-content-sha256:{EMPTY_SHA256}\n\
         x-amz-date:{amzdate}\n"
    );
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "GET\n{canon_uri}\n\n{canonical_headers}\n{signed_headers}\n{EMPTY_SHA256}"
    );
    let cr_hash = hex(&sha256(canonical_request.as_bytes()));

    // String to sign.
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!(
        "AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{cr_hash}"
    );

    // Signing key: HMAC chain.
    let k_date =
        hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, sts.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={key_id}/{scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );

    Ok(SignedRequest {
        https_url,
        headers: vec![
            ("host".into(), host),
            ("x-amz-date".into(), amzdate),
            ("x-amz-content-sha256".into(), EMPTY_SHA256.into()),
            ("Authorization".into(), authorization),
        ],
    })
}
```

- [ ] **Step 4: Run the known-answer test**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-objstore sigv4 -- --nocapture`
Expected: PASS once the asserted `Signature=` equals the real AWS-documented value (Step 1 note). If your implementation is correct but the asserted constant was the placeholder, the failure message prints the *actual* signature — verify it byte-for-byte against the AWS documentation example (canonical request hash + string-to-sign documented there), then set the constant to that verified value and re-run. The test must assert a value you confirmed against AWS docs, not merely echo the code.

- [ ] **Step 5: Add encoding + path-style unit tests**

Append to the `tests` mod:

```rust
    #[test]
    fn rfc3986_key_encoding_and_path_style() {
        let mk = |key: &str, endpoint: Option<&str>| ObjGetRequest {
            provider: Provider::S3,
            bucket_or_container: "buck".into(),
            key: key.into(),
            region: Some("us-east-1".into()),
            endpoint: endpoint.map(|s| s.to_string()),
            creds: ObjCreds::S3 {
                key_id: "AKIA".into(),
                secret: "sek".into(),
            },
        };
        // Virtual-hosted, nested key with space + unicode.
        let s = sign_get_s3(
            &mk("a b/c+d/é.json", None),
            DateTime { secs_since_epoch: 1_369_353_600 },
        )
        .unwrap();
        assert_eq!(
            s.https_url,
            "https://buck.s3.us-east-1.amazonaws.com/a%20b/c%2Bd/%C3%A9.json"
        );
        // Path-style via endpoint (MinIO/R2): bucket in the path.
        let s2 = sign_get_s3(
            &mk("k.csv", Some("https://minio.local:9000")),
            DateTime { secs_since_epoch: 1_369_353_600 },
        )
        .unwrap();
        assert_eq!(
            s2.https_url,
            "https://minio.local:9000/buck/k.csv"
        );
        // http:// endpoint rejected.
        assert!(matches!(
            sign_get_s3(
                &mk("k", Some("http://x")),
                DateTime { secs_since_epoch: 1 }
            ),
            Err(ObjError::BadEndpoint(_))
        ));
        // missing region rejected.
        let mut r = mk("k", None);
        r.region = None;
        assert!(matches!(
            sign_get_s3(&r, DateTime { secs_since_epoch: 1 }),
            Err(ObjError::BadUrl(_))
        ));
    }
```

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-objstore` → all PASS.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-objstore/src/sigv4.rs
git commit -m "objstore: AWS SigV4 GET signer (virtual-hosted + path-style, known-answer)"
```

---

### Task 3: Azure Blob Shared-Key GET signer (`azure.rs`)

**Files:**
- Modify: `crates/kessel-objstore/src/azure.rs`

- [ ] **Step 1: Write the failing test (structure + known-answer)**

Append the test module to `crates/kessel-objstore/src/azure.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{b64, DateTime, ObjCreds, ObjGetRequest, Provider};

    #[test]
    fn azure_shared_key_get_structure_and_known_answer() {
        // Account key is base64; this is a throwaway 64-byte test key.
        let key_b64 = b64::encode(&[0x2au8; 64]);
        let req = ObjGetRequest {
            provider: Provider::Azure,
            bucket_or_container: "mycontainer".into(),
            key: "path/to/blob.json".into(),
            region: None,
            endpoint: None,
            creds: ObjCreds::AzureSharedKey {
                account: "devstoreacct".into(),
                key_b64: key_b64.clone(),
            },
        };
        // 2013-11-24T08:31:35Z = 1385281895.
        let s = sign_get_azure(
            &req,
            DateTime { secs_since_epoch: 1_385_281_895 },
        )
        .unwrap();
        assert_eq!(
            s.https_url,
            "https://devstoreacct.blob.core.windows.net/mycontainer/path/to/blob.json"
        );
        let h = |n: &str| {
            s.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(n))
                .map(|(_, v)| v.clone())
        };
        assert_eq!(h("x-ms-date"), Some("Sun, 24 Nov 2013 08:31:35 GMT".into()));
        assert_eq!(h("x-ms-version"), Some("2021-08-06".into()));
        let auth = h("authorization").expect("auth");
        assert!(
            auth.starts_with("SharedKey devstoreacct:"),
            "auth = {auth}"
        );
        // Deterministic: same inputs ⇒ identical signature.
        let s2 = sign_get_azure(
            &req,
            DateTime { secs_since_epoch: 1_385_281_895 },
        )
        .unwrap();
        assert_eq!(s.headers, s2.headers);
        // Recompute the documented string-to-sign signature here
        // independently and assert equality (known-answer): the
        // canonical GET string-to-sign for these fixed inputs.
        let acct_key = b64::decode(&key_b64).unwrap();
        let canon_res =
            "/devstoreacct/mycontainer/path/to/blob.json";
        let sts = format!(
            "GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:Sun, 24 Nov 2013 08:31:35 GMT\nx-ms-version:2021-08-06\n/devstoreacct{}",
            "/mycontainer/path/to/blob.json"
        );
        let _ = canon_res;
        let expect = format!(
            "SharedKey devstoreacct:{}",
            b64::encode(&kessel_crypto::hmac_sha256(
                &acct_key,
                sts.as_bytes()
            ))
        );
        assert_eq!(auth, expect);
    }

    #[test]
    fn azure_endpoint_override_and_bad_key() {
        let req = ObjGetRequest {
            provider: Provider::Azure,
            bucket_or_container: "c".into(),
            key: "b".into(),
            region: None,
            endpoint: Some("https://custom.example.com".into()),
            creds: ObjCreds::AzureSharedKey {
                account: "acct".into(),
                key_b64: b64::encode(&[1u8; 32]),
            },
        };
        let s = sign_get_azure(
            &req,
            DateTime { secs_since_epoch: 1 },
        )
        .unwrap();
        assert_eq!(s.https_url, "https://custom.example.com/c/b");
        // Non-base64 key ⇒ Cred error.
        let mut bad = req.clone();
        bad.creds = ObjCreds::AzureSharedKey {
            account: "acct".into(),
            key_b64: "not*base64".into(),
        };
        assert!(matches!(
            sign_get_azure(&bad, DateTime { secs_since_epoch: 1 }),
            Err(ObjError::Cred(_))
        ));
    }
}
```

- [ ] **Step 2: Run it — expect failure**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-objstore azure -- --nocapture`
Expected: FAIL (`azure not implemented`).

- [ ] **Step 3: Implement the Azure Shared-Key GET signer**

Replace the top of `crates/kessel-objstore/src/azure.rs` (above `#[cfg(test)]`):

```rust
use crate::{b64, DateTime, ObjCreds, ObjError, ObjGetRequest, SignedRequest};
use kessel_crypto::hmac_sha256;

const X_MS_VERSION: &str = "2021-08-06";
const WD: [&str; 7] =
    ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MON: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
    "Oct", "Nov", "Dec",
];

/// RFC-1123 (`Sun, 24 Nov 2013 08:31:35 GMT`) from epoch seconds.
fn http_date(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let sod = (secs % 86_400) as u32;
    let (h, mi, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    // weekday: 1970-01-01 was a Thursday (index 4).
    let wd = WD[(((days % 7) + 7 + 4) % 7) as usize];
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(m <= 2);
    format!(
        "{wd}, {d:02} {} {year:04} {h:02}:{mi:02}:{s:02} GMT",
        MON[(m - 1) as usize]
    )
}

pub(crate) fn sign_get_azure(
    req: &ObjGetRequest,
    now: DateTime,
) -> Result<SignedRequest, ObjError> {
    let (account, key_b64) = match &req.creds {
        ObjCreds::AzureSharedKey { account, key_b64 } => {
            (account, key_b64)
        }
        _ => return Err(ObjError::Cred("Azure creds required".into())),
    };
    let acct_key = b64::decode(key_b64)
        .ok_or_else(|| ObjError::Cred("account key not base64".into()))?;
    let date = http_date(now.secs_since_epoch);

    let host = match &req.endpoint {
        None => format!("{account}.blob.core.windows.net"),
        Some(ep) => {
            let rest = ep.strip_prefix("https://").ok_or_else(|| {
                ObjError::BadEndpoint("endpoint must be https://".into())
            })?;
            rest.split('/').next().unwrap_or(rest).to_string()
        }
    };
    let path = format!(
        "/{}/{}",
        req.bucket_or_container,
        req.key.trim_start_matches('/')
    );
    let https_url = format!("https://{host}{path}");

    // StringToSign for Shared Key (Blob), GET, no extra headers:
    // VERB\n + 12 blank standard-header lines \n + CanonicalizedHeaders
    // + CanonicalizedResource. The 13 components after VERB are:
    // Content-Encoding, Content-Language, Content-Length,
    // Content-MD5, Content-Type, Date, If-Modified-Since,
    // If-Match, If-None-Match, If-Unmodified-Since, Range — all empty
    // (Date empty because we use x-ms-date).
    let canonical_headers = format!(
        "x-ms-date:{date}\nx-ms-version:{X_MS_VERSION}"
    );
    let canonical_resource = format!("/{account}{path}");
    let sts = format!(
        "GET\n\n\n\n\n\n\n\n\n\n\n\n{canonical_headers}\n{canonical_resource}"
    );
    let sig = b64::encode(&hmac_sha256(&acct_key, sts.as_bytes()));
    let authorization = format!("SharedKey {account}:{sig}");

    Ok(SignedRequest {
        https_url,
        headers: vec![
            ("host".into(), host),
            ("x-ms-date".into(), date),
            ("x-ms-version".into(), X_MS_VERSION.into()),
            ("Authorization".into(), authorization),
        ],
    })
}
```

> Note the StringToSign has exactly 12 `\n` between `GET` and the canonical headers (VERB + 11 standard headers + the empty Date line = the documented Blob Shared Key layout). The test's `sts` string MUST match this exactly; if the count is off the deterministic-equality assertion still passes (it recomputes with the same code) but the *layout* must follow the Azure "Authorize with Shared Key" spec — keep the 12-newline block as written.

- [ ] **Step 4: Run + iterate to green**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-objstore`
Expected: all PASS (b64, ymd_hms, sigv4, azure). The azure known-answer test recomputes the documented StringToSign with the same primitive and asserts equality — keep the `sts` in the test byte-identical to the implementation's layout.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-objstore/src/azure.rs
git commit -m "objstore: Azure Blob Shared-Key GET signer (endpoint override, deterministic)"
```

---

### Task 4: `kessel-fetch` `object-store` feature + `fetch_rows_signed`

**Files:**
- Modify: `crates/kessel-fetch/Cargo.toml`, `crates/kessel-fetch/src/http.rs`, `crates/kessel-fetch/src/lib.rs`
- Create: `crates/kessel-fetch/tests/objstore_stub.rs`

- [ ] **Step 1: Cargo wiring**

In `crates/kessel-fetch/Cargo.toml` add under `[dependencies]`:

```toml
kessel-objstore = { path = "../kessel-objstore", optional = true }
```

and under `[features]` add (object storage is HTTPS-only ⇒ implies `tls`):

```toml
object-store = ["tls", "dep:kessel-objstore"]
```

(Leave `tls` and the existing deps unchanged.)

- [ ] **Step 2: Extract `build_request_with_headers` (no behavior change)**

Read `crates/kessel-fetch/src/http.rs` `build_request`. Replace it with the split below — the existing `Auth`-based `build_request` must produce **byte-identical** output (the existing `stub_server.rs`/`paginate_stub.rs` tests are the regression net):

```rust
/// Build an HTTP/1.1 GET with caller-supplied header lines (each
/// emitted verbatim after the Host/Connection/User-Agent lines).
pub(crate) fn build_request_with_headers(
    path: &str,
    host: &str,
    extra: &[(String, String)],
) -> String {
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         User-Agent: kessel-fetch/0\r\n"
    );
    for (k, v) in extra {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req
}

/// Build the HTTP/1.1 GET request text (Host header value is the bare
/// host, unchanged from slice 1).
pub(crate) fn build_request(path: &str, host: &str, auth: &Auth) -> String {
    let extra: Vec<(String, String)> = match auth {
        Auth::None => Vec::new(),
        Auth::Bearer(t) => {
            vec![("Authorization".into(), format!("Bearer {t}"))]
        }
        Auth::Header { name, value } => {
            vec![(name.clone(), value.clone())]
        }
    };
    build_request_with_headers(path, host, &extra)
}
```

- [ ] **Step 3: Verify the http:// regression net is byte-identical**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch && cargo test -p kessel-fetch --features tls`
Expected: all existing tests PASS unchanged (the request bytes are identical: same Host/Connection/User-Agent prefix, same single Auth header, same trailing CRLF).

- [ ] **Step 4: Write the failing feature-gated test**

Create `crates/kessel-fetch/tests/objstore_stub.rs`:

```rust
//! `fetch_rows_signed` over a localhost rustls stub. Only compiled
//! with `--features object-store`. Reuses the SP99 TLS fixture.
#![cfg(feature = "object-store")]

use kessel_catalog::FieldKind;
use kessel_fetch::{
    fetch_rows_signed, ColumnMap, Format, DEFAULT_MAX_BODY,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

const CERT_PEM: &[u8] = include_bytes!("fixtures/localhost.pem");
const KEY_PEM: &[u8] = include_bytes!("fixtures/localhost.key.pem");

fn server_config() -> Arc<rustls::ServerConfig> {
    let certs: Vec<_> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(CERT_PEM))
            .collect::<Result<_, _>>()
            .unwrap();
    let key =
        rustls_pemfile::private_key(&mut std::io::BufReader::new(KEY_PEM))
            .unwrap()
            .unwrap();
    Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
    )
}

/// Accept one TLS conn, capture the request head, serve `body`.
fn stub(body: &'static str) -> (u16, Arc<std::sync::Mutex<String>>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let cfg = server_config();
    let seen = Arc::new(std::sync::Mutex::new(String::new()));
    let s2 = seen.clone();
    thread::spawn(move || {
        if let Ok((sock, _)) = l.accept() {
            let c = rustls::ServerConnection::new(cfg).unwrap();
            let mut tls = rustls::StreamOwned::new(c, sock);
            let mut buf = [0u8; 4096];
            let n = tls.read(&mut buf).unwrap_or(0);
            *s2.lock().unwrap() =
                String::from_utf8_lossy(&buf[..n]).into_owned();
            let _ = tls.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                )
                .as_bytes(),
            );
        }
    });
    (port, seen)
}

#[test]
fn fetch_rows_signed_passes_headers_and_decodes() {
    let (port, seen) = stub(r#"[{"id":42}]"#);
    let cols = vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }];
    let headers = vec![
        ("Authorization".to_string(), "AWS4-HMAC-SHA256 Test".to_string()),
        ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
    ];
    let rows = fetch_rows_signed(
        &format!("https://localhost:{port}/bucket/k.json"),
        &headers,
        Format::Json,
        &cols,
        None,
        DEFAULT_MAX_BODY,
    )
    .unwrap();
    assert_eq!(rows, vec![vec![vec![42, 0, 0, 0]]]);
    let req = seen.lock().unwrap().clone();
    assert!(req.contains("Authorization: AWS4-HMAC-SHA256 Test"), "{req}");
    assert!(req.contains("x-amz-date: 20130524T000000Z"), "{req}");
    assert!(req.starts_with("GET /bucket/k.json HTTP/1.1"), "{req}");
}

#[test]
fn fetch_rows_signed_non_https_is_typed_error() {
    let cols = vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }];
    let e = fetch_rows_signed(
        "http://localhost/x",
        &[],
        Format::Json,
        &cols,
        None,
        DEFAULT_MAX_BODY,
    )
    .unwrap_err();
    assert!(
        matches!(e, kessel_fetch::FetchError::Http(_)),
        "got {e:?}"
    );
}
```

- [ ] **Step 5: Implement `fetch_rows_signed`**

In `crates/kessel-fetch/src/lib.rs`, immediately after `fetch_rows_https_test` (the existing `#[cfg(feature="tls")]` fn), add:

```rust
/// Fetch a single object over HTTPS using caller-supplied (already
/// signed) request headers, then decode exactly like `fetch_rows`.
/// HTTPS-only (object storage is always TLS); reuses the production
/// rustls transport + `exchange` + `rows_from_body`. Used by the
/// router's object-store path (`s3://` / `az://`).
#[cfg(feature = "object-store")]
pub fn fetch_rows_signed(
    https_url: &str,
    headers: &[(String, String)],
    format: Format,
    cols: &[ColumnMap],
    rows_path: Option<&str>,
    max_body: u64,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    let (scheme, host, port, path) = http::parse_target(https_url)?;
    if scheme != http::Scheme::Https {
        return Err(FetchError::Http(
            "object-store fetch requires https://".into(),
        ));
    }
    let stream = tls::connect_tls(&host, port)?;
    let req = http::build_request_with_headers(&path, &host, headers);
    let (_h, body) = http::exchange(stream, &req, max_body)?;
    rows_from_body(&body, format, cols, rows_path)
}
```

(`http::parse_target`, `http::Scheme`, `http::build_request_with_headers`, `http::exchange`, `tls::connect_tls`, `rows_from_body` are all already `pub(crate)` / in-crate from SP97-99 + Step 2.)

- [ ] **Step 6: Run feature-on tests + default regression**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --features object-store --test objstore_stub -- --nocapture`
Expected: both PASS.
Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --test objstore_stub 2>&1 | grep "running 0 tests"`
Expected: `running 0 tests` (feature-gated out of the default build).
Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch && cargo test -p kessel-fetch --features tls`
Expected: all existing tests still PASS unchanged.

- [ ] **Step 7: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-fetch/Cargo.toml crates/kessel-fetch/src/http.rs crates/kessel-fetch/src/lib.rs crates/kessel-fetch/tests/objstore_stub.rs
git commit -m "objstore: kessel-fetch object-store feature + fetch_rows_signed (reuses tls transport)"
```

---

### Task 5: Catalog v3 trailer — `ObjStoreEnv` auth + region/endpoint

**Files:**
- Modify: `crates/kessel-catalog/src/lib.rs`

- [ ] **Step 1: Write the failing back-compat + round-trip test**

Add to `crates/kessel-catalog`'s test module (find it: `grep -n "mod tests" crates/kessel-catalog/src/lib.rs`):

```rust
    #[test]
    fn catalog_v3_objstore_roundtrip_and_v1v2_backcompat() {
        // v3 recipe with ObjStoreEnv + region + endpoint round-trips.
        let mut c = Catalog::default();
        c.next_type_id = 5;
        c.external.push(ExternalRecipe {
            type_id: 9,
            url: "s3://buck/data/x.json".into(),
            format: 0,
            key_field_id: 1,
            auth: ExternalAuth::ObjStoreEnv {
                provider: 1,
                a_env: "AWS_KEY_ID".into(),
                b_env: "AWS_SECRET".into(),
                account: None,
            },
            mapping: vec![(1, "id".into())],
            rows_path: Some("items".into()),
            pagination: None,
            region: Some("us-east-1".into()),
            endpoint: None,
        });
        let enc = c.encode();
        let dec = Catalog::decode(&enc).expect("decode v3");
        assert_eq!(dec.external, c.external);
        assert_eq!(dec.next_type_id, 5);

        // A recipe with NO objstore fields must encode byte-identically
        // to the pre-OBJ v2 layout (load-bearing back-compat: seed-7 /
        // existing digests unaffected). Build the SAME catalog the
        // pre-OBJ code would have and assert the prefix bytes are
        // unchanged through the end of the pagination tag.
        let mut c2 = Catalog::default();
        c2.next_type_id = 1;
        c2.external.push(ExternalRecipe {
            type_id: 3,
            url: "http://h/p".into(),
            format: 0,
            key_field_id: 1,
            auth: ExternalAuth::BearerEnv("TOK".into()),
            mapping: vec![(1, "id".into())],
            rows_path: None,
            pagination: None,
            region: None,
            endpoint: None,
        });
        let enc2 = c2.encode();
        // Hand-written expected v2 bytes for c2 (the EXACT pre-OBJ
        // encoding). next_type_id=1, types=0; then sentinel 0u32,
        // ver=2, n=1; type_id=3, format=0, key=1, url, auth=1+"TOK",
        // mapping(1)=(1,"id"), rows_path tag 0, pagination tag 0.
        let mut want = Vec::new();
        want.extend_from_slice(&1u32.to_le_bytes()); // next_type_id
        want.extend_from_slice(&0u32.to_le_bytes()); // types len
        want.extend_from_slice(&0u32.to_le_bytes()); // v2 sentinel
        want.push(2u8); // ver
        want.extend_from_slice(&1u32.to_le_bytes()); // n recipes
        want.extend_from_slice(&3u32.to_le_bytes()); // type_id
        want.push(0u8); // format
        want.extend_from_slice(&1u16.to_le_bytes()); // key_field_id
        want.extend_from_slice(&("http://h/p".len() as u32).to_le_bytes());
        want.extend_from_slice(b"http://h/p");
        want.push(1u8); // auth tag BearerEnv
        want.extend_from_slice(&("TOK".len() as u32).to_le_bytes());
        want.extend_from_slice(b"TOK");
        want.extend_from_slice(&1u32.to_le_bytes()); // mapping len
        want.extend_from_slice(&1u16.to_le_bytes()); // fid
        want.extend_from_slice(&("id".len() as u32).to_le_bytes());
        want.extend_from_slice(b"id");
        want.push(0u8); // rows_path tag
        want.push(0u8); // pagination tag
        assert_eq!(
            enc2, want,
            "a no-objstore recipe MUST stay byte-identical to v2"
        );
        assert_eq!(Catalog::decode(&enc2).unwrap().external, c2.external);
    }
```

- [ ] **Step 2: Run it — expect failure (compile: new fields/variant)**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-catalog catalog_v3_objstore -- --nocapture`
Expected: FAIL to compile (`ObjStoreEnv` / `region` / `endpoint` don't exist).

- [ ] **Step 3: Add the enum variant + recipe fields**

In `crates/kessel-catalog/src/lib.rs`, extend `ExternalAuth`:

```rust
pub enum ExternalAuth {
    None,
    BearerEnv(String),
    HeaderEnv { header: String, env: String },
    /// Object-store credentials by env-var NAME (resolved router-side
    /// at fetch time, never persisted/logged). provider 1=S3 (a=key
    /// -id env, b=secret env), 2=Azure (a=account-key env, b unused;
    /// `account` = the storage account).
    ObjStoreEnv {
        provider: u8,
        a_env: String,
        b_env: String,
        account: Option<String>,
    },
}
```

Add to `ExternalRecipe` (after `pagination`):

```rust
    /// S3 region (object-store sources). None ⇒ not object-store / Azure.
    pub region: Option<String>,
    /// Custom endpoint (S3-compatible path-style / custom Azure host).
    pub endpoint: Option<String>,
```

- [ ] **Step 4: Encode — v3 trailer (only when an objstore field is present, else byte-identical v2)**

In `Catalog::encode`, the recipe loop currently writes a `match &r.auth { … }` (tags 0/1/2) then mapping then rows_path tag then pagination tag. Make the trailer version conditional and append objstore data:

1. Replace the literal `b.push(2u8); // trailer version` with:

```rust
            // v3 iff ANY recipe uses object-store fields; otherwise the
            // bytes are byte-identical to the v2 layout (back-compat,
            // load-bearing for existing digests / seed-7).
            let need_v3 = self.external.iter().any(|r| {
                matches!(r.auth, ExternalAuth::ObjStoreEnv { .. })
                    || r.region.is_some()
                    || r.endpoint.is_some()
            });
            let ver: u8 = if need_v3 { 3 } else { 2 };
            b.push(ver);
```

2. In the `match &r.auth` add the new arm (after `HeaderEnv`):

```rust
                    ExternalAuth::ObjStoreEnv {
                        provider,
                        a_env,
                        b_env,
                        account,
                    } => {
                        b.push(3);
                        b.push(*provider);
                        put_str32(&mut b, a_env);
                        put_str32(&mut b, b_env);
                        match account {
                            None => b.push(0),
                            Some(a) => {
                                b.push(1);
                                put_str32(&mut b, a);
                            }
                        }
                    }
```

3. After the existing `match &r.pagination { … }` block (still inside the per-recipe loop), append:

```rust
                if ver == 3 {
                    match &r.region {
                        None => b.push(0),
                        Some(s) => {
                            b.push(1);
                            put_str32(&mut b, s);
                        }
                    }
                    match &r.endpoint {
                        None => b.push(0),
                        Some(s) => {
                            b.push(1);
                            put_str32(&mut b, s);
                        }
                    }
                }
```

(With `ver==2` — no objstore anywhere — not a single new byte is emitted ⇒ byte-identical to pre-OBJ.)

- [ ] **Step 5: Decode — accept ver 2 AND 3, new auth tag 3, trailing region/endpoint**

In `Catalog::decode`'s trailer section:

1. Replace the unknown-version guard:

```rust
                if ver != 2 {
                    return None;
                }
```

with:

```rust
                if ver != 2 && ver != 3 {
                    return None;
                }
```

and capture `is_v3`:

```rust
                let is_v3 = ver == 3;
```

(Keep `is_v2` true for both 2 and 3 — the rows_path/pagination block is shared. Rename the local if clearer: set `let is_v2 = true;` for the sentinel branch as today; add `is_v3` alongside.)

2. In the `auth = match tag { … }` add (before the `_ => return None` arm):

```rust
                    3 => {
                        let provider = *b.get(p)?;
                        p += 1;
                        let a_env = get_str32(b, &mut p)?;
                        let b_env = get_str32(b, &mut p)?;
                        let acc_tag = *b.get(p)?;
                        p += 1;
                        let account = match acc_tag {
                            0 => None,
                            1 => Some(get_str32(b, &mut p)?),
                            _ => return None,
                        };
                        ExternalAuth::ObjStoreEnv {
                            provider,
                            a_env,
                            b_env,
                            account,
                        }
                    }
```

3. After the `(rows_path, pagination)` is computed and before `out.push(ExternalRecipe { … })`, read region/endpoint:

```rust
                let (region, endpoint) = if is_v3 {
                    let rt = *b.get(p)?;
                    p += 1;
                    let region = match rt {
                        0 => None,
                        1 => Some(get_str32(b, &mut p)?),
                        _ => return None,
                    };
                    let et = *b.get(p)?;
                    p += 1;
                    let endpoint = match et {
                        0 => None,
                        1 => Some(get_str32(b, &mut p)?),
                        _ => return None,
                    };
                    (region, endpoint)
                } else {
                    (None, None)
                };
```

4. Add `region,` and `endpoint,` to the `ExternalRecipe { … }` constructor (and to the non-trailer / v1 backward-compat construction path if there is a separate one — search for every `ExternalRecipe {` literal in the file and add the two fields = `None` where the recipe is built from v1/older bytes).

- [ ] **Step 6: Fix all other `ExternalRecipe { … }` constructors in the workspace**

Run: `cd /c/Users/ihass/KesselDB && grep -rn "ExternalRecipe {" crates/ | grep -v test`
For each non-test construction site (notably `kessel-sm`), add `region: None, endpoint: None,` (these are set only via the catalog trailer / SM apply in Task 7). Do NOT change behavior — just satisfy the new fields with `None`.

- [ ] **Step 7: Run catalog + workspace gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-catalog`
Expected: PASS incl. the new test (v3 round-trip + byte-identical v2 back-compat).
Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: 0 failed; seed-7 ok; summed total == **BASELINE + N** where N = the number of new **default-build** tests added here (this catalog test runs in the default build → N≥1). Record the exact new total; T12 reconciles README/STATUS. (This is an unavoidable default-build test — the back-compat invariant MUST be guarded in the default build. State it explicitly in the commit body.)

- [ ] **Step 8: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-catalog/src/lib.rs crates/kessel-sm/src/lib.rs
git commit -m "objstore: catalog v3 trailer (ObjStoreEnv auth + region/endpoint, v1/v2 byte-identical)"
```

---

### Task 6: `kessel-proto` additive `objstore` field (tolerant decode)

**Files:**
- Modify: `crates/kessel-proto/src/lib.rs`

- [ ] **Step 1: Write the failing test (round-trip + old-frame back-compat)**

Add to `kessel-proto`'s test module (`grep -n "mod tests" crates/kessel-proto/src/lib.rs`):

```rust
    #[test]
    fn create_external_source_objstore_additive_backcompat() {
        // New op with objstore set round-trips.
        let op = Op::CreateExternalSource {
            name: "s".into(),
            type_def: vec![1, 2, 3],
            url: "s3://b/k.json".into(),
            format: 0,
            key_field_id: 1,
            auth_kind: 3,
            auth_a: "AWS_ID".into(),
            auth_b: "AWS_SECRET".into(),
            mapping: vec![(1, "id".into())],
            rows_path: None,
            pagination: None,
            objstore: Some((1, "acct".into(), "us-east-1".into(), "".into())),
        };
        let enc = op.encode();
        assert_eq!(Op::decode(&enc).unwrap(), op);

        // An OLD pagination-era frame (no objstore trailer) decodes
        // with objstore = None (tolerant, WAL-replay critical).
        let old = Op::CreateExternalSource {
            name: "s".into(),
            type_def: vec![1, 2, 3],
            url: "http://h".into(),
            format: 0,
            key_field_id: 1,
            auth_kind: 1,
            auth_a: "TOK".into(),
            auth_b: String::new(),
            mapping: vec![(1, "id".into())],
            rows_path: None,
            pagination: None,
            objstore: None,
        };
        let mut frame = old.encode();
        // Truncate the trailing objstore tag byte to simulate a frame
        // produced by the pre-OBJ build (which never wrote it).
        assert_eq!(*frame.last().unwrap(), 0u8, "objstore tag is last");
        frame.pop();
        let dec = Op::decode(&frame).expect("old frame decodes");
        assert_eq!(dec, old);
    }
```

- [ ] **Step 2: Run it — expect failure (no `objstore` field)**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-proto create_external_source_objstore_additive -- --nocapture`
Expected: FAIL to compile.

- [ ] **Step 3: Add the field + encode + tolerant decode**

In the `Op::CreateExternalSource { … }` variant declaration (around the documented fields), add after `pagination`:

```rust
        /// Object-store extras `(provider, account, region, endpoint)`.
        /// provider 1=S3 / 2=Azure; account/region/endpoint may be
        /// empty strings. `None` = not an object-store source / older
        /// frame (tolerant decode — absent ⇒ None, never a failure).
        objstore: Option<(u8, String, String, String)>,
```

In `encode`, the `Op::CreateExternalSource { … }` arm: add `objstore,` to the destructure and, immediately after the `match pagination { … }` block, append:

```rust
                // Additive (OBJ-1): None ⇒ one trailing 0 tag byte; an
                // OLD frame has neither and the tolerant decode treats
                // its absence as None.
                match objstore {
                    None => b.push(0),
                    Some((prov, acct, region, endpoint)) => {
                        b.push(1);
                        b.push(*prov);
                        codec::put_bytes(&mut b, acct.as_bytes());
                        codec::put_bytes(&mut b, region.as_bytes());
                        codec::put_bytes(&mut b, endpoint.as_bytes());
                    }
                }
```

In `decode`, after the `pagination` match (the `Some(_) => return None` arm at ~line 818) and before `Op::CreateExternalSource { … }` is constructed, add:

```rust
                let objstore = match c.u8() {
                    None | Some(0) => None,
                    Some(1) => {
                        let prov = c.u8()?;
                        let acct =
                            String::from_utf8_lossy(&c.bytes()?).into_owned();
                        let region =
                            String::from_utf8_lossy(&c.bytes()?).into_owned();
                        let endpoint =
                            String::from_utf8_lossy(&c.bytes()?).into_owned();
                        Some((prov, acct, region, endpoint))
                    }
                    // Unknown PRESENT tag ⇒ fail (matches the rows_path
                    // / pagination stance; an EXHAUSTED cursor (None)
                    // stays the slice-1/None default).
                    Some(_) => return None,
                };
```

and add `objstore,` to the `Op::CreateExternalSource { … }` constructor.

- [ ] **Step 4: Fix every other `Op::CreateExternalSource { … }` site**

Run: `cd /c/Users/ihass/KesselDB && grep -rn "Op::CreateExternalSource" crates/ | grep -v "crates/kessel-proto/src/lib.rs"`
Every construction site (kessel-sql Task 8 will set it; kessel-sm/router/tests destructure) must compile. For **constructors**, add `objstore: None,` now (kessel-sql is updated in Task 8 to set it properly). For **destructures** that use `{ … , .. }` no change is needed; for exhaustive destructures add `objstore`. Specifically check `crates/kessel-sql/src/lib.rs` (the `return Ok(Op::CreateExternalSource { … })` — add `objstore: None,` here as a placeholder; Task 8 replaces it), `crates/kessel-sm/src/lib.rs`, `crates/kesseldb-server/src/router.rs`, and any proto/sm tests.

- [ ] **Step 5: Run proto + workspace gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-proto`
Expected: PASS incl. the new additive/back-compat test.
Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: 0 failed; seed-7 ok. Record the new default total (this proto test is default-build → +1; combined with Task 5 track the running default delta for T12).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-proto/src/lib.rs crates/kessel-sql/src/lib.rs crates/kessel-sm/src/lib.rs crates/kesseldb-server/src/router.rs
git commit -m "objstore: additive Op::CreateExternalSource objstore field (tolerant decode)"
```

---

### Task 7: `kessel-sm` apply — map auth_kind 3 + objstore tuple → recipe

**Files:**
- Modify: `crates/kessel-sm/src/lib.rs`

- [ ] **Step 1: Locate the CreateExternalSource apply arm**

Run: `cd /c/Users/ihass/KesselDB && grep -n "CreateExternalSource\|ExternalRecipe\|ExternalAuth\|auth_kind\|pagination" crates/kessel-sm/src/lib.rs | head -30`
Read the arm that builds an `ExternalRecipe` from the op (it currently maps `auth_kind` 0/1/2 → `ExternalAuth` and `pagination` tuple → `PaginationRecipe`).

- [ ] **Step 2: Write the failing test**

Add to `kessel-sm`'s tests (mirror the existing CreateExternalSource apply test; `grep -n "fn .*external" crates/kessel-sm/src/lib.rs`):

```rust
    #[test]
    fn apply_create_external_source_objstore_recipe() {
        let mut sm = StateMachine::default();
        // (Use the same construction the existing external-source
        // apply test uses; only the auth/objstore fields differ.)
        let op = kessel_proto::Op::CreateExternalSource {
            name: "feed".into(),
            type_def: kessel_catalog::encode_type_def(
                "feed",
                &[kessel_catalog::Field {
                    field_id: 1,
                    name: "id".into(),
                    kind: kessel_catalog::FieldKind::U64,
                    nullable: false,
                }],
            ),
            url: "s3://bucket/data.json".into(),
            format: 0,
            key_field_id: 1,
            auth_kind: 3,
            auth_a: "AWS_KEY_ID".into(),
            auth_b: "AWS_SECRET".into(),
            mapping: vec![(1, "id".into())],
            rows_path: None,
            pagination: None,
            objstore: Some((
                1,
                String::new(),
                "us-east-1".into(),
                String::new(),
            )),
        };
        let _ = sm.apply(&op);
        let cat = kessel_catalog::Catalog::decode(
            &sm.read_catalog_blob(),
        )
        .unwrap();
        let r = cat
            .external
            .iter()
            .find(|r| r.url == "s3://bucket/data.json")
            .expect("recipe present");
        assert_eq!(
            r.auth,
            kessel_catalog::ExternalAuth::ObjStoreEnv {
                provider: 1,
                a_env: "AWS_KEY_ID".into(),
                b_env: "AWS_SECRET".into(),
                account: None,
            }
        );
        assert_eq!(r.region.as_deref(), Some("us-east-1"));
        assert_eq!(r.endpoint, None);
    }
```

> **Implementer note:** adapt `StateMachine::default()` / `sm.apply` / `read_catalog_blob` to the ACTUAL kessel-sm test harness API used by the existing external-source apply test (read it first; mirror it exactly — do not invent helpers). If `account`/`region`/`endpoint` empty-string vs `None` mapping differs, follow the rule in Step 3.

- [ ] **Step 3: Map the op fields in the apply arm**

In the CreateExternalSource apply arm, where `ExternalAuth` is built from `auth_kind`, add the `3 =>` case and thread `objstore`:

```rust
        let (region, endpoint, obj_account) = match objstore {
            None => (None, None, None),
            Some((_prov, acct, region, endpoint)) => (
                (!region.is_empty()).then(|| region.clone()),
                (!endpoint.is_empty()).then(|| endpoint.clone()),
                (!acct.is_empty()).then(|| acct.clone()),
            ),
        };
        let auth = match auth_kind {
            0 => ExternalAuth::None,
            1 => ExternalAuth::BearerEnv(auth_a.clone()),
            2 => ExternalAuth::HeaderEnv {
                header: auth_a.clone(),
                env: auth_b.clone(),
            },
            3 => ExternalAuth::ObjStoreEnv {
                provider: objstore
                    .as_ref()
                    .map(|(p, _, _, _)| *p)
                    .unwrap_or(1),
                a_env: auth_a.clone(),
                b_env: auth_b.clone(),
                account: obj_account.clone(),
            },
            _ => {
                // Unknown auth kind: reject the op (pre-mutation, like
                // the existing bad-auth handling — do NOT orphan a
                // type). Mirror the existing error path exactly.
                return /* existing SM error result for bad CreateExternalSource */;
            }
        };
```

and set `region` / `endpoint` on the `ExternalRecipe { … }` built here (replace the `region: None, endpoint: None,` placeholder from Task 5/6 with `region, endpoint,`).

> Match the existing arm's exact error-return shape for the `_ =>` case (read the current `auth_kind` match — it already has a 0/1/2 mapping; replicate its pre-mutation-ordering / error type precisely so a bad op does not half-mutate the catalog, per the SP97 C1/I1 invariant).

- [ ] **Step 4: Run sm + workspace gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-sm`
Expected: PASS incl. the new test; all existing external-source apply tests unchanged.
Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: 0 failed, seed-7 ok; record running default total.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-sm/src/lib.rs
git commit -m "objstore: SM apply maps auth_kind 3 + objstore tuple to recipe (pre-mutation safe)"
```

---

### Task 8: `kessel-sql` grammar — `s3://`/`az://`, REGION, ENDPOINT, AUTH OBJSTORE, rejections

**Files:**
- Modify: `crates/kessel-sql/src/lib.rs`

- [ ] **Step 1: Write the failing parse + rejection tests**

Add to `kessel-sql` tests (near `parse_create_external_source`):

```rust
    #[test]
    fn parse_external_source_objstore_s3() {
        let cat = Catalog::default();
        let op = compile(
            "CREATE EXTERNAL SOURCE feed (id U64 NOT NULL FROM 'id') \
             FROM 's3://bucket/data/x.json' FORMAT JSON KEY id \
             REGION 'us-east-1' \
             AUTH OBJSTORE S3 KEYID ENV 'AWS_ID' SECRET ENV 'AWS_SEC'",
            &cat,
        )
        .unwrap();
        match op {
            Op::CreateExternalSource {
                url,
                auth_kind,
                auth_a,
                auth_b,
                objstore,
                ..
            } => {
                assert_eq!(url, "s3://bucket/data/x.json");
                assert_eq!(auth_kind, 3);
                assert_eq!(auth_a, "AWS_ID");
                assert_eq!(auth_b, "AWS_SEC");
                assert_eq!(
                    objstore,
                    Some((1, String::new(), "us-east-1".into(), String::new()))
                );
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn parse_external_source_objstore_azure_and_endpoint() {
        let cat = Catalog::default();
        let op = compile(
            "CREATE EXTERNAL SOURCE f (id U64 NOT NULL FROM 'id') \
             FROM 'az://cont/blob.csv' FORMAT CSV KEY id \
             ENDPOINT 'https://acct.blob.core.windows.net' \
             AUTH OBJSTORE AZURE ACCOUNT 'acct' KEY ENV 'AZ_KEY'",
            &cat,
        )
        .unwrap();
        match op {
            Op::CreateExternalSource {
                url, auth_kind, auth_a, objstore, ..
            } => {
                assert_eq!(url, "az://cont/blob.csv");
                assert_eq!(auth_kind, 3);
                assert_eq!(auth_a, "AZ_KEY"); // a_env = account-key env
                assert_eq!(
                    objstore,
                    Some((
                        2,
                        "acct".into(),
                        String::new(),
                        "https://acct.blob.core.windows.net".into()
                    ))
                );
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn objstore_rejections_at_create() {
        let cat = Catalog::default();
        let bad = |sql: &str| compile(sql, &cat).unwrap_err();
        // PARQUET over object store (OBJ-2, not shipped).
        assert!(bad(
            "CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
             FROM 's3://b/k' FORMAT PARQUET KEY id REGION 'r' \
             AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S'"
        )
        .contains("Parquet"));
        // PAGE over object store.
        assert!(bad(
            "CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
             FROM 's3://b/k' FORMAT JSON KEY id REGION 'r' \
             AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S' \
             PAGE NEXT LINK"
        )
        .to_lowercase()
        .contains("object store"));
        // http:// ENDPOINT rejected.
        assert!(bad(
            "CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
             FROM 's3://b/k' FORMAT JSON KEY id REGION 'r' \
             ENDPOINT 'http://x' \
             AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S'"
        )
        .to_lowercase()
        .contains("https"));
        // s3:// without OBJSTORE auth rejected.
        assert!(bad(
            "CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
             FROM 's3://b/k' FORMAT JSON KEY id REGION 'r'"
        )
        .to_lowercase()
        .contains("auth objstore"));
        // s3:// without REGION and without ENDPOINT rejected.
        assert!(bad(
            "CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
             FROM 's3://b/k' FORMAT JSON KEY id \
             AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S'"
        )
        .to_lowercase()
        .contains("region"));
        // az:// with BOTH account and endpoint rejected (exactly one).
        assert!(bad(
            "CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
             FROM 'az://c/b' FORMAT JSON KEY id \
             ENDPOINT 'https://h' \
             AUTH OBJSTORE AZURE ACCOUNT 'acct' KEY ENV 'K'"
        )
        .to_lowercase()
        .contains("exactly one"));
        // Existing http:// path still works (regression).
        assert!(compile(
            "CREATE EXTERNAL SOURCE ok (id U64 NOT NULL FROM 'id') \
             FROM 'http://h/p' FORMAT JSON KEY id AUTH BEARER ENV 'T'",
            &cat
        )
        .is_ok());
    }
```

- [ ] **Step 2: Run — expect failure**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-sql objstore -- --nocapture`
Expected: FAIL (grammar not implemented; `objstore` field unset).

- [ ] **Step 3: Implement grammar + validation**

In the CREATE EXTERNAL SOURCE block of `crates/kessel-sql/src/lib.rs` (the code read at lines 688-803):

After `let url = …;` compute the scheme and add OBJ parsing. Replace the segment from the `let (mut auth_kind, …)` declaration through the `let type_def = encode_type_def(&name, &fields);` with logic that:

1. After `FORMAT`/`KEY` parsing, parse optional `REGION '<r>'` and `ENDPOINT '<url>'`:

```rust
            let is_obj = url.starts_with("s3://") || url.starts_with("az://");
            let is_s3 = url.starts_with("s3://");
            let mut region: Option<String> = None;
            let mut endpoint: Option<String> = None;
            if p.kw("REGION") {
                region = Some(match p.next() {
                    Some(Tok::Str(s)) => s,
                    _ => return Err("expected 'region' string after REGION".into()),
                });
            }
            if p.kw("ENDPOINT") {
                endpoint = Some(match p.next() {
                    Some(Tok::Str(s)) => s,
                    _ => return Err("expected 'endpoint' url after ENDPOINT".into()),
                });
            }
```

2. Extend the `if p.kw("AUTH") { … }` to accept `OBJSTORE`:

```rust
                } else if p.kw("OBJSTORE") {
                    auth_kind = 3;
                    if p.kw("S3") {
                        p.expect_kw("KEYID")?;
                        p.expect_kw("ENV")?;
                        auth_a = match p.next() {
                            Some(Tok::Str(s)) => s,
                            _ => return Err("expected 'KEYID_ENV'".into()),
                        };
                        p.expect_kw("SECRET")?;
                        p.expect_kw("ENV")?;
                        auth_b = match p.next() {
                            Some(Tok::Str(s)) => s,
                            _ => return Err("expected 'SECRET_ENV'".into()),
                        };
                        obj = Some((1u8, String::new()));
                    } else if p.kw("AZURE") {
                        p.expect_kw("ACCOUNT")?;
                        let acct = match p.next() {
                            Some(Tok::Str(s)) => s,
                            _ => return Err("expected 'account'".into()),
                        };
                        p.expect_kw("KEY")?;
                        p.expect_kw("ENV")?;
                        auth_a = match p.next() {
                            Some(Tok::Str(s)) => s,
                            _ => return Err("expected 'ACCOUNT_KEY_ENV'".into()),
                        };
                        obj = Some((2u8, acct));
                    } else {
                        return Err("AUTH OBJSTORE must be S3 …| AZURE …".into());
                    }
```

(Declare `let mut obj: Option<(u8, String)> = None;` next to `auth_kind`.)

3. After `PAGE` parsing and the existing compatibility matrix, add object-store validation + build the `objstore` tuple:

```rust
            // Object-store CREATE-time validation (boundary stays honest).
            let objstore: Option<(u8, String, String, String)> = if is_obj {
                if format == 3 /* PARQUET, see Step 4 */ {
                    return Err("FORMAT PARQUET over object store is OBJ-2 (not yet shipped)".into());
                }
                if pagination.is_some() {
                    return Err("PAGE clauses are not supported for object-store (s3://|az://) sources".into());
                }
                if let Some(ep) = &endpoint {
                    if !ep.starts_with("https://") {
                        return Err("object-store ENDPOINT must be https://".into());
                    }
                }
                let (prov, acct) = obj.ok_or(
                    "object-store (s3://|az://) requires AUTH OBJSTORE S3 …|AZURE …",
                )?;
                if is_s3 && region.is_none() && endpoint.is_none() {
                    return Err("S3 (s3://) source requires REGION '<r>' (or an ENDPOINT)".into());
                }
                if !is_s3 {
                    // az://: exactly one of ACCOUNT (non-empty) / ENDPOINT.
                    let has_acct = !acct.is_empty();
                    let has_ep = endpoint.is_some();
                    if has_acct == has_ep {
                        return Err("az:// requires exactly one of AUTH OBJSTORE AZURE ACCOUNT '<a>' or ENDPOINT '<url>'".into());
                    }
                }
                Some((
                    prov,
                    acct,
                    region.clone().unwrap_or_default(),
                    endpoint.clone().unwrap_or_default(),
                ))
            } else {
                if obj.is_some() {
                    return Err("AUTH OBJSTORE is only valid for s3://|az:// sources".into());
                }
                None
            };
```

4. Add `objstore,` to the returned `Op::CreateExternalSource { … }` (replacing the Task-6 `objstore: None` placeholder).

- [ ] **Step 4: Reserve `FORMAT PARQUET` token (rejected, honest boundary)**

In the `FORMAT` parse (`if p.kw("JSON") {0} else if p.kw("CSV") {1} else if p.kw("NDJSON") {2}`) add a branch:

```rust
            } else if p.kw("PARQUET") {
                3u8
            } else {
```

(Code `3` is parsed so the OBJ validation can emit the precise "OBJ-2 not yet shipped" error; do_refresh/SM still only accept 0/1/2 — a non-object PARQUET source is rejected by the existing `unknown format code` path, and object PARQUET is rejected at CREATE in Step 3. Add a non-object guard too: if `format==3 && !is_obj` return `Err("FORMAT PARQUET is not supported (OBJ-2)".into())`.)

- [ ] **Step 5: Run sql + workspace gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-sql`
Expected: PASS incl. all new tests AND all existing CREATE EXTERNAL SOURCE / pagination tests unchanged.
Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: 0 failed, seed-7 ok; record running default total.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-sql/src/lib.rs
git commit -m "objstore: SQL grammar (s3://|az://, REGION, ENDPOINT, AUTH OBJSTORE) + CREATE-time rejections"
```

---

### Task 9: Router `do_refresh` object-store dispatch (feature-gated)

**Files:**
- Modify: `crates/kesseldb-server/src/router.rs`, `crates/kesseldb-server/Cargo.toml`

- [ ] **Step 1: Add the composite feature**

In `crates/kesseldb-server/Cargo.toml` `[features]`, add after `external-sources-tls`:

```toml
external-sources-objstore = ["external-sources", "kessel-fetch/object-store", "dep:rustls", "dep:rustls-pemfile"]
```

(`external-sources`, `external-sources-tls`, `tls`, `default` unchanged.)

- [ ] **Step 2: Implement the scheme dispatch in `do_refresh`**

In `crates/kesseldb-server/src/router.rs` `do_refresh`, the credential resolution (`let auth = match &recipe.auth { … }`) currently handles None/BearerEnv/HeaderEnv. The fetch step later calls `fetch_rows`/`fetch_rows_paginated`. Add an object-store branch that **replaces** the fetch for `s3://`/`az://` recipes. After `recipe` is resolved and before the existing `auth`/`format` resolution, add:

```rust
        // Object-store (s3://|az://): resolve creds router-side by env
        // NAME (never values in op/WAL/log/digest/error), sign, fetch.
        // Feature-gated; without `external-sources-objstore` an
        // object-store URL is a clean typed error (no panic, no
        // plaintext, fail-closed) surfaced at REFRESH.
        let is_obj = recipe.url.starts_with("s3://")
            || recipe.url.starts_with("az://");
        if is_obj {
            #[cfg(feature = "external-sources-objstore")]
            {
                return self.do_refresh_objstore(&recipe, &ot, &name, dedup);
            }
            #[cfg(not(feature = "external-sources-objstore"))]
            {
                let _ = &dedup;
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: object-store sources require the \
                     external-sources-objstore build feature"
                ));
            }
        }
```

Add the feature-gated method (place it next to `do_refresh`; reuse the existing column-map build + ObjectId/Txn tail by factoring is out-of-scope — instead this method builds cols + signs + fetches then calls the SAME submission helper the main path uses). Implement it mirroring the existing post-fetch logic precisely:

```rust
    #[cfg(feature = "external-sources-objstore")]
    fn do_refresh_objstore(
        &mut self,
        recipe: &kessel_catalog::ExternalRecipe,
        ot: &kessel_catalog::ObjectType,
        name: &str,
        dedup: Vec<u8>,
    ) -> OpResult {
        use kessel_catalog::ExternalAuth;
        use kessel_fetch::{ColumnMap, Format};
        use kessel_objstore::{
            sign_get, DateTime, ObjCreds, ObjGetRequest, Provider,
        };

        // 1. Parse `s3://bucket/key` | `az://container/blob`.
        let (prov, scheme_len) = if recipe.url.starts_with("s3://") {
            (Provider::S3, 5)
        } else {
            (Provider::Azure, 5)
        };
        let rest = &recipe.url[scheme_len..];
        let (b_or_c, key) = match rest.split_once('/') {
            Some((b, k)) => (b.to_string(), k.to_string()),
            None => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: object URL must be \
                     <scheme>://<bucket-or-container>/<key>"
                ))
            }
        };

        // 2. Resolve credentials router-side by env NAME only.
        let creds = match &recipe.auth {
            ExternalAuth::ObjStoreEnv {
                provider,
                a_env,
                b_env,
                account,
            } => {
                let getenv = |k: &str| std::env::var(k).map_err(|_| {
                    OpResult::SchemaError(format!(
                        "REFRESH `{name}`: env `{k}` not set"
                    ))
                });
                if *provider == 1 {
                    let key_id = match getenv(a_env) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    let secret = match getenv(b_env) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    ObjCreds::S3 { key_id, secret }
                } else {
                    let key_b64 = match getenv(a_env) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    ObjCreds::AzureSharedKey {
                        account: account.clone().unwrap_or_default(),
                        key_b64,
                    }
                }
            }
            _ => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: object-store source missing \
                     OBJSTORE credentials"
                ))
            }
        };

        // 3. Sign (now = wall clock; non-deterministic but captured
        //    once at the router, never in WAL/digest — same boundary
        //    as the TLS handshake RNG, SP99).
        let now = DateTime {
            secs_since_epoch: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        let signed = match sign_get(
            &ObjGetRequest {
                provider: prov,
                bucket_or_container: b_or_c,
                key,
                region: recipe.region.clone(),
                endpoint: recipe.endpoint.clone(),
                creds,
            },
            now,
        ) {
            Ok(s) => s,
            // ObjError carries NO secret material.
            Err(e) => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: sign: {e}"
                ))
            }
        };

        // 4. Column map (verbatim from the existing path).
        let mut cols: Vec<ColumnMap> =
            Vec::with_capacity(recipe.mapping.len());
        for (fid, source) in &recipe.mapping {
            let field = match ot.fields.iter().find(|f| f.field_id == *fid) {
                Some(f) => f,
                None => {
                    return OpResult::SchemaError(format!(
                        "REFRESH `{name}`: mapping references unknown \
                         field_id {fid}"
                    ))
                }
            };
            cols.push(ColumnMap {
                name: field.name.clone(),
                kind: field.kind,
                source: source.clone(),
            });
        }
        let format = match recipe.format {
            0 => Format::Json,
            1 => Format::Csv,
            2 => Format::Ndjson,
            n => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: unknown format code {n}"
                ))
            }
        };

        // 5. Fetch the signed object, then the EXACT existing
        //    materialization (deterministic ObjectId + atomic Op::Txn
        //    + fail-closed). Call the shared submission helper used by
        //    the http path so id/codec/Txn/all-or-nothing is identical.
        let rows = match kessel_fetch::fetch_rows_signed(
            &signed.https_url,
            &signed.headers,
            format,
            &cols,
            recipe.rows_path.as_deref(),
            kessel_fetch::DEFAULT_MAX_BODY,
        ) {
            Ok(r) => r,
            Err(e) => {
                return OpResult::SchemaError(format!("refresh: {e}"))
            }
        };
        self.materialize_external_rows(recipe, ot, name, &cols, rows, dedup)
    }
```

> **Implementer note:** the existing `do_refresh` builds the ObjectId/`Op::Txn`/all-or-nothing tail inline (read it: router.rs ~660–end). Factor that tail (everything from "Build the codec record + deterministic ObjectId per row" through the `Op::Txn` submission) into a **private helper** `fn materialize_external_rows(&mut self, recipe, ot, name, cols, rows: Vec<Vec<Vec<u8>>>, dedup) -> OpResult` and call it from BOTH the existing http path and `do_refresh_objstore`. This is a pure extraction (no behavior change) — the existing EXT/TLS oracles are the regression net and MUST stay green. If extraction is risky, instead have `do_refresh_objstore` reuse the http path by stuffing the signed URL/headers — but the extraction is the clean, DRY choice and aligns with the SP99-deferred "unify fetch_rows_paginated decode" stance. Keep the extraction its own concern within this task; verify byte-identical behavior via the existing `external_source_oracle` + `external_source_tls_oracle`.

- [ ] **Step 3: Build matrix**

Run: `cd /c/Users/ihass/KesselDB && cargo build -p kesseldb-server && cargo build -p kesseldb-server --features external-sources && cargo build -p kesseldb-server --features external-sources-tls && cargo build -p kesseldb-server --features external-sources-objstore 2>&1 | tail -3`
Expected: all four compile. (Default + each feature.)

- [ ] **Step 4: Existing oracles must stay green (regression — the extraction safety net)**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources --test external_source_oracle && cargo test -p kesseldb-server --features external-sources-tls --test external_source_tls_oracle`
Expected: all PASS unchanged (proves `materialize_external_rows` extraction is behavior-identical).

- [ ] **Step 5: Workspace gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: 0 failed; seed-7 ok; default total == running total (no NEW default-build test in this task).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kesseldb-server/Cargo.toml crates/kesseldb-server/src/router.rs
git commit -m "objstore: do_refresh s3://|az:// dispatch + external-sources-objstore feature"
```

---

### Task 10: Server e2e — REFRESH from `s3://` against a localhost S3-emulating stub

**Files:**
- Create: `crates/kesseldb-server/tests/external_source_objstore_oracle.rs`

- [ ] **Step 1: Write the e2e test**

Create `crates/kesseldb-server/tests/external_source_objstore_oracle.rs` (mirror `external_source_tls_oracle.rs` exactly for the shard/router/stub harness — read that file and reuse its `spawn_shard` + rustls stub verbatim; only the recipe + assertions differ):

```rust
//! End-to-end: `REFRESH` of an `s3://` source. A localhost rustls
//! stub emulates S3 — it asserts the inbound request carries a
//! well-formed SigV4 `Authorization` + `x-amz-date` + `host`, then
//! serves a fixed JSON body. Proves do_refresh → kessel_objstore
//! sign → kessel_fetch::fetch_rows_signed → deterministic Txn.
//! Only compiled with `--features external-sources-objstore`.
#![cfg(feature = "external-sources-objstore")]

// ... reuse spawn_shard + the rustls stub from external_source_tls_oracle.rs
// (copy that harness; the stub closure additionally asserts the
//  request line contains "Authorization: AWS4-HMAC-SHA256 " and
//  "x-amz-date:" before writing the body) ...

#[test]
fn refresh_from_s3_via_path_style_endpoint_materializes_rows() {
    // Set the credential env vars (NAMES are what the recipe persists;
    // values live only in this process env, exactly the SP97 model).
    std::env::set_var("OBJ_TEST_KEYID", "AKIAEXAMPLE");
    std::env::set_var("OBJ_TEST_SECRET", "secretkeyexample");

    // Stub on https://127.0.0.1:<port>; serve [{"id":7,"nm":"zed"}].
    // (rustls stub from the tls oracle; assert Authorization shape.)
    let (port /*, ... */,) = /* start_s3_stub(r#"[{"id":7,"nm":"zed"}]"#) */;

    // 3-node shard + router (verbatim spawn_shard/serve_router).
    // CREATE EXTERNAL SOURCE via shard client:
    let ddl = format!(
        "CREATE EXTERNAL SOURCE feed (\
           id U64 NOT NULL FROM 'id', nm CHAR(16) NOT NULL FROM 'nm'\
         ) FROM 's3://bucket/data.json' FORMAT JSON KEY id \
         REGION 'us-east-1' \
         ENDPOINT 'https://127.0.0.1:{port}' \
         AUTH OBJSTORE S3 KEYID ENV 'OBJ_TEST_KEYID' SECRET ENV 'OBJ_TEST_SECRET'"
    );
    // assert CREATE Ok|TypeCreated;
    // REFRESH via router client → assert OpResult::Ok (materialized);
    // SELECT * FROM feed → exactly {(7,"zed")};
    // The stub MUST have asserted a well-formed SigV4 Authorization
    // header (fail the test from the stub thread otherwise).
    //
    // Negative: unset OBJ_TEST_SECRET, REFRESH again → fail-closed
    // SchemaError whose message does NOT contain the secret value;
    // SELECT unchanged.
}
```

> **Implementer:** flesh this out by copying `external_source_tls_oracle.rs`'s harness in full (it is the proven pattern from SP99). The stub uses the SP99 fixture cert (`crates/kessel-fetch/tests/fixtures/localhost.pem`) via `include_bytes!("../../kessel-fetch/tests/fixtures/localhost.pem")`. The recipe uses `ENDPOINT 'https://127.0.0.1:<port>'` so the SigV4 path-style URL targets the stub and rustls verifies the localhost fixture is **not** webpki-trusted — therefore, like the SP99 server e2e, the production fetch will **fail closed** on the cert. ACCORDINGLY assert the **fail-closed** outcome (REFRESH → `OpResult::SchemaError`, message contains `refresh:`, SELECT empty) — the *trusted* happy path (signing correctness + header passthrough) is already proven at the kessel-fetch layer by `objstore_stub.rs` (Task 4) which trusts the fixture. Do NOT inject fixture trust into the production router (forbidden bypass, SP99 precedent). The stub still asserts the SigV4 `Authorization` header shape on the bytes it receives during the handshake-completed request (if the handshake fails before bytes arrive, assert the REFRESH SchemaError instead — match the SP99 oracle's fail-closed structure exactly).

- [ ] **Step 2: Run the e2e**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_objstore_oracle -- --nocapture`
Expected: PASS (fail-closed REFRESH SchemaError + SELECT empty, mirroring the SP99 tls oracle; no panic).

- [ ] **Step 3: Gated out of non-objstore builds**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources --test external_source_objstore_oracle 2>&1 | grep "running 0 tests"`
Expected: `running 0 tests`.

- [ ] **Step 4: Full workspace gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: 0 failed; seed-7 ok; default total unchanged from Task 8's running total (this test is feature-gated).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kesseldb-server/tests/external_source_objstore_oracle.rs
git commit -m "objstore: feature-gated s3:// REFRESH e2e (SigV4 header-shape + fail-closed)"
```

---

### Task 11: Pentest pass — object-store path hardening

**Files:** (only if a real issue is found) the relevant crate; otherwise a findings note in the T12 record.

- [ ] **Step 1: Secret-leak audit**

Run: `cd /c/Users/ihass/KesselDB && grep -rn "secret\|key_id\|key_b64\|SECRET\|ACCOUNT_KEY\|env::var" crates/kessel-objstore crates/kessel-fetch/src/lib.rs crates/kesseldb-server/src/router.rs`
Manually verify: (a) no credential VALUE is ever `format!`-ed into an `OpResult`/`SchemaError`/`eprintln!`/`log`/panic message (only env-var NAMES and the URL/provider may appear); (b) `ObjError`'s `Display` carries no secret; (c) the signed `Authorization` header is not logged. If any leak exists, fix it (typed error without the value) and add a test asserting the error string does not contain a sentinel secret.

- [ ] **Step 2: Injection / traversal review**

Inspect `do_refresh_objstore` URL parsing + `sigv4::canonical_uri`/`enc_seg`: confirm a key containing `../`, `?`, `#`, CRLF, or NUL cannot (a) escape the bucket (path-style prefixes `/bucket/`), (b) inject extra HTTP headers (the key is RFC-3986 percent-encoded before it reaches `build_request_with_headers`; CR/LF → `%0D`/`%0A`), or (c) smuggle query params into the signed canonical request. Add a unit test in `sigv4.rs`:

```rust
    #[test]
    fn key_cannot_inject_headers_or_query() {
        let r = ObjGetRequest {
            provider: Provider::S3,
            bucket_or_container: "b".into(),
            key: "a\r\nX-Evil: 1/../../etc?z=1#frag".into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            creds: ObjCreds::S3 { key_id: "K".into(), secret: "S".into() },
        };
        let s = sign_get_s3(&r, DateTime { secs_since_epoch: 1 }).unwrap();
        assert!(!s.https_url.contains('\r') && !s.https_url.contains('\n'));
        assert!(s.https_url.contains("%0D%0A")); // CRLF encoded
        assert!(s.https_url.contains("%3F") && s.https_url.contains("%23"));
        for (k, v) in &s.headers {
            assert!(!k.contains('\n') && !v.contains('\n'));
        }
    }
```

(If `enc_seg` already encodes `?`,`#`,`\r`,`\n` — it does, per Task 2 — this test passes; it locks the property.)

- [ ] **Step 3: TLS downgrade / SSRF review**

Confirm: object-store fetch is **always** HTTPS (`fetch_rows_signed` rejects non-`https://`; `sign_get` rejects `http://` endpoints) so credentials never traverse plaintext; the URL host is whatever the recipe/endpoint specifies (operator-controlled DDL, same trust level as the existing `http(s)://` external sources — no new SSRF surface beyond the already-shipped EXT feature, which is the documented boundary). Note this explicitly in the T12 record.

- [ ] **Step 4: Run the added hardening tests + commit (only if changes made)**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-objstore`
Expected: PASS incl. `key_cannot_inject_headers_or_query`.
If Step 1/2/3 produced code changes or the new test:

```bash
cd /c/Users/ihass/KesselDB
git add -A crates/kessel-objstore crates/kessel-fetch crates/kesseldb-server
git commit -m "objstore: pentest hardening — key encoding lock + secret-leak audit"
```
(If NO code change was needed, still add the `key_cannot_inject_headers_or_query` lock test and commit it with message `objstore: lock key-encoding anti-injection invariant`.)

- [ ] **Step 5: Workspace gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: 0 failed; seed-7 ok.

---

### Task 12: Docs + gate reconciliation + internal record + memory

**Files:**
- Modify: `docs/USAGE.md`, `docs/STATUS.md`, `README.md`
- Create: `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`

- [ ] **Step 1: Measure the true default-build total**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"` and sum. The new default total = BASELINE (247) + the number of NEW default-build tests added by Tasks 5/6 (the catalog v3 back-compat test and the proto additive test — both MUST run in the default build because they guard load-bearing WAL/catalog back-compat; that is ~+2, but use the MEASURED number). Record `OBJ_NEW_TOTAL`.

- [ ] **Step 2: USAGE — document object-store sources**

In `docs/USAGE.md` §7 (external sources), add a precise subsection: `CREATE EXTERNAL SOURCE … FROM 's3://bucket/key' | 'az://container/blob'` requires building the server with `--features external-sources-objstore` (pulls rustls+webpki via the implied `tls`; default/plain `external-sources` builds remain `http(s)://`-only and dep-free). Document `REGION`, `ENDPOINT` (https-only; selects S3 path-style / custom Azure host), `AUTH OBJSTORE S3 KEYID ENV '…' SECRET ENV '…'` / `AUTH OBJSTORE AZURE ACCOUNT '…' KEY ENV '…'` (only env-var NAMES are persisted; values resolved router-side at REFRESH, never logged/persisted). State the honest boundary: JSON/CSV/NDJSON only — `FORMAT PARQUET`, Iceberg, prefix/multi-object listing, STS/SAS are explicit follow-ons (OBJ-2..5) and are rejected at CREATE with a clear message. No vague hedging.

- [ ] **Step 3: STATUS + README**

`docs/STATUS.md`: add an SP100 line (object-store external sources shipped: S3 SigV4 + Azure Shared Key GET, optional `external-sources-objstore`, no-bypass HTTPS, fail-closed, kernel/default-build deps unchanged; default-build test total 247→OBJ_NEW_TOTAL because the catalog/proto back-compat invariants are guarded in the default build; seed-7 green). Update the README headline test-count line(s) to `OBJ_NEW_TOTAL` (reconcile precisely — read current value, set to the measured number). Add the `--features external-sources-objstore` opt-in to the README "Honest boundaries" feature list (parallel to `--features tls` / `external-sources-tls`, noting it pulls rustls+webpki).

- [ ] **Step 4: Internal record**

Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md` (codename-free, factual): design link; builds on SP97/98/99; what shipped (kessel-objstore crate: b64/ymd_hms/SigV4/Azure; kessel-fetch object-store feature + fetch_rows_signed + build_request_with_headers extraction; catalog v3 trailer + ObjStoreEnv; proto additive objstore; SM apply; SQL grammar+rejections; do_refresh dispatch + materialize_external_rows extraction; server composite feature; e2e); the known-answer vectors used (cite the AWS SigV4 doc example + Azure shared-key layout); tests & which build each runs in; default-build total 247→OBJ_NEW_TOTAL with the exact reason (catalog/proto back-compat guards are default-build by necessity) — honest, no hand-waving; security posture (secret-reference router-side, never in op/WAL/log/digest/error; HTTPS-only/no-bypass; pentest findings from Task 11); determinism boundary (signing timestamp + TLS RNG captured-once, never in WAL/digest); the exact DEFERRED follow-ons: OBJ-2 Parquet, OBJ-3 Iceberg manifests, OBJ-4 prefix/multi-object listing, OBJ-5 STS/SAS/IMDS, plus carried EXT/TLS deferrals (unify fetch_rows_paginated decode — note `materialize_external_rows` extraction partially advanced this; trusted multi-page HTTPS test; test_config_trusting pub→pub(crate); gitleaks allow-list).

- [ ] **Step 5: Final workspace gate + commit**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: 0 failed; `large_seed_corpus_is_deterministic_and_converges ... ok`; summed total == `OBJ_NEW_TOTAL` (matches the README/STATUS numbers you just wrote).

```bash
cd /c/Users/ihass/KesselDB
git add docs/ README.md
git commit -m "docs: object-store external sources — USAGE/STATUS/README + subproject100 record"
git push
```

(Auto-memory lives outside the repo; the controller updates `project_kesseldb.md` + `MEMORY.md` after the final review — not committed here.)

---

## Self-Review

**1. Spec coverage:** §0 decomposition → Task 0 + scope notes; §1 architecture/resolution path → Tasks 4 (fetch_rows_signed) + 9 (do_refresh dispatch + materialize extraction); §2 crate/feature gating → Tasks 1,4,9; §3 signing (SigV4/Azure/base64/date) → Tasks 1,2,3 + Task 11 (encoding lock); §4 fetch_rows_signed → Task 4; §5 recipe/proto/SQL → Tasks 5,6,7,8; §6 security → Tasks 9 (env-by-name, ObjError no-secret) + 11 (audit); §7 testing (known-answer, stub, back-compat bytes, SQL, e2e, gate) → Tasks 2,3,4,5,6,8,10,12; §8 non-goals → enforced by Task 8 rejections + documented Task 12; §9 process → plan header + per-task review gate. No gap.

**2. Placeholder scan:** All code steps contain complete code. The two **deliberate** "obtain the real value" notes (Task 2 AWS signature constant; Task 10 harness copy) are explicit instructions with the exact source to use and a BLOCKED fallback — not silent TODOs; they exist because faking a crypto known-answer or duplicating 150 lines of proven oracle harness verbatim in the plan would be worse. The Task 10 body references the existing `external_source_tls_oracle.rs` as the literal template (it exists and was shipped in SP99).

**3. Type consistency:** `ObjGetRequest`/`SignedRequest`/`ObjCreds`/`ObjError`/`DateTime`/`Provider`/`sign_get` consistent across Tasks 1,2,3,9. `ExternalAuth::ObjStoreEnv{provider:u8,a_env,b_env,account:Option<String>}` identical in catalog (T5), SM (T7), router (T9). `ExternalRecipe.region/endpoint:Option<String>` consistent T5/T7/T9. Proto `objstore:Option<(u8,String,String,String)>` = (provider,account,region,endpoint) consistent T6/T7/T8. SQL emits `auth_kind=3`, `auth_a`/`auth_b` = env names, `objstore=Some((prov,account,region,endpoint))` (T8) → SM maps (T7) → recipe → router resolves by name (T9): the env-NAME-only invariant holds end-to-end. `fetch_rows_signed` signature identical T4/T9. `build_request_with_headers` extracted in T4, used in T4/T9-path. Trailer: v2 bytes byte-identical when no objstore (T5 hand-written assertion); proto old-frame tolerant (T6 truncation assertion). Gate accounting: Tasks 5 & 6 add default-build back-compat tests (unavoidable, guard load-bearing invariants) → explicitly tracked and reconciled in T12 (no false "0 delta" claim).
