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

#[derive(Clone, PartialEq, Eq)]
pub enum ObjCreds {
    /// AWS / S3-compatible.
    S3 { key_id: String, secret: String },
    /// Azure Blob Shared Key (`key_b64` is the base64 account key).
    AzureSharedKey { account: String, key_b64: String },
}

impl std::fmt::Debug for ObjCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjCreds::S3 { key_id, .. } => f
                .debug_struct("S3")
                .field("key_id", key_id)
                .field("secret", &"[REDACTED]")
                .finish(),
            ObjCreds::AzureSharedKey { account, .. } => f
                .debug_struct("AzureSharedKey")
                .field("account", account)
                .field("key_b64", &"[REDACTED]")
                .finish(),
        }
    }
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
    let days = (secs / 86_400) as i64;
    let sod = (secs % 86_400) as u32;
    let (h, mi, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097; // secs is u64 ⇒ z ≥ 719468 > 0; the negative branch is unreachable, kept for Hinnant-algorithm fidelity
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
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
        assert_eq!(
            ymd_hms(1_440_938_160),
            ("20150830".into(), "20150830T123600Z".into())
        );
        assert_eq!(
            ymd_hms(1_385_281_895),
            ("20131124".into(), "20131124T083135Z".into())
        );
        assert_eq!(
            ymd_hms(0),
            ("19700101".into(), "19700101T000000Z".into())
        );
    }
}

#[cfg(test)]
mod cred_tests {
    use super::*;

    /// Pentest regression: a sentinel secret must never reach the
    /// `Debug`, the `ObjError` `Display`/`Debug` (any error path), or
    /// the transport-visible `SignedRequest` (URL + header values). The
    /// `Authorization` header legitimately carries HMAC *output* derived
    /// from the secret — that is standard SigV4/SharedKey and not the
    /// raw key — so we assert the *raw sentinel string* never appears.
    #[test]
    fn sentinel_secret_never_in_error_or_request() {
        const SENTINEL: &str = "SENTINEL_SECRET_VALUE";

        // 1. Debug of creds (both variants).
        let s3c = ObjCreds::S3 {
            key_id: "AKIA".into(),
            secret: SENTINEL.into(),
        };
        assert!(!format!("{s3c:?}").contains(SENTINEL));
        let azc = ObjCreds::AzureSharedKey {
            account: "acct".into(),
            key_b64: SENTINEL.into(),
        };
        assert!(!format!("{azc:?}").contains(SENTINEL));

        // 2. Forced error path: wrong cred variant for S3 signer, and a
        //    non-base64 Azure key — both must produce a typed ObjError
        //    whose Display/Debug carry no sentinel.
        let bad_s3 = ObjGetRequest {
            provider: Provider::S3,
            bucket_or_container: "b".into(),
            key: "k".into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            creds: azc.clone(),
        };
        let e = sign_get(&bad_s3, DateTime { secs_since_epoch: 1 })
            .unwrap_err();
        assert!(!format!("{e}").contains(SENTINEL));
        assert!(!format!("{e:?}").contains(SENTINEL));

        let bad_az = ObjGetRequest {
            provider: Provider::Azure,
            bucket_or_container: "c".into(),
            key: "k".into(),
            region: None,
            endpoint: None,
            creds: ObjCreds::AzureSharedKey {
                // not valid base64 -> ObjError::Cred, and the bad value
                // happens to embed the sentinel; it must NOT echo it.
                account: "acct".into(),
                key_b64: format!("{SENTINEL}*not*b64"),
            },
        };
        let e2 = sign_get(&bad_az, DateTime { secs_since_epoch: 1 })
            .unwrap_err();
        assert!(!format!("{e2}").contains(SENTINEL));
        assert!(!format!("{e2:?}").contains(SENTINEL));

        // 3. Success path: the transport-visible SignedRequest (URL and
        //    every header value) must not contain the raw secret.
        let ok_s3 = ObjGetRequest {
            provider: Provider::S3,
            bucket_or_container: "b".into(),
            key: "k.json".into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            creds: ObjCreds::S3 {
                key_id: "AKIA".into(),
                secret: SENTINEL.into(),
            },
        };
        let sr =
            sign_get(&ok_s3, DateTime { secs_since_epoch: 1 }).unwrap();
        assert!(!sr.https_url.contains(SENTINEL));
        for (k, v) in &sr.headers {
            assert!(!k.contains(SENTINEL) && !v.contains(SENTINEL),
                "raw secret leaked into header {k}");
        }
        let ok_az = ObjGetRequest {
            provider: Provider::Azure,
            bucket_or_container: "c".into(),
            key: "b.json".into(),
            region: None,
            endpoint: None,
            creds: ObjCreds::AzureSharedKey {
                account: "acct".into(),
                key_b64: b64::encode(SENTINEL.as_bytes()),
            },
        };
        let sr2 =
            sign_get(&ok_az, DateTime { secs_since_epoch: 1 }).unwrap();
        assert!(!sr2.https_url.contains(SENTINEL));
        assert!(!sr2.https_url
            .contains(&b64::encode(SENTINEL.as_bytes())));
        for (k, v) in &sr2.headers {
            assert!(!k.contains(SENTINEL) && !v.contains(SENTINEL),
                "raw secret leaked into header {k}");
        }
    }

    #[test]
    fn objcreds_debug_redacts() {
        let c = ObjCreds::S3 { key_id: "AKIA".into(), secret: "TOPSECRET".into() };
        let s = format!("{c:?}");
        assert!(s.contains("AKIA"));
        assert!(!s.contains("TOPSECRET"));
        assert!(s.contains("REDACTED"));
        let a = ObjCreds::AzureSharedKey { account: "acct".into(), key_b64: "S3CR3TKEY".into() };
        let s2 = format!("{a:?}");
        assert!(s2.contains("acct") && !s2.contains("S3CR3TKEY") && s2.contains("REDACTED"));
    }
}
