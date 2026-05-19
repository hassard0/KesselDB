use crate::{b64, DateTime, ObjCreds, ObjError, ObjGetRequest, SignedRequest};
use kessel_crypto::hmac_sha256;

const X_MS_VERSION: &str = "2021-08-06";
const WD: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MON: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
    "Oct", "Nov", "Dec",
];

/// RFC-1123 (`Sun, 24 Nov 2013 08:31:35 GMT`) from epoch seconds.
fn http_date(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let sod = (secs % 86_400) as u32;
    let (h, mi, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{b64, DateTime, ObjCreds, ObjGetRequest, Provider};

    #[test]
    fn http_date_rfc1123_known_instants() {
        // Independent KAT: RFC-1123 is fixed; these are verifiable by
        // hand / any date tool. 1385281895 = 2013-11-24T08:31:35Z (Sun).
        assert_eq!(http_date(1_385_281_895), "Sun, 24 Nov 2013 08:31:35 GMT");
        // 0 = 1970-01-01T00:00:00Z (Thursday).
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        // 1440938160 = 2015-08-30T12:36:00Z (Sunday).
        assert_eq!(http_date(1_440_938_160), "Sun, 30 Aug 2015 12:36:00 GMT");
        // 1078012800 = 2004-02-29T00:00:00Z (leap day, Sunday).
        assert_eq!(http_date(1_078_012_800), "Sun, 29 Feb 2004 00:00:00 GMT");
    }

    #[test]
    fn azure_string_to_sign_layout_matches_spec_literal() {
        // Independent oracle: the Azure "Authorize with Shared Key"
        // Blob StringToSign for a GET with only x-ms-date/x-ms-version
        // is fully fixed by the spec. Hand-write it here and assert the
        // signer's auth equals SharedKey + base64(HMAC(key, THIS literal)).
        let key_raw = [0x2au8; 64];
        let key_b64 = b64::encode(&key_raw);
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
        // 1385281895 -> "Sun, 24 Nov 2013 08:31:35 GMT".
        let now = DateTime { secs_since_epoch: 1_385_281_895 };
        let s = sign_get_azure(&req, now).unwrap();

        // Spec-fixed StringToSign, written independently of the impl:
        // VERB \n + 11 empty standard-header lines + empty Date line
        // (12 \n total after GET) + CanonicalizedHeaders +
        // CanonicalizedResource.
        let expected_sts = concat!(
            "GET\n",                       // VERB
            "\n\n\n\n\n\n\n\n\n\n\n",      // 11 standard headers (all empty)
            "x-ms-date:Sun, 24 Nov 2013 08:31:35 GMT\n",
            "x-ms-version:2021-08-06\n",
            "/devstoreacct/mycontainer/path/to/blob.json"
        ).to_string();
        // Count check: exactly 12 '\n' before the canonical headers.
        let prefix = &expected_sts[..expected_sts.find("x-ms-date").unwrap()];
        assert_eq!(prefix.matches('\n').count(), 12, "STS must have 12 LF before canonical headers");

        let key = b64::decode(&key_b64).unwrap();
        let expected_auth = format!(
            "SharedKey devstoreacct:{}",
            b64::encode(&kessel_crypto::hmac_sha256(&key, expected_sts.as_bytes()))
        );
        let auth = s.headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .map(|(_, v)| v.clone()).unwrap();
        assert_eq!(auth, expected_auth,
            "signer STS layout must match the hand-written Azure spec literal");

        assert_eq!(
            s.https_url,
            "https://devstoreacct.blob.core.windows.net/mycontainer/path/to/blob.json"
        );
        let h = |n: &str| s.headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(n)).map(|(_, v)| v.clone());
        assert_eq!(h("x-ms-date"), Some("Sun, 24 Nov 2013 08:31:35 GMT".into()));
        assert_eq!(h("x-ms-version"), Some("2021-08-06".into()));
        assert!(h("authorization").unwrap().starts_with("SharedKey devstoreacct:"));
        // Determinism (additional check).
        assert_eq!(sign_get_azure(&req, now).unwrap().headers, s.headers);
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
        let s = sign_get_azure(&req, DateTime { secs_since_epoch: 1 }).unwrap();
        assert_eq!(s.https_url, "https://custom.example.com/c/b");
        let mut bad = req.clone();
        bad.creds = ObjCreds::AzureSharedKey {
            account: "acct".into(),
            key_b64: "not*base64".into(),
        };
        assert!(matches!(
            sign_get_azure(&bad, DateTime { secs_since_epoch: 1 }),
            Err(ObjError::Cred(_))
        ));
        // http:// endpoint rejected.
        let mut httpep = req.clone();
        httpep.endpoint = Some("http://x".into());
        assert!(matches!(
            sign_get_azure(&httpep, DateTime { secs_since_epoch: 1 }),
            Err(ObjError::BadEndpoint(_))
        ));
    }
}
