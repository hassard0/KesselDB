use crate::{ObjError, ObjGetRequest, SignedRequest};

pub(crate) fn sign_get_s3(
    _req: &ObjGetRequest,
    _now: crate::DateTime,
) -> Result<SignedRequest, ObjError> {
    Err(ObjError::Encoding("sigv4 not implemented (Task 2)".into()))
}
