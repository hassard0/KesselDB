use crate::{ObjError, ObjGetRequest, SignedRequest};

pub(crate) fn sign_get_azure(
    _req: &ObjGetRequest,
    _now: crate::DateTime,
) -> Result<SignedRequest, ObjError> {
    Err(ObjError::Encoding("azure not implemented (Task 3)".into()))
}
