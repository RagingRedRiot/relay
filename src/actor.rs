use argon2::password_hash::SaltString;
use argon2::password_hash::rand_core::OsRng;
use argon2::{Argon2, PasswordHasher};

use crate::model::NewCredential;

pub(crate) fn generate_hash(
    credential: NewCredential,
) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(credential.password.0.as_bytes(), &salt)?
        .to_string();
    Ok(hash)
}
