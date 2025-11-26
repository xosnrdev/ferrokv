use std::array::TryFromSliceError;
use std::borrow::Cow;
use std::{fmt, io, result};

#[derive(Debug)]
pub enum FerroError {
    Io(io::Error),
    Corruption(Cow<'static, str>),
    InvalidData(Cow<'static, str>),
}

impl fmt::Display for FerroError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FerroError::Io(err) => write!(f, "I/O error: {err}"),
            FerroError::Corruption(msg) => write!(f, "Data corruption: {msg}"),
            FerroError::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
        }
    }
}

impl std::error::Error for FerroError {}

macro_rules! err_from_impl {
    ($variant:ident, $err_ty:ty) => {
        impl From<$err_ty> for FerroError {
            fn from(err: $err_ty) -> Self {
                FerroError::$variant(err)
            }
        }
    };
}

err_from_impl!(Io, io::Error);

impl From<TryFromSliceError> for FerroError {
    fn from(err: TryFromSliceError) -> Self {
        FerroError::InvalidData(err.to_string().into())
    }
}

pub type Result<T> = result::Result<T, FerroError>;
