use jiter::JiterError;

/// Error type for streaming and projection operations.
#[derive(Debug)]
pub struct StreamError {
    pub message: String,
}

impl StreamError {
    #[must_use]
    pub const fn new(message: String) -> Self {
        Self { message }
    }
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<JiterError> for StreamError {
    fn from(e: JiterError) -> Self {
        Self::new(format!("{e}"))
    }
}
