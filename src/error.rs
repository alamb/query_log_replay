/// Adds the ability to convert an an Result<T> to a Result<T, String> if
/// structured errors are overkill for your usecase.
pub trait StringifyError<T> {
    /// Convert this error into a string
    fn stringify(self) -> Result<T, String>;

    /// Convert this error into a string with more context
    fn context(self, msg: &str) -> Result<T, String>;
}

/// Implement stringifying for all results
impl<T, E: std::fmt::Display> StringifyError<T> for Result<T, E> {
    fn stringify(self) -> Result<T, String> {
        self.map_err(|e| e.to_string())
    }

    fn context(self, msg: &str) -> Result<T, String> {
        self.map_err(|e| format!("{}: {}", msg, e))
    }
}
