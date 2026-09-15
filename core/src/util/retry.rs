/// Retry a TOCTOU operation only for the specified error pattern; propagate every other result.
/// The operation may await and use `?`. Exhaustion after 32 attempts is a mutation error.
macro_rules! retry_on {
    ($error:pat, $operation:block) => {{
        let mut attempts = 0;
        loop {
            match (async $operation).await {
                Err($error) => {
                    attempts += 1;
                    if attempts == 32 {
                        break Err($crate::error::MutationError::TOCTOUAttemptsExhausted);
                    }
                }
                result => break result,
            }
        }
    }};
}

pub(crate) use retry_on;

#[cfg(test)]
mod tests {
    use crate::error::MutationError;

    #[tokio::test]
    async fn retries_only_the_selected_error_and_bounds_attempts() {
        let mut attempts = 0;
        let result = retry_on!(MutationError::WriteConflict, {
            attempts += 1;
            if attempts == 1 { Err(MutationError::WriteConflict) } else { Ok(7) }
        });
        assert_eq!(result.unwrap(), 7);
        assert_eq!(attempts, 2);

        let result: Result<(), _> = retry_on!(MutationError::WriteConflict, { Err(MutationError::InvalidUpdate("fixture")) });
        assert!(matches!(result, Err(MutationError::InvalidUpdate("fixture"))));

        attempts = 0;
        let result: Result<(), _> = retry_on!(MutationError::WriteConflict, {
            attempts += 1;
            Err(MutationError::WriteConflict)
        });
        assert!(matches!(result, Err(MutationError::TOCTOUAttemptsExhausted)));
        assert_eq!(attempts, 32);
    }
}
