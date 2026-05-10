pub trait IntoDataFusionError<T> {
    fn df_err(self) -> datafusion::error::Result<T>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> IntoDataFusionError<T> for Result<T, E> {
    fn df_err(self) -> datafusion::error::Result<T> {
        self.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    }
}

pub fn arrow_err(e: arrow::error::ArrowError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::error::DataFusionError;

    #[test]
    fn df_err_passes_ok_through() {
        let r: Result<i32, std::io::Error> = Ok(7);
        assert_eq!(r.df_err().unwrap(), 7);
    }

    #[test]
    fn df_err_wraps_external_error() {
        let r: Result<(), std::io::Error> = Err(std::io::Error::other("boom"));
        let err = r.df_err().unwrap_err();
        assert!(matches!(err, DataFusionError::External(_)));
        assert!(err.to_string().contains("boom"));
    }

    #[test]
    fn arrow_err_wraps_arrow_error() {
        let e = arrow::error::ArrowError::ComputeError("compute failed".to_owned());
        let df_e = arrow_err(e);
        assert!(matches!(df_e, DataFusionError::ArrowError(_, None)));
        assert!(df_e.to_string().contains("compute failed"));
    }
}
