use crate::provider::extract_u64_literal;
use datafusion::prelude::Expr;

pub fn extract_string_eq(filters: &[Expr], col_name: &str) -> Option<String> {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    for filter in filters {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                if let (
                    Expr::Column(c),
                    Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(v)), _),
                ) = (left.as_ref(), right.as_ref())
                    && c.name() == col_name
                {
                    return Some(v.clone());
                }
                if let (
                    Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(v)), _),
                    Expr::Column(c),
                ) = (left.as_ref(), right.as_ref())
                    && c.name() == col_name
                {
                    return Some(v.clone());
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
                if let Some(v) = extract_string_eq(&[*left.clone()], col_name) {
                    return Some(v);
                }
                if let Some(v) = extract_string_eq(&[*right.clone()], col_name) {
                    return Some(v);
                }
            }
            _ => {}
        }
    }
    None
}

/// Extract a u64 literal equality filter for a given column name.
pub fn extract_u64_eq(filters: &[Expr], col_name: &str) -> Option<u64> {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    for filter in filters {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                if let Expr::Column(c) = left.as_ref()
                    && c.name() == col_name
                    && let Some(v) = extract_u64_literal(right)
                {
                    return Some(v);
                }
                if let Expr::Column(c) = right.as_ref()
                    && c.name() == col_name
                    && let Some(v) = extract_u64_literal(left)
                {
                    return Some(v);
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
                if let Some(v) = extract_u64_eq(&[*left.clone()], col_name) {
                    return Some(v);
                }
                if let Some(v) = extract_u64_eq(&[*right.clone()], col_name) {
                    return Some(v);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn extract_string_eq_column_left() {
        let filter = col("ann_key").eq(lit("token"));
        assert_eq!(
            extract_string_eq(&[filter], "ann_key"),
            Some("token".to_owned())
        );
    }

    #[test]
    fn extract_string_eq_column_right() {
        let filter = lit("token").eq(col("ann_key"));
        assert_eq!(
            extract_string_eq(&[filter], "ann_key"),
            Some("token".to_owned())
        );
    }

    #[test]
    fn extract_string_eq_wrong_column() {
        let filter = col("other").eq(lit("token"));
        assert_eq!(extract_string_eq(&[filter], "ann_key"), None);
    }

    #[test]
    fn extract_string_eq_missing() {
        assert_eq!(extract_string_eq(&[], "ann_key"), None);
    }

    #[test]
    fn extract_string_eq_inside_and_left() {
        let filter = col("ann_key")
            .eq(lit("token"))
            .and(col("block_number").eq(lit(10u64)));
        assert_eq!(
            extract_string_eq(&[filter], "ann_key"),
            Some("token".to_owned())
        );
    }

    #[test]
    fn extract_string_eq_inside_and_right() {
        let filter = col("block_number")
            .eq(lit(10u64))
            .and(col("ann_key").eq(lit("token")));
        assert_eq!(
            extract_string_eq(&[filter], "ann_key"),
            Some("token".to_owned())
        );
    }

    #[test]
    fn extract_string_eq_ignores_non_eq_op() {
        // GtEq on a string column should not match
        let filter = col("ann_key").gt_eq(lit("token"));
        assert_eq!(extract_string_eq(&[filter], "ann_key"), None);
    }

    #[test]
    fn extract_u64_eq_column_left() {
        let filter = col("ann_value").eq(lit(42u64));
        assert_eq!(extract_u64_eq(&[filter], "ann_value"), Some(42));
    }

    #[test]
    fn extract_u64_eq_column_right() {
        let filter = lit(42u64).eq(col("ann_value"));
        assert_eq!(extract_u64_eq(&[filter], "ann_value"), Some(42));
    }

    #[test]
    fn extract_u64_eq_int64_literal() {
        let filter = col("ann_value").eq(lit(42i64));
        assert_eq!(extract_u64_eq(&[filter], "ann_value"), Some(42));
    }

    #[test]
    fn extract_u64_eq_negative_int_rejected() {
        let filter = col("ann_value").eq(lit(-1i64));
        assert_eq!(extract_u64_eq(&[filter], "ann_value"), None);
    }

    #[test]
    fn extract_u64_eq_wrong_column() {
        let filter = col("other").eq(lit(42u64));
        assert_eq!(extract_u64_eq(&[filter], "ann_value"), None);
    }

    #[test]
    fn extract_u64_eq_inside_and() {
        let filter = col("block_number")
            .eq(lit(10u64))
            .and(col("ann_value").eq(lit(99u64)));
        assert_eq!(extract_u64_eq(&[filter], "ann_value"), Some(99));
    }
}
