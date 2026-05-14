use datafusion::prelude::Expr;
use glint_primitives::columns;

use super::extract_u64_literal;

pub fn extract_block_range(filters: &[Expr]) -> Option<(u64, u64)> {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    let mut lower: Option<u64> = None;
    let mut upper: Option<u64> = None;

    for filter in filters {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if *op == Operator::And {
                    if let Some((l, u)) = extract_block_range(&[*left.clone(), *right.clone()]) {
                        lower = Some(lower.map_or(l, |cur| cur.max(l)));
                        upper = Some(upper.map_or(u, |cur| cur.min(u)));
                    }
                } else if let Some((col_side_op, literal)) = normalize_comparison(left, *op, right)
                {
                    apply_bound(col_side_op, literal, &mut lower, &mut upper);
                }
            }
            Expr::Between(between) if is_block_number_col(&between.expr) => {
                if let (Some(lo), Some(hi)) = (
                    extract_u64_literal(&between.low),
                    extract_u64_literal(&between.high),
                ) {
                    lower = Some(lower.map_or(lo, |cur| cur.max(lo)));
                    upper = Some(upper.map_or(hi, |cur| cur.min(hi)));
                }
            }
            _ => {}
        }
    }

    match (lower, upper) {
        (Some(l), Some(u)) => Some((l, u)),
        _ => None,
    }
}

fn normalize_comparison(
    left: &Expr,
    op: datafusion::logical_expr::Operator,
    right: &Expr,
) -> Option<(datafusion::logical_expr::Operator, u64)> {
    use datafusion::logical_expr::Operator;

    if is_block_number_col(left) {
        extract_u64_literal(right).map(|v| (op, v))
    } else if is_block_number_col(right) {
        let flipped = match op {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            other => other,
        };
        extract_u64_literal(left).map(|v| (flipped, v))
    } else {
        None
    }
}

fn apply_bound(
    op: datafusion::logical_expr::Operator,
    v: u64,
    lower: &mut Option<u64>,
    upper: &mut Option<u64>,
) {
    use datafusion::logical_expr::Operator;

    match op {
        Operator::Eq => {
            *lower = Some(lower.map_or(v, |cur| cur.max(v)));
            *upper = Some(upper.map_or(v, |cur| cur.min(v)));
        }
        Operator::GtEq => {
            *lower = Some(lower.map_or(v, |cur| cur.max(v)));
        }
        Operator::Gt => {
            *lower = Some(lower.map_or(v + 1, |cur| cur.max(v + 1)));
        }
        Operator::LtEq => {
            *upper = Some(upper.map_or(v, |cur| cur.min(v)));
        }
        Operator::Lt => {
            let bound = v.saturating_sub(1);
            *upper = Some(upper.map_or(bound, |cur| cur.min(bound)));
        }
        _ => {}
    }
}

fn is_block_number_col(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(c) if c.name() == columns::BLOCK_NUMBER)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::Between;
    use datafusion::prelude::*;

    #[test]
    fn between_expression() {
        let filter = Expr::Between(Between::new(
            Box::new(col(columns::BLOCK_NUMBER)),
            false,
            Box::new(lit(10u64)),
            Box::new(lit(20u64)),
        ));
        assert_eq!(extract_block_range(&[filter]), Some((10, 20)));
    }

    #[test]
    fn between_with_non_block_column_ignored() {
        let filter = Expr::Between(Between::new(
            Box::new(col("owner")),
            false,
            Box::new(lit(10u64)),
            Box::new(lit(20u64)),
        ));
        assert_eq!(extract_block_range(&[filter]), None);
    }

    #[test]
    fn gt_lt_strict_bounds_offset_by_one() {
        let filter = col(columns::BLOCK_NUMBER)
            .gt(lit(99u64))
            .and(col(columns::BLOCK_NUMBER).lt(lit(501u64)));
        assert_eq!(extract_block_range(&[filter]), Some((100, 500)));
    }

    #[test]
    fn lt_zero_saturates_to_zero() {
        let filter = col(columns::BLOCK_NUMBER)
            .lt(lit(0u64))
            .and(col(columns::BLOCK_NUMBER).gt_eq(lit(0u64)));
        assert_eq!(extract_block_range(&[filter]), Some((0, 0)));
    }

    #[test]
    fn flipped_literal_left_lt() {
        let filter = lit(100u64)
            .lt(col(columns::BLOCK_NUMBER))
            .and(col(columns::BLOCK_NUMBER).lt_eq(lit(500u64)));
        assert_eq!(extract_block_range(&[filter]), Some((101, 500)));
    }

    #[test]
    fn flipped_literal_left_lt_eq() {
        let filter = lit(100u64)
            .lt_eq(col(columns::BLOCK_NUMBER))
            .and(col(columns::BLOCK_NUMBER).lt_eq(lit(500u64)));
        assert_eq!(extract_block_range(&[filter]), Some((100, 500)));
    }

    #[test]
    fn flipped_literal_left_gt() {
        let filter = col(columns::BLOCK_NUMBER)
            .gt_eq(lit(100u64))
            .and(lit(500u64).gt(col(columns::BLOCK_NUMBER)));
        assert_eq!(extract_block_range(&[filter]), Some((100, 499)));
    }

    #[test]
    fn flipped_literal_left_gt_eq() {
        let filter = col(columns::BLOCK_NUMBER)
            .gt_eq(lit(100u64))
            .and(lit(500u64).gt_eq(col(columns::BLOCK_NUMBER)));
        assert_eq!(extract_block_range(&[filter]), Some((100, 500)));
    }

    #[test]
    fn unrelated_column_ignored() {
        let filter = col("owner").eq(lit("foo"));
        assert_eq!(extract_block_range(&[filter]), None);
    }

    #[test]
    fn intersect_takes_tightest_bounds() {
        let lower = col(columns::BLOCK_NUMBER).gt_eq(lit(50u64));
        let tighter_lower = col(columns::BLOCK_NUMBER).gt_eq(lit(100u64));
        let upper = col(columns::BLOCK_NUMBER).lt_eq(lit(500u64));
        let tighter_upper = col(columns::BLOCK_NUMBER).lt_eq(lit(400u64));
        assert_eq!(
            extract_block_range(&[lower, tighter_lower, upper, tighter_upper]),
            Some((100, 400))
        );
    }
}
