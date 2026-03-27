use std::{any::Any, fmt, future::Future, pin::Pin, sync::Arc};

use datafusion::{
    catalog::Session,
    common::Result as DfResult,
    datasource::{TableProvider, TableType},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use glint_historical::provider::has_block_range_filter;

pub struct GlintEntityProvider {
    live: Arc<dyn TableProvider>,
    historical: Arc<dyn TableProvider>,
}

impl fmt::Debug for GlintEntityProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlintEntityProvider").finish()
    }
}

impl GlintEntityProvider {
    pub fn new(live: Arc<dyn TableProvider>, historical: Arc<dyn TableProvider>) -> Self {
        Self { live, historical }
    }

    fn select_backend(&self, filters: &[Expr]) -> &Arc<dyn TableProvider> {
        if has_block_range_filter(filters) {
            &self.historical
        } else {
            &self.live
        }
    }
}

impl TableProvider for GlintEntityProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.live.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        let owned: Vec<Expr> = filters.iter().map(|f| (*f).clone()).collect();
        let backend = self.select_backend(&owned);
        backend.supports_filters_pushdown(filters)
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = DfResult<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let backend = self.select_backend(filters);
        backend.scan(state, projection, filters, limit)
    }
}

#[cfg(test)]
mod tests {
    use glint_historical::provider::has_block_range_filter;

    #[test]
    fn routing_with_block_range_selects_historical() {
        use datafusion::prelude::*;
        let filter = col("block_number")
            .gt_eq(lit(100u64))
            .and(col("block_number").lt_eq(lit(500u64)));
        assert!(has_block_range_filter(&[filter]));
    }

    #[test]
    fn routing_without_block_range_selects_live() {
        use datafusion::prelude::*;
        let filter = col("owner").eq(lit("0xabc"));
        assert!(!has_block_range_filter(&[filter]));
    }
}
