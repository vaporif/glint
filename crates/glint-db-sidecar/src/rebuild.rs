use std::path::Path;

use glint_historical::schema;
use rusqlite::Connection;
use tracing::info;

pub fn rebuild(db_path: &Path, _rpc_url: &str, _from_block: u64) -> eyre::Result<()> {
    info!(db = %db_path.display(), "starting database rebuild");

    let conn = Connection::open(db_path)?;
    schema::drop_and_recreate(&conn)?;
    info!("database reset -- populate via ExEx stream or implement eth_getLogs fetching");

    Ok(())
}
