use std::path::PathBuf;

use clap::Args;

#[derive(Debug, Args)]
#[command(next_help_heading = "Mote")]
pub struct MoteArgs {
    /// Unix socket path for `ExEx` -> Analytics IPC
    #[arg(long = "mote.exex-socket-path", default_value = "/tmp/mote-exex.sock")]
    pub exex_socket_path: PathBuf,

    /// Run without `ExEx` (debug builds only)
    #[cfg(debug_assertions)]
    #[arg(long = "mote.disable-exex")]
    pub disable_exex: bool,
}

impl MoteArgs {
    #[cfg(not(debug_assertions))]
    pub const fn disable_exex(&self) -> bool {
        false
    }

    #[cfg(debug_assertions)]
    pub const fn disable_exex(&self) -> bool {
        self.disable_exex
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    // Wrapper struct since MoteArgs uses Args trait (flatten), not Parser
    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(flatten)]
        mote: MoteArgs,
    }

    #[test]
    fn default_socket_path() {
        let cli = TestCli::parse_from(["test"]);
        assert_eq!(
            cli.mote.exex_socket_path,
            PathBuf::from("/tmp/mote-exex.sock")
        );
    }

    #[test]
    fn custom_socket_path() {
        let cli = TestCli::parse_from(["test", "--mote.exex-socket-path", "/custom/path.sock"]);
        assert_eq!(
            cli.mote.exex_socket_path,
            PathBuf::from("/custom/path.sock")
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    fn disable_exex_flag_exists_in_debug() {
        let cli = TestCli::parse_from(["test", "--mote.disable-exex"]);
        assert!(cli.mote.disable_exex());
    }
}
