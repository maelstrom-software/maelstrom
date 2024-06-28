use anyhow::Result;
use maelstrom_util::process::ExitCode;

pub fn client_bg_proc() -> Result<ExitCode> {
    maelstrom_client::bg_proc_main()?;
    Ok(ExitCode::from(0))
}
