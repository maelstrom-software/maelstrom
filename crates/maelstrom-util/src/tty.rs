use anyhow::{Context as _, Result};
use maelstrom_base::WindowSize;
use maelstrom_linux::{self as linux, OpenFlags, OwnedFd};
use std::ffi::CStr;

pub fn open_pseudoterminal(
    window_size: WindowSize,
    master_nonblock: bool,
) -> Result<(OwnedFd, OwnedFd)> {
    // Open the master.
    let mut master_flags = OpenFlags::RDWR | OpenFlags::NOCTTY;
    if master_nonblock {
        master_flags |= OpenFlags::NONBLOCK;
    }
    let master = linux::posix_openpt(master_flags).context("posix_openpt")?;
    linux::ioctl_tiocswinsz(&master, window_size.rows, window_size.columns)
        .context("ioctl_tiocswinsz")?;
    linux::grantpt(&master).context("grantpt")?;
    linux::unlockpt(&master).context("unlockpt")?;

    // Open the slave.
    let mut slave_name = [0u8; 100];
    linux::ptsname(&master, slave_name.as_mut_slice()).context("ptsname")?;
    let slave_name = CStr::from_bytes_until_nul(slave_name.as_slice())?;
    let slave = linux::open(
        slave_name,
        OpenFlags::RDWR | OpenFlags::NOCTTY,
        Default::default(),
    )
    .context("open(<ptsname>)")?;
    Ok((master, slave))
}
