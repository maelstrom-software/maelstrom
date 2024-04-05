//! FUSE kernel driver communication
//!
//! Raw communication channel to the FUSE kernel driver.

mod fuse_pure;
pub mod mount_options;

use std::fs::File;
use std::io;

pub use fuse_pure::fuse_mount_pure;
pub use fuse_pure::Mount;
use std::ffi::CStr;

#[inline]
fn libc_umount(mnt: &CStr) -> io::Result<()> {
    let r = unsafe { libc::umount(mnt.as_ptr()) };
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Warning: This will return true if the filesystem has been detached (lazy unmounted), but not
/// yet destroyed by the kernel.
fn is_mounted(fuse_device: &File) -> bool {
    use libc::{poll, pollfd};
    use std::os::unix::prelude::AsRawFd;

    let mut poll_result = pollfd {
        fd: fuse_device.as_raw_fd(),
        events: 0,
        revents: 0,
    };
    loop {
        let res = unsafe { poll(&mut poll_result, 1, 0) };
        break match res {
            0 => true,
            1 => (poll_result.revents & libc::POLLERR) != 0,
            -1 => {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                } else {
                    // This should never happen. The fd is guaranteed good as `File` owns it.
                    // According to man poll ENOMEM is the only error code unhandled, so we panic
                    // consistent with rust's usual ENOMEM behaviour.
                    panic!("Poll failed with error {}", err)
                }
            }
            _ => unreachable!(),
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::mem::ManuallyDrop;

    fn cmd_mount() -> String {
        std::str::from_utf8(
            std::process::Command::new("sh")
                .arg("-c")
                .arg("mount | grep fuse")
                .output()
                .unwrap()
                .stdout
                .as_ref(),
        )
        .unwrap()
        .to_owned()
    }

    #[tokio::test]
    async fn mount_unmount() {
        // We use ManuallyDrop here to leak the directory on test failure.  We don't
        // want to try and clean up the directory if it's a mountpoint otherwise we'll
        // deadlock.
        let tmp = ManuallyDrop::new(tempfile::tempdir().unwrap());
        let (file, mount) = Mount::new(tmp.path(), &[]).unwrap();
        let mnt = cmd_mount();
        eprintln!("Our mountpoint: {:?}\nfuse mounts:\n{}", tmp.path(), mnt,);
        assert!(mnt.contains(&*tmp.path().to_string_lossy()));
        assert!(is_mounted(file.get_ref()));
        drop(mount);
        let mnt = cmd_mount();
        eprintln!("Our mountpoint: {:?}\nfuse mounts:\n{}", tmp.path(), mnt,);

        let detached = !mnt.contains(&*tmp.path().to_string_lossy());
        // Linux supports MNT_DETACH, so we expect unmount to succeed even if the FS
        // is busy.  Other systems don't so the unmount may fail and we will still
        // have the mount listed.  The mount will get cleaned up later.
        assert!(detached);

        if detached {
            // We've detached successfully, it's safe to clean up:
            std::mem::ManuallyDrop::<_>::into_inner(tmp);
        }

        // Filesystem may have been lazy unmounted, so we can't assert this:
        // assert!(!is_mounted(&file));
    }
}
