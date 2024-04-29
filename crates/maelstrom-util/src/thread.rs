use crate::fs::Fs;
use anyhow::Result;

pub fn assert_single_threaded() -> Result<()> {
    let fs = Fs::new();
    let num_tasks = fs
        .read_dir("/proc/self/task")?
        .filter(|e| e.is_ok())
        .count();
    if num_tasks != 1 {
        panic!("Process not single threaded, found {num_tasks} threads");
    }
    Ok(())
}
