//! Function wrappers for Linux syscalls.
#![no_std]

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct sockaddr_nl_t {
    pub sin_family: nc::sa_family_t,
    pub nl_pad: u16,
    pub nl_pid: u32,
    pub nl_groups: u32,
}
