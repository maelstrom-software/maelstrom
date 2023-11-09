use crate::{ErrnoExt as _, Result};
use core::ffi::c_int;
use core::mem;
use netlink_packet_core::{
    NetlinkMessage, NetlinkSerializable, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST,
};
use netlink_packet_route::{rtnl::constants::RTM_SETLINK, LinkMessage, RtnlMessage, IFF_UP};

struct NetlinkRoute {
    socket_fd: c_int,
}

#[repr(C)]
pub struct sockaddr_nl_t {
    pub sin_family: nc::sa_family_t,
    pub nl_pad: u16,
    pub nl_pid: u32,
    pub nl_groups: u32,
}

const NETLINK_ROUTE: i32 = 0;

impl NetlinkRoute {
    fn new() -> Result<Self> {
        let socket_fd = unsafe {
            nc::socket(
                nc::AF_NETLINK,
                nc::SOCK_RAW | nc::SOCK_CLOEXEC,
                NETLINK_ROUTE,
            )
        }
        .map_system_errno("failed to create netlink socket")?;
        let addr = sockaddr_nl_t {
            sin_family: nc::AF_NETLINK as nc::sa_family_t,
            nl_pad: 0,
            nl_pid: 0, // the kernel
            nl_groups: 0,
        };
        let addr_alias = unsafe { mem::transmute::<&sockaddr_nl_t, &nc::sockaddr_t>(&addr) };
        unsafe {
            nc::bind(
                socket_fd,
                addr_alias,
                mem::size_of::<sockaddr_nl_t>() as u32,
            )
        }
        .map_system_errno("failed to bind netlink socket")?;
        Ok(Self { socket_fd })
    }

    fn request<I>(&mut self, req: &NetlinkMessage<I>) -> Result<()>
    where
        I: NetlinkSerializable,
    {
        let mut buffer = [0; 1024];
        req.serialize(&mut buffer);

        let mut addr = nc::sockaddr_in_t::default();
        unsafe { nc::sendto(self.socket_fd, &buffer[0..req.buffer_len()], 0, &addr, 0) }
            .map_system_errno("sendto on netlink socket failed")?;

        let mut addr_len = 0;
        unsafe { nc::recvfrom(self.socket_fd, &mut buffer, 0, &mut addr, &mut addr_len) }
            .map_system_errno("recvfrom on netlink socket failed")?;

        // we can't deserialize since that allocates memory, so we just hope that it worked
        Ok(())
    }
}

pub fn ifup_loopback() -> Result<()> {
    let mut route = NetlinkRoute::new()?;

    let mut message = LinkMessage::default();
    message.header.index = 1;
    message.header.flags |= IFF_UP;
    message.header.change_mask |= IFF_UP;

    let mut req = NetlinkMessage::from(RtnlMessage::SetLink(message));
    req.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_EXCL | NLM_F_CREATE;
    req.header.length = req.buffer_len() as u32;
    req.header.message_type = RTM_SETLINK;

    route.request(&req)?;
    Ok(())
}
