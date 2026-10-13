//! Deliberately short incompatible table: host must reject before reading callbacks.
#[repr(C)]
struct Header {
    version: u32,
    size: u32,
}
static HEADER: Header = Header {
    version: 99,
    size: 8,
};
#[unsafe(no_mangle)]
pub extern "C" fn mrsm_plugin_v1() -> *const std::ffi::c_void {
    (&HEADER as *const Header).cast()
}
