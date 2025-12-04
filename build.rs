fn main() {
    #[cfg(target_os = "linux")]
    unsafe {
        std::env::set_var("RUSTFLAGS", "--cfg tokio_unstable");
    };
}
