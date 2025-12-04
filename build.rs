fn main() {
    #[cfg(target_os = "linux")]
    unsafe {
        std::env::set_var("RUSTFLAGS", "--cfg tokio_unstable");
    };
    #[cfg(all(feature = "io-uring", not(target_os = "linux")))]
    panic!("The `io-uring` feature is only supported on Linux.");
}
