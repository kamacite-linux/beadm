fn main() {
    println!("cargo:rustc-link-lib=zfs");
    println!("cargo:rustc-link-lib=nvpair");
}
