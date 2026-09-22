use std::env;

#[test]
fn print_cwd() {
    let cwd = env::current_dir().unwrap();
    println!("CWD: {}", cwd.display());
    assert!(cwd.ends_with("polars-logfmt"));
}
