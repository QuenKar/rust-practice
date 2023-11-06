use rust_decimal::{prelude::ToPrimitive, Decimal};

#[allow(dead_code)]
fn rust_decimal_to_i128() {
    let d = Decimal::new(123, 2);
    let i = d.to_i128();
    println!("i128: {}", i.unwrap());
}

#[test]
fn test_rust_decimal_to_i128() {
    rust_decimal_to_i128();
}
