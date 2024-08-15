use maelstrom_macro::pocket_definition;

#[allow(dead_code)]
#[pocket_definition]
struct FooBar {
    a: u32,
    b: String,
}

#[test]
fn struct_pocketed() {
    macro_rules! as_string {
        ($($t:tt)*) => {
            const S: &str = stringify!($($t)*);
        }
    }
    foo_bar_call_with_definition!(as_string);

    assert_eq!(
        S,
        "#[allow(dead_code)] struct FooBar { a : u32, b : String, }"
    );
}

#[allow(dead_code)]
#[pocket_definition]
enum Baz {
    A(u32),
    B(String),
}

#[test]
fn enum_pocketed() {
    macro_rules! as_string {
        ($($t:tt)*) => {
            const S: &str = stringify!($($t)*);
        }
    }
    baz_call_with_definition!(as_string);
    assert_eq!(S, "#[allow(dead_code)] enum Baz { A(u32), B(String), }");
}

#[allow(dead_code)]
#[pocket_definition]
trait Bin {
    fn a();
    fn b();
}

#[test]
fn trait_pocketed() {
    macro_rules! as_string {
        ($($t:tt)*) => {
            const S: &str = stringify!($($t)*);
        }
    }
    bin_call_with_definition!(as_string);
    assert_eq!(S, "#[allow(dead_code)] trait Bin { fn a(); fn b(); }");
}
