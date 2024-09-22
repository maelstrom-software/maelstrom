use maelstrom_macro::pocket_definition;

#[expect(dead_code)]
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
    foo_bar_pocket_definition!(as_string);

    assert_eq!(
        S,
        "#[expect(dead_code)] struct FooBar { a : u32, b : String, }"
    );
}

#[test]
fn struct_pocketed_with_args() {
    macro_rules! as_string {
        ($($t:tt)*) => {
            const S: &str = stringify!($($t)*);
        }
    }
    foo_bar_pocket_definition!(as_string, other_arg);

    assert_eq!(
        S,
        "#[expect(dead_code)] struct FooBar { a : u32, b : String, }, other_arg"
    );
}

#[expect(dead_code)]
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
    baz_pocket_definition!(as_string);
    assert_eq!(S, "#[expect(dead_code)] enum Baz { A(u32), B(String), }");
}

#[expect(dead_code)]
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
    bin_pocket_definition!(as_string);
    assert_eq!(S, "#[expect(dead_code)] trait Bin { fn a(); fn b(); }");
}
