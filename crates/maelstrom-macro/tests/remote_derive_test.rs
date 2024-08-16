use maelstrom_macro::{pocket_definition, remote_derive};

#[pocket_definition]
struct FooBar {
    _a: u32,
    _b: String,
}

macro_rules! as_string_remote_derive {
    (struct $name:ident { $($n:ident : $t:ty,)* }) => {
        impl $name {
            fn as_string() -> &'static str {
                stringify!(struct $name { $($n: $t),* })
            }
        }
    }
}

remote_derive!(FooBar, AsString);

#[test]
fn remote_derive() {
    assert_eq!(
        FooBar::as_string(),
        "struct FooBar { _a : u32, _b : String }"
    );
}
