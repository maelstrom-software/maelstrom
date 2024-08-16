use maelstrom_macro::{pocket_definition, remote_derive, remote_derive_inner};

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

macro_rules! as_string_attrs_remote_derive {
    ($($tokens:tt)*) => {
        impl FooBar {
            fn as_string_attrs() -> &'static str {
                stringify!($($tokens)*)
            }
        }
    }
}

remote_derive!(FooBar, AsStringAttrs, attr1(a = "b"), attr2);

#[test]
fn remote_derive_attrs() {
    assert_eq!(
        FooBar::as_string_attrs(),
        "#[attr1(a = \"b\")] #[attr2] struct FooBar { _a : u32, _b : String, }"
    );
}
