#![allow(clippy::manual_unwrap_or_default)]

mod config;
mod into_proto_buf;
mod into_result;
mod pocket_definition;
mod remote_derive;
mod try_from_proto_buf;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Item};

/// Derives an implementation of `maelstrom_client_base::IntoResult`
#[proc_macro_derive(IntoResult)]
pub fn into_result(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match into_result::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

/// Derives an implementation of `maelstrom_client_base::proto_buf_conv::IntoProtoBuf`
///
/// Attributes are shared with [`TryFromProtoBuf`] macro, so if they seem needless for this macro,
/// they may have some different meaning for the other macro.
///
/// container attributes:
/// - `other_type` (required) the type-path to the protobuf type we are converting into
/// - `enum_type` If we are deriving for an `enum` and the protobuf type is a `struct` containing
///   an enum, convert ourselves into this given enum type and convert into `other_type` by calling
///   [`std::convert::Into::into`].
/// - `option_all` applies the `option` field attribute to all fields
/// - `try_from_into` Implement by forwarding to [`std::convert::Into::into`]
///
/// field attributes:
/// - `option` / `default` Indicates that this field is wrapped in an `Option` in the protobuf
/// type. When converting the value will be wrapped in `Some`.
#[proc_macro_derive(IntoProtoBuf, attributes(proto))]
pub fn into_proto_buf(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match into_proto_buf::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

/// See [`IntoProtoBuf`]. Import this to use `remote_derive` with the `IntoProtoBuf` macro.
#[proc_macro]
pub fn into_proto_buf_remote_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match into_proto_buf::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

/// Derives an implementation of `maelstrom_client_base::proto_buf_conv::TryFromProtoBuf`
///
/// Attributes are shared with [`IntoProtoBuf`] macro, so if they seem needless for this macro,
/// they may have some different meaning for the other macro.
///
/// container attributes:
/// - `other_type` (required) the type-path to the protobuf type we are converting from
/// - `enum_type` If we are deriving for an `enum` and the protobuf type is a `struct` containing
///   an enum, convert ourselves from this given enum type which we obtain by calling
///   [`std::convert::TryFrom::try_from`] on `other_type`
/// - `option_all` applies the `option` field attribute to all fields
/// - `try_from_into` Implement by forwarding to [`std::convert::TryFrom::try_from`]
///
/// field attributes:
/// - `option` Indicates that this field is wrapped in an `Option` in the protobuf type. An error
/// will occur during conversion if `None` is encoundered.
/// - `default` Indicates that this field is wrapped in an `Option` in the protobuf type. If `None`
/// is encountered, the value of [`std::default::Default::default()`] will be used.
#[proc_macro_derive(TryFromProtoBuf, attributes(proto))]
pub fn try_from_proto_buf(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match try_from_proto_buf::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

/// See [`TryFromProtoBuf`]. Import this to use `remote_derive` with the `TryFromProtoBuf` macro.
#[proc_macro]
pub fn try_from_proto_buf_remote_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match try_from_proto_buf::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

/// Derives an implementation for the `maelstrom_util::config::Config` trait.
#[proc_macro_derive(Config, attributes(config))]
pub fn config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match config::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

/// This macro can be applied to a `struct` or an `enum` and it will save the tokens of the
/// definition inside a `macro_rules` macro named `<name>_pocket_definition`. This can then be used
/// to derive stuff for it using the [`remote_derive!`] macro.
///
/// arguments:
/// - `export` apply `#[macro_export]` attribute to the generated `macro_rules` macro.
#[proc_macro_attribute]
pub fn pocket_definition(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as Item);
    match pocket_definition::main(&input, attrs.into()) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote! {
            #input
            #v
        }
        .into(),
    }
}

#[doc(hidden)]
#[proc_macro]
pub fn remote_derive_inner(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as remote_derive::InnerArguments);
    match remote_derive::inner_main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

/// Derive a trait for some type in a different module (or crate) than the one where the definition
/// is. The orphan rule still applies.
///
/// The `_pocket_definition` macro for the given type, and the `_remote_derive` macro for the given
/// macro must be in scope. Also since the derive macro usually outputs tokens which expect the
/// given type name itself and any type expressions found in its definition to be resolvable from
/// the current module, the type itself has to be in scope and possibly other types too.
///
/// arguments:
/// - `<TypeName>` (required) the name of the type to derive the trait for.
/// - `<MacroName>` (required) the name of the macro (which must support `remote_derive`)
/// - `<attributes>..` any container or field/variant attribute to apply during the expansion
///
/// Field/Variant attributes take the form of `@<field-or-variant-name>: <attr>`. Container
/// attributes (ones which apply to the whole type) have no name designation (are just `<attr>`).
/// The `<attr>` part for either is whatever would be contained inside `#[..]`.
#[proc_macro]
pub fn remote_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as remote_derive::Arguments);
    match remote_derive::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#(#v)*).into(),
    }
}
