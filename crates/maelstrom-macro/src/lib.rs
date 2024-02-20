mod into_proto_buf;
mod into_result;
mod try_from_proto_buf;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(IntoResult)]
pub fn into_result(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match into_result::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

#[proc_macro_derive(IntoProtoBuf, attributes(proto))]
pub fn into_proto_buf(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match into_proto_buf::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}

#[proc_macro_derive(TryFromProtoBuf, attributes(proto))]
pub fn try_from_proto_buf(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match try_from_proto_buf::main(input) {
        Err(e) => e.into_compile_error().into(),
        Ok(v) => quote!(#v).into(),
    }
}
