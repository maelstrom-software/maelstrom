use heck::ToSnakeCase as _;
use proc_macro2::Span;
use syn::{
    parse::{Parse, ParseStream},
    parse_quote, Ident, ItemMacro, Result, Token,
};

pub struct Arguments {
    remote_type: Ident,
    _comma_token: Token![,],
    derive_macro: Ident,
}

impl Parse for Arguments {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            remote_type: input.parse()?,
            _comma_token: input.parse()?,
            derive_macro: input.parse()?,
        })
    }
}

pub fn main(args: Arguments) -> Result<ItemMacro> {
    let call_with_definition = Ident::new(
        &format!(
            "{}_call_with_definition",
            args.remote_type.to_string().to_snake_case()
        ),
        Span::call_site(),
    );
    let remote_derive = Ident::new(
        &format!(
            "{}_remote_derive",
            args.derive_macro.to_string().to_snake_case(),
        ),
        Span::call_site(),
    );
    Ok(parse_quote! {
        #call_with_definition!(#remote_derive);
    })
}
