use heck::ToSnakeCase as _;
use proc_macro2::Span;
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_quote,
    punctuated::Punctuated,
    Attribute, DeriveInput, Ident, ItemMacro, Meta, Result, Token,
};

struct DeriveAttrs {
    _comma_token: Token![,],
    attrs: Punctuated<Meta, Token![,]>,
}

pub struct Arguments {
    remote_type: Ident,
    _comma_token: Token![,],
    derive_macro: Ident,
    derive_attrs: Option<DeriveAttrs>,
}

impl Parse for Arguments {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            remote_type: input.parse()?,
            _comma_token: input.parse()?,
            derive_macro: input.parse()?,
            derive_attrs: input
                .peek(Token![,])
                .then(|| -> Result<_> {
                    Ok(DeriveAttrs {
                        _comma_token: input.parse()?,
                        attrs: input.parse_terminated(Meta::parse, Token![,])?,
                    })
                })
                .transpose()?,
        })
    }
}

pub struct InnerArguments {
    input: DeriveInput,
    _comma_token: Token![,],
    remote_derive: Ident,
    derive_attrs: Option<DeriveAttrs>,
}

impl Parse for InnerArguments {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            input: input.parse()?,
            _comma_token: input.parse()?,
            remote_derive: input.parse()?,
            derive_attrs: input
                .peek(Token![,])
                .then(|| -> Result<_> {
                    Ok(DeriveAttrs {
                        _comma_token: input.parse()?,
                        attrs: input.parse_terminated(
                            |s| {
                                let content;
                                parenthesized!(content in s);
                                Meta::parse(&content)
                            },
                            Token![,],
                        )?,
                    })
                })
                .transpose()?,
        })
    }
}

pub fn inner_main(args: InnerArguments) -> Result<ItemMacro> {
    let remote_derive = &args.remote_derive;
    let input = &args.input;
    if let Some(attrs) = args.derive_attrs {
        let attrs = attrs
            .attrs
            .iter()
            .map(|a| -> Attribute { parse_quote!(#[#a]) });
        Ok(parse_quote! {
            #remote_derive!(
                #(#attrs)*
                #input
            );
        })
    } else {
        Ok(parse_quote! {
            #remote_derive!(#input);
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
    if let Some(attrs) = args.derive_attrs {
        let attrs = attrs.attrs.iter();
        Ok(parse_quote! {
            #call_with_definition!(
                remote_derive_inner, #remote_derive, #((#attrs)),*
            );
        })
    } else {
        Ok(parse_quote! {
            #call_with_definition!(remote_derive_inner, #remote_derive);
        })
    }
}
