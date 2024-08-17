use heck::ToSnakeCase as _;
use proc_macro2::Span;
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_quote,
    punctuated::Punctuated,
    token::Paren,
    Attribute, Data, DeriveInput, Error, Ident, ItemMacro, Meta, Result, Token,
};

struct DeriveFieldAttribute {
    at_token: Token![@],
    field_or_variant: Ident,
    colon_token: Token![:],
    attr: Meta,
}

impl Parse for DeriveFieldAttribute {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            at_token: input.parse()?,
            field_or_variant: input.parse()?,
            colon_token: input.parse()?,
            attr: input.parse()?,
        })
    }
}

impl quote::ToTokens for DeriveFieldAttribute {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        self.at_token.to_tokens(tokens);
        self.field_or_variant.to_tokens(tokens);
        self.colon_token.to_tokens(tokens);
        self.attr.to_tokens(tokens);
    }
}

enum DeriveAttribute {
    Container(Meta),
    FieldOrVariant(DeriveFieldAttribute),
}

impl Parse for DeriveAttribute {
    fn parse(input: ParseStream) -> Result<Self> {
        if input.peek(Token![@]) {
            Ok(Self::FieldOrVariant(input.parse()?))
        } else {
            Ok(Self::Container(input.parse()?))
        }
    }
}

impl quote::ToTokens for DeriveAttribute {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            Self::Container(m) => m.to_tokens(tokens),
            Self::FieldOrVariant(f) => f.to_tokens(tokens),
        }
    }
}

struct DeriveAttrs {
    _comma_token: Token![,],
    attrs: Punctuated<DeriveAttribute, Token![,]>,
}

impl DeriveAttrs {
    fn container_attrs(&self) -> impl Iterator<Item = &Meta> {
        self.attrs.iter().filter_map(|a| {
            if let DeriveAttribute::Container(a) = a {
                Some(a)
            } else {
                None
            }
        })
    }

    fn field_or_variant_attrs(&self) -> impl Iterator<Item = &DeriveFieldAttribute> {
        self.attrs.iter().filter_map(|a| {
            if let DeriveAttribute::FieldOrVariant(a) = a {
                Some(a)
            } else {
                None
            }
        })
    }
}

pub struct Arguments {
    remote_type: Ident,
    _comma_token: Token![,],
    derive_macros: Punctuated<Ident, Token![,]>,
    derive_attrs: Option<DeriveAttrs>,
}

impl Parse for Arguments {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            remote_type: input.parse()?,
            _comma_token: input.parse()?,
            derive_macros: if input.peek(Paren) {
                let content;
                parenthesized!(content in input);
                content.parse_terminated(Ident::parse, Token![,])?
            } else {
                [input.parse::<Ident>()?].into_iter().collect()
            },
            derive_attrs: input
                .peek(Token![,])
                .then(|| -> Result<_> {
                    Ok(DeriveAttrs {
                        _comma_token: input.parse()?,
                        attrs: input.parse_terminated(DeriveAttribute::parse, Token![,])?,
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
                                DeriveAttribute::parse(&content)
                            },
                            Token![,],
                        )?,
                    })
                })
                .transpose()?,
        })
    }
}

fn add_field_or_variant_attribute(
    data: &mut Data,
    field_or_variant_to_find: &Ident,
    attr: &Meta,
) -> Result<()> {
    match data {
        Data::Struct(s) => {
            let mut found = false;
            for f in s.fields.iter_mut() {
                let field_ident = f.ident.as_ref().ok_or_else(|| {
                    Error::new(
                        Span::call_site(),
                        "field attributes not supported for unnamed fields",
                    )
                })?;
                if field_ident == field_or_variant_to_find {
                    f.attrs.push(parse_quote!(#[#attr]));
                    found = true;
                    break;
                }
            }
            if !found {
                Err(Error::new(
                    field_or_variant_to_find.span(),
                    "failed to find field",
                ))
            } else {
                Ok(())
            }
        }
        Data::Enum(e) => {
            let mut found = false;
            for v in &mut e.variants {
                if &v.ident == field_or_variant_to_find {
                    v.attrs.push(parse_quote!(#[#attr]));
                    found = true;
                    break;
                }
            }
            if !found {
                Err(Error::new(
                    field_or_variant_to_find.span(),
                    "failed to find variant",
                ))
            } else {
                Ok(())
            }
        }
        _ => Err(Error::new(
            Span::call_site(),
            "field attributes not supported for enum or union",
        )),
    }
}

pub fn inner_main(args: InnerArguments) -> Result<ItemMacro> {
    let remote_derive = &args.remote_derive;
    let mut input = args.input;
    if let Some(attrs) = args.derive_attrs {
        let container_attrs = attrs
            .container_attrs()
            .map(|a| -> Attribute { parse_quote!(#[#a]) });
        for f in attrs.field_or_variant_attrs() {
            add_field_or_variant_attribute(&mut input.data, &f.field_or_variant, &f.attr)?;
        }
        Ok(parse_quote! {
            #remote_derive!(
                #(#container_attrs)*
                #input
            );
        })
    } else {
        Ok(parse_quote! {
            #remote_derive!(#input);
        })
    }
}

pub fn main(args: Arguments) -> Result<Vec<ItemMacro>> {
    let mut items = vec![];
    for derive_macro in args.derive_macros {
        let pocket_definition = Ident::new(
            &format!(
                "{}_pocket_definition",
                args.remote_type.to_string().to_snake_case()
            ),
            Span::call_site(),
        );
        let remote_derive = Ident::new(
            &format!("{}_remote_derive", derive_macro.to_string().to_snake_case(),),
            Span::call_site(),
        );
        if let Some(attrs) = &args.derive_attrs {
            let attrs = attrs.attrs.iter();
            items.push(parse_quote! {
                #pocket_definition!(
                    maelstrom_macro::remote_derive_inner, #remote_derive, #((#attrs)),*
                );
            });
        } else {
            items.push(parse_quote! {
                #pocket_definition!(maelstrom_macro::remote_derive_inner, #remote_derive);
            });
        }
    }
    Ok(items)
}
