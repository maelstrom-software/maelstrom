use darling::{FromDeriveInput, FromField, FromVariant};
use proc_macro2::Span;
use syn::{parse_quote, Arm, DeriveInput, Error, Expr, Ident, ItemImpl, Path, Result};

fn try_from_proto_buf_struct(
    self_ident: Path,
    option_all: bool,
    proto_buf_type: Path,
    fields: darling::ast::Fields<TryFromProtoBufStructField>,
) -> Result<ItemImpl> {
    let field_idents = fields
        .fields
        .iter()
        .map(|f| {
            f.ident
                .as_ref()
                .ok_or_else(|| Error::new(Span::call_site(), "named fields required"))
        })
        .collect::<Result<Vec<_>>>()?;
    let exprs = fields.fields.iter().map(|f| -> Expr {
        let field = f.ident.as_ref().unwrap();
        if f.option || option_all {
            let field_name = field.to_string();
            parse_quote! {
                crate::TryFromProtoBuf::try_from_proto_buf(
                    p.#field.ok_or(::anyhow::anyhow!("missing field {}", #field_name))?
                )?
            }
        } else if f.default {
            parse_quote! {
                if let ::std::option::Option::Some(v) = p.#field {
                    crate::TryFromProtoBuf::try_from_proto_buf(v)?
                } else {
                    ::std::default::Default::default()
                }
            }
        } else {
            parse_quote! {
                crate::TryFromProtoBuf::try_from_proto_buf(p.#field)?
            }
        }
    });

    Ok(parse_quote! {
        impl crate::TryFromProtoBuf for #self_ident {
            type ProtoBufType = #proto_buf_type;

            fn try_from_proto_buf(p: Self::ProtoBufType) -> ::anyhow::Result<Self> {
                Ok(Self {
                    #(#field_idents: #exprs),*
                })
            }
        }
    })
}

fn try_from_proto_buf_struct_try_from_into(
    self_ident: Path,
    proto_buf_type: Path,
) -> Result<ItemImpl> {
    Ok(parse_quote! {
        impl crate::TryFromProtoBuf for #self_ident {
            type ProtoBufType = #proto_buf_type;

            fn try_from_proto_buf(p: Self::ProtoBufType) -> ::anyhow::Result<Self> {
                ::std::result::Result::map_err(
                    ::std::convert::TryFrom::try_from(p),
                    |_| {
                        ::anyhow::anyhow!(
                            ::std::concat!("malformed ", ::std::stringify!(#self_ident))
                        )
                    }
                )
            }
        }
    })
}

fn try_from_proto_buf_unit_enum(
    self_path: Path,
    proto_buf_type: Path,
    variants: Vec<TryFromProtoBufEnumVariant>,
) -> Result<ItemImpl> {
    let arms = variants.iter().map(|v| -> Arm {
        let variant_ident = &v.ident;
        parse_quote! {
            Ok(#proto_buf_type::#variant_ident) => Ok(Self::#variant_ident)
        }
    });

    let self_name = self_path.segments.last().unwrap().ident.to_string();
    Ok(parse_quote! {
        impl crate::TryFromProtoBuf for #self_path {
            type ProtoBufType = i32;

            fn try_from_proto_buf(p: Self::ProtoBufType) -> ::anyhow::Result<Self> {
                match #proto_buf_type::try_from(p) {
                    #(#arms,)*
                    _ => Err(::anyhow::anyhow!("malformed `{}`", #self_name))
                }
            }
        }
    })
}

fn try_from_proto_buf_enum(
    self_path: Path,
    proto_buf_type: Path,
    enum_type: Path,
    variants: Vec<TryFromProtoBufEnumVariant>,
) -> Result<ItemImpl> {
    let self_name = self_path.segments.last().unwrap().ident.to_string();
    let arms = variants
        .iter()
        .map(|v| -> Result<Arm> {
            let variant_ident = &v.ident;
            Ok(match v.fields.style {
                darling::ast::Style::Unit => {
                    parse_quote! {
                        #enum_type::#variant_ident(_) => Ok(Self::#variant_ident)
                    }
                }
                darling::ast::Style::Tuple => {
                    let num_fields = v.fields.len();
                    let field_idents1 =
                        (0..num_fields).map(|n| Ident::new(&format!("f{n}"), Span::call_site()));
                    let field_idents2 = field_idents1.clone();
                    parse_quote! {
                        #enum_type::#variant_ident(#(#field_idents1),*) => Ok(Self::#variant_ident(
                            #(crate::TryFromProtoBuf::try_from_proto_buf(#field_idents2)?),*
                        ))
                    }
                }
                darling::ast::Style::Struct => {
                    let field_idents1 = v.fields.iter().map(|f| &f.ident);
                    let field_idents2 = field_idents1.clone();
                    let variant_type = v
                        .other_type
                        .as_ref()
                        .ok_or(Error::new(variant_ident.span(), "missing path_type"))?;
                    let field_exprs = v.fields.iter().map(|v| -> Expr {
                        let ident = &v.ident;
                        if v.option {
                            parse_quote! {
                                crate::TryFromProtoBuf::try_from_proto_buf(
                                    #ident.ok_or(::anyhow::anyhow!("malformed `{}`", #self_name))?
                                )?
                            }
                        } else if v.default {
                            parse_quote! {
                                if let ::std::option::Option::Some(v) = #ident {
                                    crate::TryFromProtoBuf::try_from_proto_buf(v)?
                                } else {
                                    ::std::default::Default::default()
                                }
                            }
                        } else {
                            parse_quote! { crate::TryFromProtoBuf::try_from_proto_buf(#ident)? }
                        }
                    });
                    parse_quote! {
                        #enum_type::#variant_ident(#variant_type { #(#field_idents1),* }) => {
                            Ok(Self::#variant_ident {
                                #(#field_idents2: #field_exprs),*
                            })
                        }
                    }
                }
            })
        })
        .collect::<Result<Vec<Arm>>>()?;

    Ok(parse_quote! {
        impl crate::TryFromProtoBuf for #self_path {
            type ProtoBufType = #proto_buf_type;

            fn try_from_proto_buf(p: Self::ProtoBufType) -> ::anyhow::Result<Self> {
                match <#enum_type as ::std::convert::TryFrom<#proto_buf_type>>::try_from(p)? {
                    #(#arms,)*
                    _ => Err(::anyhow::anyhow!("malformed `{}`", #self_name))
                }
            }
        }
    })
}

pub fn main(input: DeriveInput) -> Result<ItemImpl> {
    let input = TryFromProtoBufInput::from_derive_input(&input)?;

    let self_path = input.ident.into();
    let proto_buf_type = input.other_type;

    match input.data {
        darling::ast::Data::Struct(fields) => {
            if input.try_from_into {
                try_from_proto_buf_struct_try_from_into(self_path, proto_buf_type)
            } else {
                try_from_proto_buf_struct(self_path, input.option_all, proto_buf_type, fields)
            }
        }
        darling::ast::Data::Enum(variants) => {
            let all_unit = variants
                .iter()
                .all(|f| f.fields.style == darling::ast::Style::Unit);

            if all_unit {
                try_from_proto_buf_unit_enum(self_path, proto_buf_type, variants)
            } else {
                let enum_type = input.enum_type.unwrap_or(proto_buf_type.clone());
                try_from_proto_buf_enum(self_path, proto_buf_type, enum_type, variants)
            }
        }
    }
}

#[derive(Clone, Debug, FromField)]
#[darling(attributes(proto))]
struct TryFromProtoBufStructField {
    ident: Option<Ident>,
    #[darling(default)]
    option: bool,
    #[darling(default)]
    default: bool,
}

#[derive(Clone, Debug, FromVariant)]
#[darling(attributes(proto))]
struct TryFromProtoBufEnumVariant {
    ident: Ident,
    fields: darling::ast::Fields<TryFromProtoBufStructField>,
    other_type: Option<Path>,
}

#[derive(Clone, Debug, FromDeriveInput)]
#[darling(supports(struct_any, enum_any))]
#[darling(attributes(proto))]
struct TryFromProtoBufInput {
    ident: Ident,
    data: darling::ast::Data<TryFromProtoBufEnumVariant, TryFromProtoBufStructField>,
    /// This is the protobuf type we are converting from
    other_type: Path,
    /// If the protobuf type isn't an enum, but instead a struct containing an enum, this can be
    /// used to unpack the struct. It causes the code to use TryFrom on the protobuf to convert to
    /// this enum type, and uses this type for the conversion.
    enum_type: Option<Path>,
    #[darling(default)]
    option_all: bool,
    #[darling(default)]
    try_from_into: bool,
}
