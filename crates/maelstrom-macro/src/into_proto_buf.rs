use darling::{FromDeriveInput, FromField, FromVariant};
use proc_macro2::Span;
use syn::{parse_quote, Arm, DeriveInput, Error, Expr, Ident, ItemImpl, Path, Result};

fn into_proto_buf_struct(
    self_ident: Path,
    option_all: bool,
    proto_buf_type: Path,
    fields: darling::ast::Fields<IntoProtoBufStructField>,
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
        if f.option || f.default || option_all {
            parse_quote! {
                crate::IntoProtoBuf::into_proto_buf(::std::option::Option::Some(self.#field))
            }
        } else {
            parse_quote! {
                crate::IntoProtoBuf::into_proto_buf(self.#field)
            }
        }
    });

    Ok(parse_quote! {
        impl crate::IntoProtoBuf for #self_ident {
            type ProtoBufType = #proto_buf_type;

            fn into_proto_buf(self) -> Self::ProtoBufType {
                #proto_buf_type {
                    #(#field_idents: #exprs),*
                }
            }
        }
    })
}

fn into_proto_buf_struct_try_from_into(self_ident: Path, proto_buf_type: Path) -> Result<ItemImpl> {
    Ok(parse_quote! {
        impl crate::IntoProtoBuf for #self_ident {
            type ProtoBufType = #proto_buf_type;

            fn into_proto_buf(self) -> Self::ProtoBufType {
                ::std::convert::Into::into(self)
            }
        }
    })
}

fn into_proto_buf_unit_enum(
    self_path: Path,
    proto_buf_type: Path,
    variants: Vec<IntoProtoBufEnumVariant>,
) -> Result<ItemImpl> {
    let arms = variants.iter().map(|v| -> Arm {
        let variant_ident = &v.ident;
        parse_quote! {
            Self::#variant_ident => #proto_buf_type::#variant_ident as i32
        }
    });

    Ok(parse_quote! {
        impl crate::IntoProtoBuf for #self_path {
            type ProtoBufType = i32;

            fn into_proto_buf(self) -> Self::ProtoBufType {
                match self {
                    #(#arms),*
                }
            }
        }
    })
}

fn into_proto_buf_enum(
    self_path: Path,
    proto_buf_type: Path,
    enum_type: Path,
    variants: Vec<IntoProtoBufEnumVariant>,
) -> Result<ItemImpl> {
    let arms = variants
        .iter()
        .map(|v| -> Result<Arm> {
            let variant_ident = &v.ident;
            let field_idents1 = v.fields.iter().map(|f| &f.ident);
            let field_idents2 = field_idents1.clone();
            Ok(match v.fields.style {
                darling::ast::Style::Unit => {
                    parse_quote! {
                        Self::#variant_ident => #enum_type::#variant_ident(proto::Void {})
                    }
                }
                darling::ast::Style::Tuple => {
                    let num_fields = v.fields.len();
                    let field_idents1 =
                        (0..num_fields).map(|n| Ident::new(&format!("f{n}"), Span::call_site()));
                    let field_idents2 = field_idents1.clone();
                    parse_quote! {
                        Self::#variant_ident(#(#field_idents1),*) => #enum_type::#variant_ident(
                            #(crate::IntoProtoBuf::into_proto_buf(#field_idents2)),*
                        )
                    }
                }
                darling::ast::Style::Struct => {
                    let variant_type = v
                        .other_type
                        .as_ref()
                        .ok_or(Error::new(variant_ident.span(), "missing other_type"))?;
                    let field_exprs = v.fields.iter().map(|v| -> Expr {
                        let ident = &v.ident;
                        if v.option || v.default {
                            parse_quote! { ::std::option::Option::Some(
                                crate::IntoProtoBuf::into_proto_buf(#ident)
                            ) }
                        } else {
                            parse_quote! { crate::IntoProtoBuf::into_proto_buf(#ident) }
                        }
                    });
                    parse_quote! {
                        Self::#variant_ident { #(#field_idents1),* } => #enum_type::#variant_ident(
                            #variant_type {
                                #(#field_idents2: #field_exprs),*
                            }
                        )
                    }
                }
            })
        })
        .collect::<Result<Vec<Arm>>>()?;

    Ok(parse_quote! {
        impl crate::IntoProtoBuf for #self_path {
            type ProtoBufType = #proto_buf_type;

            fn into_proto_buf(self) -> Self::ProtoBufType {
                <#proto_buf_type as ::std::convert::From<#enum_type>>::from(match self {
                    #(#arms),*
                })
            }
        }
    })
}

pub fn main(input: DeriveInput) -> Result<ItemImpl> {
    let input = IntoProtoBufInput::from_derive_input(&input)?;

    let self_path = input.ident.into();
    let proto_buf_type = input.other_type;

    match input.data {
        darling::ast::Data::Struct(fields) => {
            if input.try_from_into {
                into_proto_buf_struct_try_from_into(self_path, proto_buf_type)
            } else {
                into_proto_buf_struct(self_path, input.option_all, proto_buf_type, fields)
            }
        }
        darling::ast::Data::Enum(variants) => {
            let all_unit = variants
                .iter()
                .all(|f| f.fields.style == darling::ast::Style::Unit);
            if all_unit {
                into_proto_buf_unit_enum(self_path, proto_buf_type, variants)
            } else {
                let enum_type = input.enum_type.unwrap_or(proto_buf_type.clone());
                into_proto_buf_enum(self_path, proto_buf_type, enum_type, variants)
            }
        }
    }
}

#[derive(Clone, Debug, FromField)]
#[darling(attributes(proto))]
struct IntoProtoBufStructField {
    ident: Option<Ident>,
    #[darling(default)]
    option: bool,
    #[darling(default)]
    default: bool,
}

#[derive(Clone, Debug, FromVariant)]
#[darling(attributes(proto))]
struct IntoProtoBufEnumVariant {
    ident: Ident,
    fields: darling::ast::Fields<IntoProtoBufStructField>,
    other_type: Option<Path>,
}

#[derive(Clone, Debug, FromDeriveInput)]
#[darling(supports(struct_any, enum_any))]
#[darling(attributes(proto))]
struct IntoProtoBufInput {
    ident: Ident,
    data: darling::ast::Data<IntoProtoBufEnumVariant, IntoProtoBufStructField>,
    other_type: Path,
    enum_type: Option<Path>,
    #[darling(default)]
    option_all: bool,
    #[darling(default)]
    try_from_into: bool,
}
