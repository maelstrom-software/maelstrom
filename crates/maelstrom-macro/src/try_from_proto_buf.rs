use darling::{FromDeriveInput, FromField, FromVariant};
use proc_macro2::Span;
use syn::{parse_quote, Arm, DeriveInput, Error, Expr, Ident, ItemImpl, Path, Result, Type};

fn try_from_proto_buf_struct(
    mut self_ident: Path,
    option_all: bool,
    reverse: bool,
    mut proto_buf_type: Path,
    fields: darling::ast::Fields<TryFromProtoBufStructField>,
) -> Result<ItemImpl> {
    if reverse {
        std::mem::swap(&mut self_ident, &mut proto_buf_type);
    }
    let field_idents = fields.fields.iter().map(|f| f.ident.as_ref().unwrap());
    let exprs = fields.fields.iter().map(|f| -> Expr {
        let field = f.ident.as_ref().unwrap();
        if f.option || option_all {
            let field_name = field.to_string();
            parse_quote! {
                crate::TryFromProtoBuf::try_from_proto_buf(
                    p.#field.ok_or(::anyhow::anyhow!("missing field {}", #field_name))?
                )?
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

fn try_from_proto_buf_unit_enum(
    mut self_path: Path,
    reverse: bool,
    mut proto_buf_type: Path,
    variants: Vec<TryFromProtoBufEnumVariant>,
) -> Result<ItemImpl> {
    if reverse {
        std::mem::swap(&mut self_path, &mut proto_buf_type);
    }

    let arms = variants.iter().map(|v| -> Arm {
        let variant_ident = &v.ident;
        parse_quote! {
            Ok(#proto_buf_type::#variant_ident) => Ok(Self::#variant_ident)
        }
    });

    Ok(parse_quote! {
        impl crate::TryFromProtoBuf for #self_path {
            type ProtoBufType = i32;

            fn try_from_proto_buf(p: Self::ProtoBufType) -> ::anyhow::Result<Self> {
                match #proto_buf_type::try_from(p) {
                    #(#arms,)*
                    _ => Err(::anyhow::anyhow!("malformed response"))
                }
            }
        }
    })
}

fn try_from_proto_buf_enum(
    mut self_path: Path,
    reverse: bool,
    mut proto_buf_type: Path,
    variants: Vec<TryFromProtoBufEnumVariant>,
) -> Result<ItemImpl> {
    if reverse {
        std::mem::swap(&mut self_path, &mut proto_buf_type);
    }

    let arms = variants.iter().map(|v| -> Result<Arm> {
        let variant_ident = &v.ident;
        Ok(match v.fields.style {
            darling::ast::Style::Unit => {
                return Err(Error::new(variant_ident.span(), "unit not allowed here"));
            }
            darling::ast::Style::Tuple => {
                let num_fields = v.fields.len();
                let void = parse_quote!(super::Void);
                if num_fields == 1 && v.fields.fields.first().unwrap().ty == void {
                    return Ok(parse_quote! {
                        #proto_buf_type::#variant_ident(_) => Ok(Self::#variant_ident)
                    });
                }

                let field_idents1 = (0..num_fields).map(|n| {
                    Ident::new(&format!("f{n}"), Span::call_site())
                });
                let field_idents2 = field_idents1.clone();
                parse_quote! {
                    #proto_buf_type::#variant_ident(#(#field_idents1),*) => Ok(Self::#variant_ident(
                        #(crate::TryFromProtoBuf::try_from_proto_buf(#field_idents2)?),*
                    ))
                }
            }
            darling::ast::Style::Struct => {
                let field_idents1 = v.fields.iter().map(|f| &f.ident);
                let field_idents2 = field_idents1.clone();
                let variant_type = v
                    .type_path
                    .as_ref()
                    .ok_or(Error::new(variant_ident.span(), "missing path_type"))?;
                let field_exprs = v.fields.iter().map(|v| -> Expr {
                    let ident = &v.ident;
                    if v.option {
                        parse_quote!{
                            crate::TryFromProtoBuf::try_from_proto_buf(
                                #ident.ok_or(::anyhow::anyhow!("malformed response"))?
                            )?
                        }
                    } else {
                        parse_quote!{ crate::TryFromProtoBuf::try_from_proto_buf(#ident)? }
                    }
                });
                parse_quote! {
                    #proto_buf_type::#variant_ident(#variant_type { #(#field_idents1),* }) => {
                        Ok(Self::#variant_ident {
                            #(#field_idents2: #field_exprs),*
                        })
                    }
                }
            }
        })
    }).collect::<Result<Vec<Arm>>>()?;

    Ok(parse_quote! {
        impl crate::TryFromProtoBuf for #self_path {
            type ProtoBufType = #proto_buf_type;

            fn try_from_proto_buf(p: Self::ProtoBufType) -> ::anyhow::Result<Self> {
                match p {
                    #(#arms,)*
                    _ => Err(::anyhow::anyhow!("malformed response"))
                }
            }
        }
    })
}

pub fn main(input: DeriveInput) -> Result<ItemImpl> {
    let input = TryFromProtoBufInput::from_derive_input(&input)?;

    let self_path = input.ident.into();
    match input.data {
        darling::ast::Data::Struct(fields) => try_from_proto_buf_struct(
            self_path,
            input.option_all,
            input.reverse,
            input.type_path,
            fields,
        ),
        darling::ast::Data::Enum(variants) => {
            let all_unit = variants
                .iter()
                .all(|f| f.fields.style == darling::ast::Style::Unit);

            if all_unit {
                try_from_proto_buf_unit_enum(self_path, input.reverse, input.type_path, variants)
            } else {
                try_from_proto_buf_enum(self_path, input.reverse, input.type_path, variants)
            }
        }
    }
}

#[derive(Clone, Debug, FromField)]
#[darling(attributes(proto))]
struct TryFromProtoBufStructField {
    ident: Option<Ident>,
    ty: Type,
    #[darling(default)]
    option: bool,
}

#[derive(Clone, Debug, FromVariant)]
#[darling(attributes(proto))]
struct TryFromProtoBufEnumVariant {
    ident: Ident,
    fields: darling::ast::Fields<TryFromProtoBufStructField>,
    type_path: Option<Path>,
}

#[derive(Clone, Debug, FromDeriveInput)]
#[darling(supports(struct_named, enum_any))]
#[darling(attributes(proto))]
struct TryFromProtoBufInput {
    ident: Ident,
    data: darling::ast::Data<TryFromProtoBufEnumVariant, TryFromProtoBufStructField>,
    type_path: Path,
    #[darling(default)]
    reverse: bool,
    #[darling(default)]
    option_all: bool,
}
