use darling::{FromDeriveInput, FromField, FromVariant};
use proc_macro2::Span;
use syn::{parse_quote, Arm, DeriveInput, Error, Expr, Ident, ItemImpl, Path, Result, Type};

fn into_proto_buf_struct(
    mut self_ident: Path,
    option_all: bool,
    remote: bool,
    mut proto_buf_type: Path,
    fields: darling::ast::Fields<IntoProtoBufStructField>,
) -> Result<ItemImpl> {
    if remote {
        std::mem::swap(&mut self_ident, &mut proto_buf_type);
    }
    let field_idents = fields.fields.iter().map(|f| f.ident.as_ref().unwrap());
    let exprs = fields.fields.iter().map(|f| -> Expr {
        let field = f.ident.as_ref().unwrap();
        if f.option || option_all {
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

fn into_proto_buf_unit_enum(
    mut self_path: Path,
    remote: bool,
    mut proto_buf_type: Path,
    variants: Vec<IntoProtoBufEnumVariant>,
) -> Result<ItemImpl> {
    if remote {
        std::mem::swap(&mut self_path, &mut proto_buf_type);
    }

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
    mut self_path: Path,
    remote: bool,
    mut proto_buf_type: Path,
    variants: Vec<IntoProtoBufEnumVariant>,
) -> Result<ItemImpl> {
    if remote {
        std::mem::swap(&mut self_path, &mut proto_buf_type);
    }

    let arms = variants.iter().map(|v| -> Result<Arm> {
        let variant_ident = &v.ident;
        let field_idents1 = v.fields.iter().map(|f| &f.ident);
        let field_idents2 = field_idents1.clone();
        Ok(match v.fields.style {
            darling::ast::Style::Unit => {
                return Err(Error::new(variant_ident.span(), "unit not allowed here"));
            }
            darling::ast::Style::Tuple => {
                let num_fields = v.fields.len();
                let void = parse_quote!(super::Void);
                if num_fields == 1 && v.fields.fields.first().unwrap().ty == void {
                    return Ok(parse_quote! {
                        Self::#variant_ident => #proto_buf_type::#variant_ident(super::Void {}),
                    });
                }

                let field_idents1 = (0..num_fields).map(|n| {
                    Ident::new(&format!("f{n}"), Span::call_site())
                });
                let field_idents2 = field_idents1.clone();
                parse_quote! {
                    Self::#variant_ident(#(#field_idents1),*) => #proto_buf_type::#variant_ident(
                        #(crate::IntoProtoBuf::into_proto_buf(#field_idents2)),*
                    )
                }
            }
            darling::ast::Style::Struct => {
                let variant_type = v
                    .other_type
                    .as_ref()
                    .ok_or(Error::new(variant_ident.span(), "missing path_type"))?;
                let field_exprs = v.fields.iter().map(|v| -> Expr {
                    let ident = &v.ident;
                    if v.option {
                        parse_quote!{ ::std::option::Option::Some(
                            crate::IntoProtoBuf::into_proto_buf(#ident)
                        ) }
                    } else {
                        parse_quote!{ crate::IntoProtoBuf::into_proto_buf(#ident) }
                    }
                });
                parse_quote! {
                    Self::#variant_ident { #(#field_idents1),* } => #proto_buf_type::#variant_ident(
                        #variant_type {
                            #(#field_idents2: #field_exprs),*
                        }
                    )
                }
            }
        })
    }).collect::<Result<Vec<Arm>>>()?;

    Ok(parse_quote! {
        impl crate::IntoProtoBuf for #self_path {
            type ProtoBufType = #proto_buf_type;

            fn into_proto_buf(self) -> Self::ProtoBufType {
                match self {
                    #(#arms),*
                }
            }
        }
    })
}

pub fn main(input: DeriveInput) -> Result<ItemImpl> {
    let input = IntoProtoBufInput::from_derive_input(&input)?;

    let self_path = input.ident.into();
    match input.data {
        darling::ast::Data::Struct(fields) => into_proto_buf_struct(
            self_path,
            input.option_all,
            input.remote,
            input.other_type,
            fields,
        ),
        darling::ast::Data::Enum(variants) => {
            let all_unit = variants
                .iter()
                .all(|f| f.fields.style == darling::ast::Style::Unit);
            if all_unit {
                into_proto_buf_unit_enum(self_path, input.remote, input.other_type, variants)
            } else {
                into_proto_buf_enum(self_path, input.remote, input.other_type, variants)
            }
        }
    }
}

#[derive(Clone, Debug, FromField)]
#[darling(attributes(proto))]
struct IntoProtoBufStructField {
    ident: Option<Ident>,
    ty: Type,
    #[darling(default)]
    option: bool,
}

#[derive(Clone, Debug, FromVariant)]
#[darling(attributes(proto))]
struct IntoProtoBufEnumVariant {
    ident: Ident,
    fields: darling::ast::Fields<IntoProtoBufStructField>,
    other_type: Option<Path>,
}

#[derive(Clone, Debug, FromDeriveInput)]
#[darling(supports(struct_named, enum_any))]
#[darling(attributes(proto))]
struct IntoProtoBufInput {
    ident: Ident,
    data: darling::ast::Data<IntoProtoBufEnumVariant, IntoProtoBufStructField>,
    other_type: Path,
    #[darling(default)]
    remote: bool,
    #[darling(default)]
    option_all: bool,
}
