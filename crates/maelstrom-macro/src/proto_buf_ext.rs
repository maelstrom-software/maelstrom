use darling::{ast::NestedMeta, util::path_to_string, FromDeriveInput, FromField, FromMeta};
use syn::{parse_quote, DeriveInput, Field, Ident, ItemImpl, Path, Result};

/// If a message struct is just a wrapper around a single `oneof` field, we want to generated some
/// useful conversion between the enum type and the struct type. This can then be leveraged by the
/// `enum_type` attribute in the [`crate::TryFromProtoBuf`] and [`crate::IntoProtoBuf`] macros.
fn oneof_wrapper(self_path: &Path, field_ident: &Ident, enum_path: &Path) -> Result<Vec<ItemImpl>> {
    Ok(vec![
        parse_quote! {
            impl ::std::convert::From<#enum_path> for #self_path {
                fn from(f: #enum_path) -> Self {
                    Self {
                        #field_ident: ::std::option::Option::Some(f),
                    }
                }
            }
        },
        parse_quote! {
            impl ::std::convert::TryFrom<#self_path> for #enum_path {
                type Error = ::anyhow::Error;

                fn try_from(f: #self_path) -> ::anyhow::Result<Self> {
                    ::std::option::Option::ok_or_else(
                        f.#field_ident,
                        || {
                            ::anyhow::anyhow!(
                                ::std::concat!("malformed ", ::std::stringify!(#self_path))
                            )
                        }
                    )
                }
            }
        },
    ])
}

fn proto_buf_ext_struct(
    self_path: Path,
    fields: darling::ast::Fields<ProtoBufExtField>,
) -> Result<Vec<ItemImpl>> {
    let mut impls = vec![];
    if fields.len() == 1 {
        // check if the struct has a single field which is a `oneof`
        let single_field = &fields.fields[0];
        let field_ident = single_field.ident.as_ref().expect("struct_named required");
        if let Some(enum_path) = &single_field.oneof {
            impls.extend(oneof_wrapper(&self_path, field_ident, enum_path)?);
        }
    }
    Ok(impls)
}

pub fn main(input: DeriveInput) -> Result<Vec<ItemImpl>> {
    let input = ProtoBufExtInput::from_derive_input(&input)?;

    let self_path = input.ident.into();

    match input.data {
        darling::ast::Data::Struct(fields) => proto_buf_ext_struct(self_path, fields),
        _ => Ok(vec![]),
    }
}

#[derive(Clone, Debug)]
struct ProtoBufExtField {
    ident: Option<Ident>,
    oneof: Option<Path>,
}

/// We are implementing this manually because we want to ignore unknown attributes. We are spying
/// on attributes for `prost` macros and we only want to look at whatever ones we are interested in
/// to avoid being too restrictive.
impl FromField for ProtoBufExtField {
    fn from_field(field: &Field) -> darling::Result<Self> {
        let mut oneof: Option<Path> = None;
        for attr in &field.attrs {
            if path_to_string(attr.path()) != "prost" {
                continue;
            }
            let data = darling::util::parse_attribute_to_meta_list(attr)?;
            for item in NestedMeta::parse_meta_list(data.tokens)? {
                match item {
                    #[allow(clippy::single_match)]
                    NestedMeta::Meta(ref inner) => match path_to_string(inner.path()).as_str() {
                        "oneof" => {
                            oneof = FromMeta::from_meta(inner)
                                .map_err(|e| e.with_span(&inner).at("oneof"))?;
                        }
                        _ => {}
                    },
                    NestedMeta::Lit(ref inner) => {
                        return Err(darling::Error::unsupported_format("literal").with_span(inner));
                    }
                }
            }
        }
        Ok(Self {
            ident: field.ident.clone(),
            oneof,
        })
    }
}

#[derive(Clone, Debug, FromDeriveInput)]
#[darling(supports(struct_named, enum_any))]
#[darling(attributes(prost))]
struct ProtoBufExtInput {
    ident: Ident,
    data: darling::ast::Data<(), ProtoBufExtField>,
}
