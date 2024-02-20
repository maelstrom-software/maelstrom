use proc_macro2::Span;
use syn::{
    parse_quote, spanned::Spanned as _, Data, DataEnum, DataStruct, DeriveInput, Error, Fields,
    Ident, ItemImpl, Result,
};

fn into_result_struct(self_ident: Ident, struct_data: DataStruct) -> Result<ItemImpl> {
    let Fields::Named(fields) = struct_data.fields else {
        return Err(Error::new(
            struct_data.fields.span(),
            "unnamed fields not supported",
        ));
    };
    if fields.named.is_empty() {
        return Err(Error::new(fields.span(), "unit structs not supported"));
    }
    if fields.named.len() == 1 {
        let single_field = fields.named.first().unwrap();
        let field_ident = &single_field.ident;
        let field_type = &single_field.ty;
        Ok(parse_quote! {
            impl crate::IntoResult for #self_ident {
                type Output = <#field_type as crate::IntoResult>::Output;

                fn into_result(self) -> ::anyhow::Result<Self::Output> {
                    crate::IntoResult::into_result(self.#field_ident)
                }
            }
        })
    } else {
        let field_idents = fields.named.iter().map(|f| &f.ident);
        let field_types = fields.named.iter().map(|f| &f.ty);
        Ok(parse_quote! {
            impl crate::IntoResult for #self_ident {
                type Output = <(#(#field_types),*) as crate::IntoResult>::Output;

                fn into_result(self) -> ::anyhow::Result<Self::Output> {
                    crate::IntoResult::into_result((#(self.#field_idents),*))
                }
            }
        })
    }
}

fn into_result_enum(self_ident: Ident, enum_data: DataEnum) -> Result<ItemImpl> {
    if enum_data.variants.len() != 2 {
        return Err(Error::new(
            enum_data.variants.span(),
            "expected only two variants",
        ));
    }
    if !enum_data.variants.iter().any(|v| v.ident == "Error") {
        return Err(Error::new(
            enum_data.variants.span(),
            "error variant not found",
        ));
    }
    let value_variant = enum_data
        .variants
        .iter()
        .find(|v| v.ident != "Error")
        .ok_or(Error::new(
            enum_data.variants.span(),
            "value variant not found",
        ))?;
    let value_ident = &value_variant.ident;
    if value_variant.fields.len() != 1 {
        return Err(Error::new(
            value_variant.fields.span(),
            "expected single field variant",
        ));
    }
    let value_type = &value_variant.fields.iter().next().unwrap().ty;

    Ok(parse_quote! {
        impl crate::IntoResult for #self_ident {
            type Output = #value_type;

            fn into_result(self) -> ::anyhow::Result<Self::Output> {
                match self {
                    Self::Error(e) => ::anyhow::Result::Err(::std::convert::Into::into(e)),
                    Self::#value_ident(v) => ::anyhow::Result::Ok(v),
                }
            }
        }
    })
}

pub fn main(input: DeriveInput) -> Result<ItemImpl> {
    let self_ident = input.ident;
    match input.data {
        Data::Struct(struct_data) => into_result_struct(self_ident, struct_data),
        Data::Enum(enum_data) => into_result_enum(self_ident, enum_data),
        Data::Union(_) => Err(Error::new(Span::call_site(), "unions not supported")),
    }
}
