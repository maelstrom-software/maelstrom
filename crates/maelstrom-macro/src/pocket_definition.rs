use darling::{ast::NestedMeta, FromMeta};
use heck::ToSnakeCase as _;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{parse_quote, Error, Ident, Item, ItemMacro, Result};

fn get_item_name(item: &Item) -> Result<&Ident> {
    match item {
        Item::Struct(s) => Ok(&s.ident),
        Item::Enum(e) => Ok(&e.ident),
        Item::Trait(t) => Ok(&t.ident),
        _ => Err(Error::new(
            Span::call_site(),
            "item unsupported for export_definition",
        )),
    }
}

#[derive(FromMeta)]
struct Attributes {
    #[darling(default)]
    export: bool,
}

pub fn main(item: &Item, attrs: TokenStream) -> Result<ItemMacro> {
    let attrs = Attributes::from_list(&NestedMeta::parse_meta_list(attrs)?)?;
    let name = get_item_name(item)?;
    let macro_name = Ident::new(
        &format!("{}_call_with_definition", name.to_string().to_snake_case()),
        Span::call_site(),
    );
    let macro_export = attrs.export.then_some(quote!(#[macro_export]));
    Ok(parse_quote! {
        #macro_export
        macro_rules! #macro_name {
            ($macro_name:tt) => {
                $macro_name!(#item);
            }
        }
    })
}
