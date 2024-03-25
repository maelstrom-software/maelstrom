use darling::{ast::Data, FromDeriveInput, FromField};
use proc_macro2::Span;
use syn::{
    parse_quote, Attribute, DeriveInput, Expr, ExprLit, Ident, Item, ItemImpl, Lit, Meta,
    MetaNameValue, Path, Result, Type,
};

enum DefaultValue<'a> {
    None,
    Expression(&'a Expr),
    Closure(&'a Expr),
}

#[derive(Clone, Debug, FromField)]
#[darling(attributes(config), forward_attrs)]
struct ConfigStructField {
    ident: Option<Ident>,
    ty: Type,
    flag: Option<()>,
    option: Option<()>,
    flatten: Option<()>,
    short: Option<char>,
    value_name: Option<String>,
    default: Option<Expr>,
    next_help_heading: Option<String>,
    attrs: Vec<Attribute>,
}

impl ConfigStructField {
    fn ident(&self) -> &Ident {
        self.ident.as_ref().unwrap()
    }

    fn value_name(&self) -> Result<&str> {
        self.value_name
            .as_deref()
            .ok_or_else(|| syn::Error::new(self.ident().span(), "no value_name attribute found"))
    }

    fn default(&self) -> DefaultValue {
        match &self.default {
            Some(expr @ Expr::Closure(_)) => DefaultValue::Closure(expr),
            Some(expr) => DefaultValue::Expression(expr),
            None => DefaultValue::None,
        }
    }

    fn doc_comment(&self) -> Result<String> {
        let doc = self
            .attrs
            .iter()
            .filter_map(|attr| -> Option<String> {
                let Meta::NameValue(MetaNameValue { path, value, .. }) = &attr.meta else {
                    return None;
                };
                if !path.is_ident(&Ident::new("doc", Span::call_site())) {
                    return None;
                }
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(ref value),
                    ..
                }) = value
                else {
                    return None;
                };
                Some(value.value().trim().to_string())
            })
            .collect::<Vec<_>>()
            .join(" ");
        if doc.is_empty() {
            Err(syn::Error::new(
                self.ident.as_ref().unwrap().span(),
                "no documentation comment found",
            ))
        } else {
            Ok(doc)
        }
    }

    fn gen_builder_value_call(&self) -> Result<Expr> {
        let name = self.ident().to_string();
        let short: Expr = match &self.short {
            Some(short) => parse_quote!(Some(#short)),
            None => parse_quote!(None),
        };
        let value_name = self.value_name()?;
        let default: Expr = match self.default() {
            DefaultValue::Closure(closure) => {
                parse_quote!(Some((#closure)(base_directories).to_string()))
            }
            DefaultValue::Expression(expr) => {
                parse_quote!(Some((#expr).to_string()))
            }
            DefaultValue::None => parse_quote!(None),
        };
        let doc = self.doc_comment()?;
        Ok(parse_quote! {
            let builder = builder.value(
                #name,
                #short,
                #value_name,
                #default,
                #doc,
            )
        })
    }

    fn gen_builder_flag_value_call(&self) -> Result<Expr> {
        let name = self.ident().to_string();
        let short: Expr = match &self.short {
            Some(short) => parse_quote!(Some(#short)),
            None => parse_quote!(None),
        };
        let doc = self.doc_comment()?;
        Ok(parse_quote! {
            let builder = builder.flag_value(
                #name,
                #short,
                #doc,
            )
        })
    }

    fn gen_builder_option_value_call(&self) -> Result<Expr> {
        let name = self.ident().to_string();
        let short: Expr = match &self.short {
            Some(short) => parse_quote!(Some(#short)),
            None => parse_quote!(None),
        };
        let value_name = self.value_name()?;
        let default: Expr = match self.default() {
            DefaultValue::Closure(closure) => {
                parse_quote!(Some((#closure)(base_directories).to_string()))
            }
            DefaultValue::Expression(expr) => {
                parse_quote!(Some((#expr).to_string()))
            }
            DefaultValue::None => parse_quote!(None),
        };
        let doc = self.doc_comment()?;
        Ok(parse_quote! {
            let builder = builder.value(
                #name,
                #short,
                #value_name,
                #default,
                #doc,
            )
        })
    }

    fn gen_flatten_add_command_line_options_call(&self) -> Expr {
        let field_type = &self.ty;
        parse_quote! {
            let builder = #field_type::add_command_line_options(builder, base_directories)
        }
    }

    fn gen_flatten_from_config_bag_call(&self) -> Expr {
        let field_type = &self.ty;
        parse_quote! {
            #field_type::from_config_bag(config_bag, base_directories)?
        }
    }

    fn gen_config_bag_get_call(&self) -> Expr {
        let name = self.ident().to_string();
        match self.default() {
            DefaultValue::Closure(closure) => {
                parse_quote! {
                    config_bag.get_or_else(#name, || (#closure)(base_directories).try_into().unwrap())?
                }
            }
            DefaultValue::Expression(expr) => {
                parse_quote! {
                    config_bag.get_or_else(#name, || #expr.try_into().unwrap())?
                }
            }
            DefaultValue::None => parse_quote!(config_bag.get(#name)?),
        }
    }

    fn gen_config_bag_get_flag_call(&self) -> Expr {
        let name = self.ident().to_string();
        let field_type = &self.ty;
        parse_quote! {
            config_bag.get_flag(#name)?.unwrap_or(#field_type::from(false))
        }
    }

    fn gen_config_bag_get_option_call(&self) -> Expr {
        let name = self.ident().to_string();
        parse_quote! {
            config_bag.get_option(#name)?
        }
    }
}

#[derive(Clone, Debug, FromDeriveInput)]
#[darling(supports(struct_named))]
#[darling(attributes(config))]
struct ConfigInput {
    ident: Ident,
    data: darling::ast::Data<(), ConfigStructField>,
}

impl ConfigInput {
    fn gen_add_command_line_options_fn(&self) -> Result<Item> {
        let Data::Struct(ref fields) = self.data else {
            panic!()
        };
        let builder_value_calls = fields
            .fields
            .iter()
            .flat_map(|field| {
                let mut exprs = vec![];
                if let Some(heading) = &field.next_help_heading {
                    exprs.push(Ok(parse_quote! {
                        let builder = builder.next_help_heading(#heading)
                    }))
                }
                exprs.push(if field.flatten.is_some() {
                    Ok(field.gen_flatten_add_command_line_options_call())
                } else if field.flag.is_some() {
                    field.gen_builder_flag_value_call()
                } else if field.option.is_some() {
                    field.gen_builder_option_value_call()
                } else {
                    field.gen_builder_value_call()
                });
                exprs
            })
            .collect::<syn::Result<Vec<_>>>()?;
        Ok(parse_quote! {
            fn add_command_line_options(
                builder: ::maelstrom_config::CommandBuilder,
                base_directories: &::xdg::BaseDirectories
            ) -> ::maelstrom_config::CommandBuilder {
                #(#builder_value_calls;)*
                builder
            }
        })
    }

    fn gen_from_config_bag_fn(&self) -> Result<Item> {
        let Data::Struct(ref fields) = self.data else {
            panic!()
        };
        let field_names = fields
            .fields
            .iter()
            .map(ConfigStructField::ident)
            .map(Clone::clone);
        let field_exprs = fields.fields.iter().map(|field| {
            if field.flatten.is_some() {
                field.gen_flatten_from_config_bag_call()
            } else if field.flag.is_some() {
                field.gen_config_bag_get_flag_call()
            } else if field.option.is_some() {
                field.gen_config_bag_get_option_call()
            } else {
                field.gen_config_bag_get_call()
            }
        });
        Ok(parse_quote! {
            fn from_config_bag(
                config_bag: &mut ::maelstrom_config::ConfigBag,
                base_directories: &::xdg::BaseDirectories
            ) -> ::anyhow::Result<Self> {
                Ok(Self {
                    #(#field_names: #field_exprs,)*
                })
            }
        })
    }

    fn gen_config_impl_item(&self) -> Result<ItemImpl> {
        let self_ident: Path = self.ident.clone().into();
        let add_command_line_options = self.gen_add_command_line_options_fn()?;
        let from_config_bag = self.gen_from_config_bag_fn()?;
        Ok(parse_quote! {
            impl ::maelstrom_config::Config for #self_ident {
                #add_command_line_options
                #from_config_bag
            }
        })
    }
}

pub fn main(input: DeriveInput) -> Result<ItemImpl> {
    ConfigInput::from_derive_input(&input)?.gen_config_impl_item()
}
