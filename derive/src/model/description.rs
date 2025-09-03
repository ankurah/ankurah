use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{punctuated::Punctuated, AngleBracketedGenericArguments, Data, DeriveInput, Fields, Ident, Type, Visibility};

/// Encapsulates all the parsed information about a model and provides clean accessors
pub struct ModelDescription {
    // Basic identifiers
    name: Ident,

    // Field collections - only store what we actually parsed
    active_fields: Vec<syn::Field>,
    ephemeral_fields: Vec<syn::Field>,
}

impl ModelDescription {
    /// Parse a DeriveInput and create a ModelDescription
    pub fn parse(input: &DeriveInput) -> syn::Result<Self> {
        let name = input.ident.clone();

        let fields = match &input.data {
            Data::Struct(data) => match &data.fields {
                Fields::Named(fields) => fields.named.clone(),
                fields => return Err(syn::Error::new_spanned(fields, format!("Only named fields are supported this is a {:#?}", fields))),
            },
            _ => return Err(syn::Error::new_spanned(&name, "Only structs are supported")),
        };

        // Split fields into active and ephemeral
        let mut active_fields = Vec::new();
        let mut ephemeral_fields = Vec::new();
        for field in fields.into_iter() {
            if get_model_flag(&field.attrs, "ephemeral") {
                ephemeral_fields.push(field);
            } else {
                active_fields.push(field);
            }
        }

        Ok(Self { name, active_fields, ephemeral_fields })
    }

    // Basic identifier accessors
    pub fn name(&self) -> &Ident { &self.name }
    pub fn collection_str(&self) -> String { self.name.to_string().to_lowercase() }
    pub fn view_name(&self) -> Ident { format_ident!("{}View", self.name) }
    pub fn mutable_name(&self) -> Ident { format_ident!("{}Mut", self.name) }

    #[cfg(feature = "wasm")]
    pub fn resultset_name(&self) -> Ident { format_ident!("{}ResultSet", self.name) }
    #[cfg(feature = "wasm")]
    pub fn livequery_name(&self) -> Ident { format_ident!("{}LiveQuery", self.name) }

    // Field accessors
    pub fn active_fields(&self) -> &[syn::Field] { &self.active_fields }
    pub fn ephemeral_fields(&self) -> &[syn::Field] { &self.ephemeral_fields }

    // Computed accessors for active fields
    pub fn active_field_visibility(&self) -> Vec<&Visibility> { self.active_fields.iter().map(|f| &f.vis).collect() }
    pub fn active_field_names(&self) -> Vec<&Option<Ident>> { self.active_fields.iter().map(|f| &f.ident).collect() }
    pub fn active_field_name_strs(&self) -> Vec<String> {
        self.active_fields.iter().map(|f| f.ident.as_ref().unwrap().to_string().to_lowercase()).collect()
    }
    pub fn projected_field_types(&self) -> Vec<&Type> { self.active_fields.iter().map(|f| &f.ty).collect() }
    pub fn active_field_types(&self) -> syn::Result<Vec<syn::Type>> { self.active_fields.iter().map(get_active_type).collect() }
    pub fn projected_field_types_turbofish(&self) -> Vec<TokenStream> { self.active_fields.iter().map(|f| as_turbofish(&f.ty)).collect() }
    pub fn active_field_types_turbofish(&self) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let active_types = self.active_field_types()?;
        Ok(active_types.iter().map(as_turbofish).collect())
    }

    // Computed accessors for ephemeral fields
    pub fn ephemeral_field_names(&self) -> Vec<&Option<Ident>> { self.ephemeral_fields.iter().map(|f| &f.ident).collect() }
    pub fn ephemeral_field_types(&self) -> Vec<&Type> { self.ephemeral_fields.iter().map(|f| &f.ty).collect() }
    pub fn ephemeral_field_visibility(&self) -> Vec<&Visibility> { self.ephemeral_fields.iter().map(|f| &f.vis).collect() }
}

static ACTIVE_TYPE_MOD_PREFIX: &str = "::ankurah::property::value";
fn get_active_type(field: &syn::Field) -> Result<syn::Type, syn::Error> {
    let active_type_ident = format_ident!("active_type");

    let mut active_type = None;
    // First check if there's an explicit attribute
    if let Some(active_type_attr) = field.attrs.iter().find(|attr| attr.path().get_ident() == Some(&active_type_ident)) {
        active_type = Some(active_type_attr.parse_args::<syn::Type>()?);
    } else {
        // Check for exact type matches and provide default Active types
        if let Type::Path(type_path) = &field.ty {
            let path_str = quote!(#type_path).to_string().replace(" ", "");
            match path_str.as_str() {
                // Add more default mappings here as needed
                "String" | "std::string::String" => {
                    let path = format!("{}::YrsString", ACTIVE_TYPE_MOD_PREFIX);
                    let yrs = syn::parse_str(&path).map_err(|_| syn::Error::new_spanned(&field.ty, "Failed to create YrsString path"))?;
                    active_type = Some(yrs);
                }
                _ => {
                    // Everything else should use `LWW`` by default.
                    // TODO: Return a list of compile_error! for these types to specify
                    // that these need to be `Serialize + for<'de> Deserialize<'de>``
                    let path = format!("{}::LWW", ACTIVE_TYPE_MOD_PREFIX);
                    let lww = syn::parse_str(&path).map_err(|_| syn::Error::new_spanned(&field.ty, "Failed to create YrsString path"))?;
                    active_type = Some(lww);
                }
            }
        };
    }

    if let Some(active_type) = active_type {
        Ok(ActiveFieldType::convert_type_with_projected(&active_type, &field.ty))
    } else {
        // If we get here, we don't have a supported default Active type
        let field_name = field.ident.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "unnamed".to_string());
        let type_str = format!("{:?}", &field.ty);

        Err(syn::Error::new_spanned(
            &field.ty,
            format!(
                "No active value type found for field '{}' (type: {}). Please specify using #[active_type(Type)] attribute",
                field_name, type_str
            ),
        ))
    }
}

// Parse the active field type
struct ActiveFieldType {
    pub base: syn::Type,
    pub generics: Option<syn::AngleBracketedGenericArguments>,
}

impl ActiveFieldType {
    pub fn convert_type_with_projected(ty: &syn::Type, projected: &syn::Type) -> syn::Type {
        let mut base = Self::from_type(ty);
        base.with_projected(projected);
        base.as_type()
    }

    pub fn from_type(ty: &syn::Type) -> Self {
        if let syn::Type::Path(path) = ty {
            if let Some(last_segment) = path.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(generics) = &last_segment.arguments {
                    return Self { base: ty.clone(), generics: Some(generics.clone()) };
                }
            }
        }

        Self { base: ty.clone(), generics: None }
    }

    pub fn with_projected(&mut self, ty: &syn::Type) {
        let mut generics = match &self.generics {
            Some(generics) => generics.clone(),
            None => AngleBracketedGenericArguments {
                colon2_token: None,
                lt_token: Default::default(),
                args: Punctuated::default(),
                gt_token: Default::default(),
            },
        };

        // Replace inferred `_` with projected
        if let Some(last @ syn::GenericArgument::Type(syn::Type::Infer(_))) = generics.args.last_mut() {
            *last = syn::GenericArgument::Type(ty.clone());
        } else {
            // Otherwise just push to the end.
            generics.args.push(syn::GenericArgument::Type(ty.clone()));
        }

        self.generics = Some(generics);
    }

    pub fn as_type(&self) -> syn::Type {
        let mut new_type = self.base.clone();
        let syn::Type::Path(ref mut path) = new_type else {
            unimplemented!("Non-path types aren't supported for active types");
        };

        let Some(last_segment) = path.path.segments.last_mut() else {
            unreachable!("Need at least a single segment for type paths...?");
        };

        if let Some(generics) = &self.generics {
            last_segment.arguments = syn::PathArguments::AngleBracketed(generics.clone());
        }

        new_type
    }
}

fn get_model_flag(attrs: &Vec<syn::Attribute>, flag_name: &str) -> bool {
    attrs.iter().any(|attr| {
        attr.path().segments.iter().any(|seg| seg.ident == "model")
            && attr.meta.require_list().ok().and_then(|list| list.parse_args::<syn::Ident>().ok()).is_some_and(|ident| ident == flag_name)
    })
}

fn as_turbofish(type_path: &syn::Type) -> proc_macro2::TokenStream {
    if let syn::Type::Path(path) = type_path {
        let mut without_generics = path.clone();
        let mut generics = syn::PathArguments::None;
        if let Some(last_segment) = without_generics.path.segments.last_mut() {
            generics = last_segment.arguments.clone();
            last_segment.arguments = syn::PathArguments::None;
        }

        if let syn::PathArguments::AngleBracketed(generics) = generics {
            quote! {
                #without_generics::#generics
            }
        } else {
            quote! {
                #without_generics
            }
        }
    } else {
        unimplemented!("as_turbofish is not supported for non-path types")
    }
}
