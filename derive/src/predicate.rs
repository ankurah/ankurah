use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parse, parse::ParseStream, parse_macro_input, Expr, Result, Token};

// Parse the macro input: predicate!(template, arg1, arg2, ...)
struct PredicateInput {
    template: String,
    args: Vec<Expr>,
}

impl Parse for PredicateInput {
    fn parse(input: ParseStream) -> Result<Self> {
        // Support both quoted and unquoted syntax
        if input.peek(syn::LitStr) {
            // Quoted syntax: predicate!("name = {}", value)
            let literal = input.parse::<syn::LitStr>()?;
            let mut args = Vec::new();

            while !input.is_empty() {
                input.parse::<Token![,]>()?;
                if input.is_empty() {
                    break;
                }
                args.push(input.parse::<Expr>()?);
            }

            Ok(PredicateInput { template: literal.value(), args })
        } else {
            // Unquoted syntax: predicate!(name = {name})
            let mut tokens = proc_macro2::TokenStream::new();
            while !input.is_empty() {
                let token = input.parse::<proc_macro2::TokenTree>()?;
                tokens.extend(std::iter::once(token));
            }

            Ok(PredicateInput { template: tokens_to_template_string(&tokens), args: Vec::new() })
        }
    }
}

// Parse the fetch macro input: fetch!(ctx, template, arg1, arg2, ...)
struct FetchInput {
    context: Expr,
    predicate_input: PredicateInput,
}

impl Parse for FetchInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let context = input.parse::<Expr>()?;
        input.parse::<Token![,]>()?;

        // Parse the rest as predicate input
        let predicate_input = if input.peek(syn::LitStr) {
            // Quoted syntax: fetch!(ctx, "name = {}", value)
            let literal = input.parse::<syn::LitStr>()?;
            let mut args = Vec::new();

            while !input.is_empty() {
                input.parse::<Token![,]>()?;
                if input.is_empty() {
                    break;
                }
                args.push(input.parse::<Expr>()?);
            }

            PredicateInput { template: literal.value(), args }
        } else {
            // Unquoted syntax: fetch!(ctx, name = {name})
            let mut tokens = proc_macro2::TokenStream::new();
            while !input.is_empty() {
                let token = input.parse::<proc_macro2::TokenTree>()?;
                tokens.extend(std::iter::once(token));
            }

            PredicateInput { template: tokens_to_template_string(&tokens), args: Vec::new() }
        };

        Ok(FetchInput { context, predicate_input })
    }
}

// Parse the subscribe macro input: subscribe!(ctx, callback, template, arg1, arg2, ...)
struct SubscribeInput {
    context: Expr,
    callback: Expr,
    predicate_input: PredicateInput,
}

impl Parse for SubscribeInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let context = input.parse::<Expr>()?;
        input.parse::<Token![,]>()?;
        let callback = input.parse::<Expr>()?;
        input.parse::<Token![,]>()?;

        // Parse the rest as predicate input
        let predicate_input = if input.peek(syn::LitStr) {
            // Quoted syntax: subscribe!(ctx, callback, "name = {}", value)
            let literal = input.parse::<syn::LitStr>()?;
            let mut args = Vec::new();

            while !input.is_empty() {
                input.parse::<Token![,]>()?;
                if input.is_empty() {
                    break;
                }
                args.push(input.parse::<Expr>()?);
            }

            PredicateInput { template: literal.value(), args }
        } else {
            // Unquoted syntax: subscribe!(ctx, callback, name = {name})
            let mut tokens = proc_macro2::TokenStream::new();
            while !input.is_empty() {
                let token = input.parse::<proc_macro2::TokenTree>()?;
                tokens.extend(std::iter::once(token));
            }

            PredicateInput { template: tokens_to_template_string(&tokens), args: Vec::new() }
        };

        Ok(SubscribeInput { context, callback, predicate_input })
    }
}

/// Implementation function for the predicate macro.
///
/// This function handles the actual parsing and code generation.
/// See the `predicate!` macro documentation for usage examples.
pub fn predicate_macro(input: TokenStream) -> TokenStream {
    let PredicateInput { template, args } = parse_macro_input!(input as PredicateInput);
    let predicate_code = generate_predicate_from_template(&template, &args);

    let expanded = quote! {
        {
            #predicate_code
        }
    };

    TokenStream::from(expanded)
}

/// Implementation function for the fetch macro.
pub fn fetch_macro(input: TokenStream) -> TokenStream {
    let FetchInput { context, predicate_input } = parse_macro_input!(input as FetchInput);
    let predicate_code = generate_predicate_from_template(&predicate_input.template, &predicate_input.args);

    let expanded = quote! {
        (#context).fetch({#predicate_code})
    };

    TokenStream::from(expanded)
}

/// Implementation function for the subscribe macro.
pub fn subscribe_macro(input: TokenStream) -> TokenStream {
    let SubscribeInput { context, callback, predicate_input } = parse_macro_input!(input as SubscribeInput);
    let predicate_code = generate_predicate_from_template(&predicate_input.template, &predicate_input.args);

    let expanded = quote! {
        (#context).subscribe({#predicate_code}, #callback)
    };

    TokenStream::from(expanded)
}

fn process_quoted_template(template: &str, positional_args: &[Expr]) -> (String, Vec<(Option<String>, String, Expr)>) {
    use regex::Regex;

    // Process both {} (positional) and {variable_name} (named) placeholders in order
    let re = Regex::new(r"\{([a-zA-Z_][a-zA-Z0-9_]*|)\}").unwrap();
    let mut query_with_placeholders = String::new();
    let mut last_end = 0;
    let mut final_args = Vec::new();
    let mut positional_index = 0;

    for captures in re.captures_iter(template) {
        let full_match = captures.get(0).unwrap();
        let var_name = captures.get(1).unwrap().as_str();

        // Add the text before this match
        query_with_placeholders.push_str(&template[last_end..full_match.start()]);

        // Replace placeholder with ?
        query_with_placeholders.push('?');

        if var_name.is_empty() {
            // Empty {} - use positional argument
            if positional_index < positional_args.len() {
                final_args.push((None, format!("arg_{}", positional_index), positional_args[positional_index].clone()));
                positional_index += 1;
            } else {
                panic!("Not enough positional arguments for {{}} placeholders");
            }
        } else {
            // Named {variable_name} - create expression for variable
            let ident = syn::Ident::new(var_name, proc_macro2::Span::call_site());
            let expr = syn::Expr::Path(syn::ExprPath {
                attrs: Vec::new(),
                qself: None,
                path: syn::Path {
                    leading_colon: None,
                    segments: {
                        let mut segments = syn::punctuated::Punctuated::new();
                        segments.push(syn::PathSegment { ident, arguments: syn::PathArguments::None });
                        segments
                    },
                },
            });
            final_args.push((None, var_name.to_string(), expr));
        }

        last_end = full_match.end();
    }

    // Add any remaining text
    query_with_placeholders.push_str(&template[last_end..]);

    // Check if we used all positional arguments
    if positional_index != positional_args.len() {
        panic!("Too many positional arguments provided: expected {}, got {}", positional_index, positional_args.len());
    }

    (query_with_placeholders, final_args)
}

fn process_unquoted_template(template: &str) -> (String, Vec<(Option<String>, String, Expr)>) {
    use regex::Regex;

    // Extract all {operator?identifier} patterns and replace with ?
    // Support optional comparison operators: {>age}, {<=count}, {!=status}, etc.
    let re = Regex::new(r"\{(>\s*=|<\s*=|!\s*=|<\s*>|>|<|=)?\s*([a-zA-Z_][a-zA-Z0-9_]*)\}").unwrap();
    let mut query_with_placeholders = String::new();
    let mut last_end = 0;
    let mut variable_mapping = Vec::new();

    for captures in re.captures_iter(template) {
        let full_match = captures.get(0).unwrap();
        let operator = captures.get(1).map(|m| m.as_str().replace(" ", ""));
        let var_name = captures.get(2).unwrap().as_str();

        // Add the text before this match
        query_with_placeholders.push_str(&template[last_end..full_match.start()]);

        // Replace {operator?identifier} with ?
        query_with_placeholders.push('?');

        // Create an expression that references the variable
        let ident = syn::Ident::new(var_name, proc_macro2::Span::call_site());
        let expr = syn::Expr::Path(syn::ExprPath {
            attrs: Vec::new(),
            qself: None,
            path: syn::Path {
                leading_colon: None,
                segments: {
                    let mut segments = syn::punctuated::Punctuated::new();
                    segments.push(syn::PathSegment { ident, arguments: syn::PathArguments::None });
                    segments
                },
            },
        });
        variable_mapping.push((operator, var_name.to_string(), expr));

        last_end = full_match.end();
    }

    // Add any remaining text
    query_with_placeholders.push_str(&template[last_end..]);

    (query_with_placeholders, variable_mapping)
}

fn generate_predicate_from_template(template: &str, args: &[Expr]) -> proc_macro2::TokenStream {
    let (query_with_placeholders, final_args) = if args.is_empty() {
        // Unquoted syntax with {operator?identifier} patterns
        let (query, extracted_args) = process_unquoted_template(template);
        (query, extracted_args)
    } else {
        // Quoted syntax supports both {} (positional) and {name} (named) substitutions
        let (query, template_args) = process_quoted_template(template, args);
        (query, template_args)
    };

    // Parse the query at compile time
    let selection = match ankql::parser::parse_selection(&query_with_placeholders) {
        Ok(sel) => sel,
        Err(e) => {
            return syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to parse predicate: {}", e)).to_compile_error();
        }
    };

    // Generate Rust code that constructs the AST with placeholders replaced
    let mut arg_index = 0;
    generate_predicate_code_with_replacements(&selection.predicate, &final_args, &mut arg_index)
}

fn generate_predicate_code_with_replacements(
    predicate: &ankql::ast::Predicate,
    args: &[(Option<String>, String, Expr)],
    arg_index: &mut usize,
) -> proc_macro2::TokenStream {
    match predicate {
        ankql::ast::Predicate::Placeholder => {
            // This is a standalone placeholder like {name} or {>age}
            if *arg_index >= args.len() {
                panic!("Not enough arguments for placeholders");
            }
            let (operator, var_name, arg) = &args[*arg_index];
            *arg_index += 1;

            // Determine the comparison operator (default to Equal)
            let op = operator.as_deref().unwrap_or("=");
            let op_code = match op {
                "=" => quote! { ::ankql::ast::ComparisonOperator::Equal },
                "!=" => quote! { ::ankql::ast::ComparisonOperator::NotEqual },
                "<>" => quote! { ::ankql::ast::ComparisonOperator::NotEqual },
                ">" => quote! { ::ankql::ast::ComparisonOperator::GreaterThan },
                ">=" => quote! { ::ankql::ast::ComparisonOperator::GreaterThanOrEqual },
                "<" => quote! { ::ankql::ast::ComparisonOperator::LessThan },
                "<=" => quote! { ::ankql::ast::ComparisonOperator::LessThanOrEqual },
                _ => panic!("Unsupported operator: {}", op),
            };

            quote! {
                ::ankql::ast::Predicate::Comparison {
                    left: Box::new(::ankql::ast::Expr::Identifier(
                        ::ankql::ast::Identifier::Property(#var_name.to_string())
                    )),
                    operator: #op_code,
                    right: Box::new((#arg).into()),
                }
            }
        }
        ankql::ast::Predicate::Comparison { left, operator, right } => {
            let left_code = generate_expr_code_with_replacements(left, args, arg_index);
            let right_code = generate_expr_code_with_replacements(right, args, arg_index);
            let op_code = generate_comparison_op_code(operator);
            quote! {
                ::ankql::ast::Predicate::Comparison {
                    left: Box::new(#left_code),
                    operator: #op_code,
                    right: Box::new(#right_code),
                }
            }
        }
        ankql::ast::Predicate::And(left, right) => {
            let left_code = generate_predicate_code_with_replacements(left, args, arg_index);
            let right_code = generate_predicate_code_with_replacements(right, args, arg_index);
            quote! {
                ::ankql::ast::Predicate::And(
                    Box::new(#left_code),
                    Box::new(#right_code),
                )
            }
        }
        ankql::ast::Predicate::Or(left, right) => {
            let left_code = generate_predicate_code_with_replacements(left, args, arg_index);
            let right_code = generate_predicate_code_with_replacements(right, args, arg_index);
            quote! {
                ::ankql::ast::Predicate::Or(
                    Box::new(#left_code),
                    Box::new(#right_code),
                )
            }
        }
        ankql::ast::Predicate::Not(pred) => {
            let pred_code = generate_predicate_code_with_replacements(pred, args, arg_index);
            quote! {
                ankql::ast::Predicate::Not(Box::new(#pred_code))
            }
        }
        ankql::ast::Predicate::IsNull(expr) => {
            let expr_code = generate_expr_code_with_replacements(expr, args, arg_index);
            quote! {
                ::ankql::ast::Predicate::IsNull(Box::new(#expr_code))
            }
        }
        ankql::ast::Predicate::True => quote! { ::ankql::ast::Predicate::True },
        ankql::ast::Predicate::False => quote! { ::ankql::ast::Predicate::False },
    }
}

fn generate_expr_code_with_replacements(
    expr: &ankql::ast::Expr,
    args: &[(Option<String>, String, Expr)],
    arg_index: &mut usize,
) -> proc_macro2::TokenStream {
    match expr {
        ankql::ast::Expr::Placeholder => {
            // This is a literal placeholder like the {rank} in "rank = {rank}"
            if *arg_index >= args.len() {
                panic!("Not enough arguments for placeholders");
            }
            let (_, _, arg) = &args[*arg_index];
            *arg_index += 1;
            quote! { (#arg).into() }
        }
        ankql::ast::Expr::Literal(lit) => generate_literal_code_with_replacements(lit, args, arg_index),
        ankql::ast::Expr::Identifier(id) => generate_identifier_code(id),
        ankql::ast::Expr::Predicate(pred) => {
            let pred_code = generate_predicate_code_with_replacements(pred, args, arg_index);
            quote! { ::ankql::ast::Expr::Predicate(#pred_code) }
        }
        ankql::ast::Expr::InfixExpr { left, operator, right } => {
            let left_code = generate_expr_code_with_replacements(left, args, arg_index);
            let right_code = generate_expr_code_with_replacements(right, args, arg_index);
            let op_code = generate_infix_op_code(operator);
            quote! {
                ::ankql::ast::Expr::InfixExpr {
                    left: Box::new(#left_code),
                    operator: #op_code,
                    right: Box::new(#right_code),
                }
            }
        }
        ankql::ast::Expr::ExprList(exprs) => {
            let expr_codes: Vec<_> = exprs.iter().map(|e| generate_expr_code_with_replacements(e, args, arg_index)).collect();
            quote! {
                ::ankql::ast::Expr::ExprList(vec![#(#expr_codes),*])
            }
        }
    }
}

fn generate_literal_code_with_replacements(
    literal: &ankql::ast::Literal,
    _args: &[(Option<String>, String, Expr)],
    _arg_index: &mut usize,
) -> proc_macro2::TokenStream {
    match literal {
        ankql::ast::Literal::String(s) => {
            quote! { ::ankql::ast::Expr::Literal(::ankql::ast::Literal::String(#s.to_string())) }
        }
        ankql::ast::Literal::Integer(i) => {
            quote! { ::ankql::ast::Expr::Literal(::ankql::ast::Literal::Integer(#i)) }
        }
        ankql::ast::Literal::Float(f) => {
            quote! { ::ankql::ast::Expr::Literal(::ankql::ast::Literal::Float(#f)) }
        }
        ankql::ast::Literal::Boolean(b) => {
            quote! { ::ankql::ast::Expr::Literal(::ankql::ast::Literal::Boolean(#b)) }
        }
    }
}

fn tokens_to_template_string(tokens: &proc_macro2::TokenStream) -> String {
    let mut result = String::new();

    for token in tokens.clone() {
        match token {
            proc_macro2::TokenTree::Group(group) => match group.delimiter() {
                proc_macro2::Delimiter::Brace => {
                    result.push('{');
                    result.push_str(&tokens_to_template_string(&group.stream()));
                    result.push('}');
                }
                proc_macro2::Delimiter::Parenthesis => {
                    result.push('(');
                    result.push_str(&tokens_to_template_string(&group.stream()));
                    result.push(')');
                }
                proc_macro2::Delimiter::Bracket => {
                    result.push('[');
                    result.push_str(&tokens_to_template_string(&group.stream()));
                    result.push(']');
                }
                proc_macro2::Delimiter::None => {
                    result.push_str(&tokens_to_template_string(&group.stream()));
                }
            },
            proc_macro2::TokenTree::Ident(ident) => {
                if !result.is_empty() && !result.ends_with(' ') && !result.ends_with('(') && !result.ends_with('{') {
                    result.push(' ');
                }
                result.push_str(&ident.to_string());
            }
            proc_macro2::TokenTree::Punct(punct) => {
                let ch = punct.as_char();
                if ch == '=' || ch == '<' || ch == '>' || ch == '!' {
                    if !result.is_empty() && !result.ends_with(' ') {
                        result.push(' ');
                    }
                    result.push(ch);
                    result.push(' ');
                } else {
                    result.push(ch);
                }
            }
            proc_macro2::TokenTree::Literal(literal) => {
                if !result.is_empty() && !result.ends_with(' ') && !result.ends_with('(') {
                    result.push(' ');
                }
                result.push_str(&literal.to_string());
            }
        }
    }
    result
}

fn generate_identifier_code(identifier: &ankql::ast::Identifier) -> proc_macro2::TokenStream {
    match identifier {
        ankql::ast::Identifier::Property(name) => {
            quote! { ::ankql::ast::Expr::Identifier(::ankql::ast::Identifier::Property(#name.to_string())) }
        }
        ankql::ast::Identifier::CollectionProperty(collection, property) => {
            quote! {
                ::ankql::ast::Expr::Identifier(::ankql::ast::Identifier::CollectionProperty(
                    #collection.to_string(),
                    #property.to_string()
                ))
            }
        }
    }
}

fn generate_comparison_op_code(operator: &ankql::ast::ComparisonOperator) -> proc_macro2::TokenStream {
    match operator {
        ankql::ast::ComparisonOperator::Equal => quote! { ::ankql::ast::ComparisonOperator::Equal },
        ankql::ast::ComparisonOperator::NotEqual => quote! { ::ankql::ast::ComparisonOperator::NotEqual },
        ankql::ast::ComparisonOperator::GreaterThan => quote! { ::ankql::ast::ComparisonOperator::GreaterThan },
        ankql::ast::ComparisonOperator::GreaterThanOrEqual => {
            quote! { ::ankql::ast::ComparisonOperator::GreaterThanOrEqual }
        }
        ankql::ast::ComparisonOperator::LessThan => quote! { ::ankql::ast::ComparisonOperator::LessThan },
        ankql::ast::ComparisonOperator::LessThanOrEqual => quote! { ::ankql::ast::ComparisonOperator::LessThanOrEqual },
        ankql::ast::ComparisonOperator::In => quote! { ::ankql::ast::ComparisonOperator::In },
        ankql::ast::ComparisonOperator::Between => quote! { ::ankql::ast::ComparisonOperator::Between },
    }
}

fn generate_infix_op_code(operator: &ankql::ast::InfixOperator) -> proc_macro2::TokenStream {
    match operator {
        ankql::ast::InfixOperator::Add => quote! { ::ankql::ast::InfixOperator::Add },
        ankql::ast::InfixOperator::Subtract => quote! { ::ankql::ast::InfixOperator::Subtract },
        ankql::ast::InfixOperator::Multiply => quote! { ::ankql::ast::InfixOperator::Multiply },
        ankql::ast::InfixOperator::Divide => quote! { ::ankql::ast::InfixOperator::Divide },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_quoted_template_positional() {
        // Test with positional arguments only
        let args = vec![syn::parse_str::<syn::Expr>("name").unwrap(), syn::parse_str::<syn::Expr>("age").unwrap()];
        let (query, vars) = process_quoted_template("name = {} AND age > {}", &args);
        assert_eq!(query, "name = ? AND age > ?");
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].0, None); // positional
        assert_eq!(vars[0].1, "arg_0");
        assert_eq!(vars[1].0, None); // positional
        assert_eq!(vars[1].1, "arg_1");
    }

    #[test]
    fn test_process_quoted_template_named() {
        // Test with named placeholders only
        let (query, vars) = process_quoted_template("name = {name} AND age > {age}", &[]);
        assert_eq!(query, "name = ? AND age > ?");
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].0, None); // named placeholder
        assert_eq!(vars[0].1, "name");
        assert_eq!(vars[1].0, None); // named placeholder
        assert_eq!(vars[1].1, "age");
    }

    #[test]
    fn test_process_quoted_template_mixed() {
        // Test with mixed positional and named placeholders
        let args = vec![syn::parse_str::<syn::Expr>("status").unwrap()];
        let (query, vars) = process_quoted_template("name = {name} AND status = {} AND age > {age}", &args);
        assert_eq!(query, "name = ? AND status = ? AND age > ?");
        assert_eq!(vars.len(), 3);
        assert_eq!(vars[0].0, None); // named
        assert_eq!(vars[0].1, "name");
        assert_eq!(vars[1].0, None); // positional
        assert_eq!(vars[1].1, "arg_0");
        assert_eq!(vars[2].0, None); // named
        assert_eq!(vars[2].1, "age");
    }

    #[test]
    fn test_process_quoted_template_no_placeholders() {
        // Test with no placeholders
        let (query, vars) = process_quoted_template("name = 'test'", &[]);
        assert_eq!(query, "name = 'test'");
        assert_eq!(vars.len(), 0);
    }

    #[test]
    #[should_panic(expected = "Not enough positional arguments for {} placeholders")]
    fn test_process_quoted_template_insufficient_positional_args() {
        // Test with insufficient positional arguments
        let args = vec![syn::parse_str::<syn::Expr>("name").unwrap()];
        process_quoted_template("name = {} AND age > {}", &args);
    }

    #[test]
    #[should_panic(expected = "Too many positional arguments provided")]
    fn test_process_quoted_template_too_many_positional_args() {
        // Test with too many positional arguments
        let args = vec![
            syn::parse_str::<syn::Expr>("name").unwrap(),
            syn::parse_str::<syn::Expr>("age").unwrap(),
            syn::parse_str::<syn::Expr>("status").unwrap(),
        ];
        process_quoted_template("name = {}", &args);
    }

    #[test]
    fn test_compile_time_parsing() {
        // Test that we can parse queries at compile time
        let query = "name = ? AND age > ?";
        let result = ankql::parser::parse_selection(query);
        assert!(result.is_ok());

        // Test with a simple comparison
        let query = "status = ?";
        let result = ankql::parser::parse_selection(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_process_unquoted_template_with_operators() {
        // Test default behavior (no operator specified)
        let (query, vars) = process_unquoted_template("name = {name} AND {status}");
        assert_eq!(query, "name = ? AND ?");
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].0, None); // name has no operator
        assert_eq!(vars[0].1, "name");
        assert_eq!(vars[1].0, None); // status has no operator
        assert_eq!(vars[1].1, "status");

        // Test with various operators
        let (query, vars) = process_unquoted_template("{> age} AND {<= count} AND {!= status}");
        assert_eq!(query, "? AND ? AND ?");
        assert_eq!(vars.len(), 3);
        assert_eq!(vars[0].0, Some(">".to_string()));
        assert_eq!(vars[0].1, "age");
        assert_eq!(vars[1].0, Some("<=".to_string()));
        assert_eq!(vars[1].1, "count");
        assert_eq!(vars[2].0, Some("!=".to_string()));
        assert_eq!(vars[2].1, "status");

        // Test with >= and <> operators
        let (query, vars) = process_unquoted_template("{>= score} AND {< > type}");
        assert_eq!(query, "? AND ?");
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].0, Some(">=".to_string()));
        assert_eq!(vars[0].1, "score");
        assert_eq!(vars[1].0, Some("<>".to_string()));
        assert_eq!(vars[1].1, "type");

        // Test mixed with existing comparison context
        let (query, vars) = process_unquoted_template("name = {name} AND age > {age}");
        assert_eq!(query, "name = ? AND age > ?");
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].0, None); // name has no operator
        assert_eq!(vars[0].1, "name");
        assert_eq!(vars[1].0, None); // age has no operator in this context
        assert_eq!(vars[1].1, "age");

        // Test with explicit = operator
        let (query, vars) = process_unquoted_template("{=name} AND {>age}");
        assert_eq!(query, "? AND ?");
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].0, Some("=".to_string()));
        assert_eq!(vars[0].1, "name");
        assert_eq!(vars[1].0, Some(">".to_string()));
        assert_eq!(vars[1].1, "age");
    }
}
