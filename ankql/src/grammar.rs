use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "ankql.pest"]
pub struct AnkqlParser;

#[cfg(test)]
mod tests {
    use super::*;
    use pest::*;

    #[test]
    fn test_literal_comparison() {
        parses_to! {
            parser: AnkqlParser,
            input: "a=1",
            rule: Rule::Selection,
            tokens: [
                Expr(0, 3, [
                    PathExpr(0, 1, [Identifier(0, 1)]),
                    Eq(1, 2),
                    Unsigned(2, 3)
                ])
            ]
        };
    }

    #[test]
    fn test_path_comparison() {
        let parser = AnkqlParser::parse(Rule::Selection, "a.foo = b.foo").unwrap();
        println!("{:#?}", parser);
        parses_to! {
            parser: AnkqlParser,
            input: "a.foo = b.foo",
            rule: Rule::Selection,
            tokens: [
                Expr(0, 13, [
                    PathExpr(0, 5, [Identifier(0, 1), Identifier(2, 5)]),
                    Eq(6, 7),
                    PathExpr(8, 13, [Identifier(8, 9), Identifier(10, 13)])
                ])
            ]
        };
    }

    #[test]
    fn test_boolean_expression() {
        let parser = AnkqlParser::parse(Rule::Selection, "a.foo = b.foo AND a.bar > 1 OR b.bar > 1").unwrap();
        println!("{:#?}", parser);
        parses_to! {
            parser: AnkqlParser,
            input: "a.foo = b.foo AND a.bar > 1 OR b.bar > 1",
            rule: Rule::Selection,
            tokens: [
                    Expr(0, 40, [
                        PathExpr(0, 5, [Identifier(0, 1), Identifier(2, 5)]),
                        Eq(6, 7),
                        PathExpr(8, 13, [Identifier(8, 9), Identifier(10, 13)]),
                        And(14, 17),
                        PathExpr(18, 23, [Identifier(18, 19), Identifier(20, 23)]),
                        Gt(24, 25),
                        Unsigned(26, 27),
                        Or(28, 30),
                        PathExpr(31, 36, [Identifier(31, 32), Identifier(33, 36)]),
                        Gt(37, 38),
                        Unsigned(39, 40)
                    ])
            ]
        };
    }

    #[test]
    fn test_boolean_expression_parenthetical() {
        parses_to! {
            parser: AnkqlParser,
            input: "(a.foo = b.foo AND a.bar > 1) OR b.bar > 1",
            rule: Rule::Selection,
            tokens: [
                    Expr(0, 42, [
                        ExpressionInParentheses(0, 29, [
                            Expr(1, 28, [
                                PathExpr(1, 6, [Identifier(1, 2), Identifier(3, 6)]),
                                Eq(7, 8),
                                PathExpr(9, 14, [Identifier(9, 10), Identifier(11, 14)]),
                                And(15, 18),
                                PathExpr(19, 24, [Identifier(19, 20), Identifier(21, 24)]),
                                Gt(25, 26),
                                Unsigned(27, 28)
                            ])
                        ]),
                        Or(30, 32),
                        PathExpr(33, 38, [Identifier(33, 34), Identifier(35, 38)]),
                        Gt(39, 40),
                        Unsigned(41, 42)
                    ])
            ]
        };
    }

    #[test]
    fn test_order_by_clause_basic() {
        parses_to! {
            parser: AnkqlParser,
            input: "true ORDER BY name",
            rule: Rule::Selection,
            tokens: [
                Expr(0, 5, [True(0, 4)]),
                OrderByClause(5, 18, [OrderByItem(14, 18, [Identifier(14, 18)])])
            ]
        };
    }

    #[test]
    fn test_order_by_clause_with_direction() {
        parses_to! {
            parser: AnkqlParser,
            input: "true ORDER BY name DESC",
            rule: Rule::Selection,
            tokens: [
                Expr(0, 5, [True(0, 4)]),
                OrderByClause(5, 23, [OrderByItem(14, 23, [Identifier(14, 18), OrderDirection(19, 23)])])
            ]
        };
    }

    #[test]
    fn test_limit_clause() {
        parses_to! {
            parser: AnkqlParser,
            input: "true LIMIT 10",
            rule: Rule::Selection,
            tokens: [
                Expr(0, 5, [True(0, 4)]),
                LimitClause(5, 13, [Unsigned(11, 13)])
            ]
        };
    }

    #[test]
    fn test_order_by_and_limit() {
        parses_to! {
            parser: AnkqlParser,
            input: "status = 'active' ORDER BY name ASC LIMIT 5",
            rule: Rule::Selection,
            tokens: [
                Expr(0, 18, [
                    PathExpr(0, 7, [Identifier(0, 6)]),
                    Eq(7, 8),
                    SingleQuotedString(9, 17)
                ]),
                OrderByClause(18, 36, [OrderByItem(27, 35, [Identifier(27, 31), OrderDirection(32, 35)])]),
                LimitClause(36, 43, [Unsigned(42, 43)])
            ]
        };
    }

    #[test]
    fn test_order_by_multiple_items() {
        parses_to! {
            parser: AnkqlParser,
            input: "true ORDER BY name ASC, created_at DESC,id", // intentionally omitted the space on the last item
            rule: Rule::Selection,
            tokens: [
                Expr(0, 5, [True(0, 4)]),
                OrderByClause(5, 42, [
                    OrderByItem(14, 22, [Identifier(14, 18), OrderDirection(19, 22)]),
                    OrderByItem(24, 39, [Identifier(24, 34), OrderDirection(35, 39)]),
                    OrderByItem(40, 42, [Identifier(40, 42)])
                ])
            ]
        };
    }

    #[test]
    fn test_pathological_cases() {
        // Test that keywords can be used as identifiers when not in keyword context
        parses_to! {
            parser: AnkqlParser,
            input: "limit = 1",
            rule: Rule::Selection,
            tokens: [
                Expr(0, 9, [
                    PathExpr(0, 6, [Identifier(0, 5)]),
                    Eq(6, 7),
                    Unsigned(8, 9)
                ])
            ]
        };

        parses_to! {
            parser: AnkqlParser,
            input: "order = 1 ORDER BY name",
            rule: Rule::Selection,
            tokens: [
                Expr(0, 10, [
                    PathExpr(0, 6, [Identifier(0, 5)]),
                    Eq(6, 7),
                    Unsigned(8, 9)
                ]),
                OrderByClause(10, 23, [OrderByItem(19, 23, [Identifier(19, 23)])])
            ]
        };
    }
}
