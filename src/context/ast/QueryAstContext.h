/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COONTEXT_AST_QUERYASTCONTEXT_H_
#define COONTEXT_AST_QUERYASTCONTEXT_H_

namespace nebula {
namespace graph {
enum class CypherClauseKind : uint8_t {
    kMatch,
    kUnwind,
    kWith,
    kWhere,
    kReturn,
    kWith,
    kOrderBy,
    kPagination,
};

struct CypherAstContextBase : AstContext {
    explicit CypherAstContextBase(CypherClauseKind k) : kind(k) {}

    const CypherClauseKind  kind;
};

struct WhereClauseContext final : CypherAstContextBase {
    WhereClauseContext() : CypherAstContextBase(CypherClauseKind::kWhere) {}

    std::unique_ptr<Expression>     filter;
};

struct OrderByClauseContext final : CypherClauseContextBase {
    OrderByClauseContext() : CypherAstContextBase(CypherClauseKind::kOrderBy) {}

    std::vector<std::pair<size_t, OrderFactor::OrderType>>      indexedOrderFactors;
};

struct PaginationContext final : CypherClauseContextBase {
    PaginationContext() : CypherAstContextBase(CypherClauseKind::kPagination) {}

    int64_t     skip;
    int64_t     limit;
};

struct ReturnClauseContext final : CypherAstContextBase {
    ReturnClauseContext() : CypherAstContextBase(CypherClauseKind::kReturn) {}

    const YieldColumns*                     yieldColumns;
    std::unique_ptr<OrderByClauseContext>   order;
    std::unique_ptr<PaginationContext>      pagination;
    // TODO: grouping columns
};

struct WithClauseContext final : CypherAstContextBase {
    WithClauseContext() : CypherAstContextBase(CypherClauseKind::kWith) {}

    const YieldColumns*                         yieldColumns;
    std::unique_ptr<OrderByClauseContext>       order;
    std::unique_ptr<PaginationContext>          pagination;
    std::unique_ptr<WhereClauseContext>         where;
    // TODO: grouping columns
};

struct MatchClauseContext final : CypherAstContextBase {
    MatchClauseContext() : CypherAstContextBase(CypherClauseKind::kMatch) {}

    std::vector<MatchValidator::NodeInfo>                       nodeInfos;
    std::vector<MatchValidator::EdgeInfo>                       edgeInfos;
    std::unordered_map<std::string, MatchValidator::AliasType>  aliases;
    std::unique_ptr<PathBuildExpression>                        pathBuild;
    MatchValidator::ScanInfo                                    scanInfo;
    const Expression*                                           ids;
    std::unique_ptr<WhereClauseContext>                         where;
};

struct UnwindClauseContext final : CypherAstContextBase {
    UnwindClauseContext() : CypherAstContextBase(CypherClauseKind::kUnwind) {}
    const YieldColumns*     yieldColumns;
};

struct MatchAstContext final : AstContext {
    // Alternative of Match/Unwind/With and ends with Return.
    std::vector<std::unique_ptr<CypherAstContextBase>>  clauses;
};
}  // namespace graph
}  // namespace nebula
#endif  // COONTEXT_AST_QUERYASTCONTEXT_H_
