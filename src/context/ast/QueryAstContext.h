/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COONTEXT_AST_QUERYASTCONTEXT_H_
#define COONTEXT_AST_QUERYASTCONTEXT_H_

#include "common/base/Base.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/PathBuildExpression.h"
#include "context/ast/AstContext.h"
#include "parser/MatchSentence.h"

namespace nebula {
namespace graph {
enum class CypherClauseKind : uint8_t {
    kMatch,
    kUnwind,
    kWith,
    kWhere,
    kReturn,
    kOrderBy,
    kPagination,
};

enum class PatternKind : uint8_t {
    kNode,
    kEdge,
};

using Direction = MatchEdge::Direction;
struct NodeInfo {
    TagID                                   tid{0};
    bool                                    anonymous{false};
    const std::string                      *label{nullptr};
    const std::string                      *alias{nullptr};
    const MapExpression                    *props{nullptr};
    Expression                             *filter{nullptr};
};

struct EdgeInfo {
    bool                                    anonymous{false};
    MatchStepRange                         *range{nullptr};
    std::vector<EdgeType>                   edgeTypes;
    MatchEdge::Direction                    direction{MatchEdge::Direction::OUT_EDGE};
    std::vector<std::string>                types;
    const std::string                      *alias{nullptr};
    const MapExpression                    *props{nullptr};
    Expression                             *filter{nullptr};
};

enum AliasType {
    kNode, kEdge, kPath
};

struct ScanInfo {
    Expression                             *filter{nullptr};
    int32_t                                 schemaId{0};
};

struct PatternContext {
    PatternContext(PatternKind k, Expression* f) : kind(k), matchFilter(f) {}
    const PatternKind kind;
    Expression* matchFilter;
};

struct NodeContext final : PatternContext {
    NodeContext(Expression* filter, NodeInfo* i)
        : PatternContext(PatternKind::kNode, filter), info(i) {}

    NodeInfo* info;
};

struct EdgeContext final : PatternContext {
    EdgeContext(Expression* filter, EdgeInfo* i)
        : PatternContext(PatternKind::kEdge, filter), info(i) {}

    EdgeInfo* info;
};

struct CypherClauseContextBase : AstContext {
    explicit CypherClauseContextBase(CypherClauseKind k) : kind(k) {}

    const CypherClauseKind  kind;
};

struct WhereClauseContext final : CypherClauseContextBase {
    WhereClauseContext() : CypherClauseContextBase(CypherClauseKind::kWhere) {}

    std::unique_ptr<Expression>     filter;
};

struct OrderByClauseContext final : CypherClauseContextBase {
    OrderByClauseContext() : CypherClauseContextBase(CypherClauseKind::kOrderBy) {}

    std::vector<std::pair<size_t, OrderFactor::OrderType>>      indexedOrderFactors;
};

struct PaginationContext final : CypherClauseContextBase {
    PaginationContext() : CypherClauseContextBase(CypherClauseKind::kPagination) {}

    int64_t     skip;
    int64_t     limit;
};

struct ReturnClauseContext final : CypherClauseContextBase {
    ReturnClauseContext() : CypherClauseContextBase(CypherClauseKind::kReturn) {}

    const YieldColumns*                     yieldColumns;
    std::unique_ptr<OrderByClauseContext>   order;
    std::unique_ptr<PaginationContext>      pagination;
    // TODO: grouping columns
};

struct WithClauseContext final : CypherClauseContextBase {
    WithClauseContext() : CypherClauseContextBase(CypherClauseKind::kWith) {}

    const YieldColumns*                         yieldColumns;
    std::unique_ptr<OrderByClauseContext>       order;
    std::unique_ptr<PaginationContext>          pagination;
    std::unique_ptr<WhereClauseContext>         where;
    // TODO: grouping columns
};

struct MatchClauseContext final : CypherClauseContextBase {
    MatchClauseContext() : CypherClauseContextBase(CypherClauseKind::kMatch) {}

    std::vector<NodeInfo>                       nodeInfos;
    std::vector<EdgeInfo>                       edgeInfos;
    std::unordered_map<std::string, AliasType>  aliases;
    std::unique_ptr<PathBuildExpression>        pathBuild;
    ScanInfo                                    scanInfo;
    const Expression*                           ids;
    std::unique_ptr<WhereClauseContext>         where;
};

struct UnwindClauseContext final : CypherClauseContextBase {
    UnwindClauseContext() : CypherClauseContextBase(CypherClauseKind::kUnwind) {}
    const YieldColumns*     yieldColumns;
};

struct MatchAstContext final : AstContext {
    // Alternative of Match/Unwind/With and ends with Return.
    std::vector<std::unique_ptr<CypherClauseContextBase>>  clauses;
};
}  // namespace graph
}  // namespace nebula
#endif  // COONTEXT_AST_QUERYASTCONTEXT_H_
