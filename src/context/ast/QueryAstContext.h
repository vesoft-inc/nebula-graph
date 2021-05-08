/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_AST_QUERYASTCONTEXT_H_
#define CONTEXT_AST_QUERYASTCONTEXT_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "context/ast/AstContext.h"

namespace nebula {
namespace graph {

enum FromType {
    kInstant,
    kVariable,
    kPipe,
};

struct Starts {
    FromType fromType{kInstant};
    Expression* src{nullptr};
    Expression* originalSrc{nullptr};
    std::string userDefinedVarName;
    std::string firstBeginningSrcVidColName;
    std::vector<Value> vids;
};

struct Over {
    bool isOverAll{false};
    std::vector<EdgeType> edgeTypes;
    storage::cpp2::EdgeDirection direction;
    std::vector<std::string> allEdges;
};

// path context
struct PathContext final : AstContext {
    Starts from;
    Starts to;
    StepClause steps;
    Over over;

    std::string fromStartVidsVar;
    std::string toStartVidsVar;

    bool isShortest{false};
    bool isWeight{false};
    bool noLoop{false};

    // runtime
    PlanNode* fromProjectStartVid{nullptr};
    PlanNode* toProjectStartVid{nullptr};
    PlanNode* fromDedupStartVid{nullptr};
    PlanNode* toDedupStartVid{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_AST_QUERYASTCONTEXT_H_
