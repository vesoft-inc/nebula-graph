/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_FINDPATHVALIDATOR_H_
#define VALIDATOR_FINDPATHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"

namespace nebula {
namespace graph {


class FindPathValidator final : public TraversalValidator {
public:
    struct Starts {
        FromType                fromType{kInstantExpr};
        Expression*             srcRef{nullptr};
        std::string             userDefinedVarName;
        std::vector<Value>      vids;
    };

    struct Over {
        bool                            isOverAll{false};
        std::vector<EdgeType>           edgeTypes;
        storage::cpp2::EdgeDirection    direction;
        std::vector<std::string>        allEdges;
    };

    struct Step {
        StepClause::MToN*     mToN{nullptr};
        uint32_t              steps{1};
    };

    FindPathValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateStarts(const VerticesClause* clause, Starts& starts);

    Status validateOver(const OverClause* clause, Over& over);

    Status validateStep(const StepClause* clause, Step& step);

private:
    bool            isShortest_{false};
    Starts          from_;
    Starts          to_;
    Over            over_;
    Step            step_;
};
}  // namespace graph
}  // namespace nebula
#endif
