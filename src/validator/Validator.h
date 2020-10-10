/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATOR_H_
#define VALIDATOR_VALIDATOR_H_

#include <type_traits>
#include "common/base/Base.h"
#include "planner/ExecutionPlan.h"
#include "parser/Sentence.h"
#include "context/ValidateContext.h"
#include "context/QueryContext.h"
#include "validator/ExpressionProps.h"

namespace nebula {

class YieldColumns;

namespace graph {

class Validator {
public:
    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence,
                                                    QueryContext* context);

    static Status validate(Sentence* sentence, QueryContext* qctx);

    Status validate();

    MUST_USE_RESULT Status appendPlan(PlanNode* tail);

    PlanNode* root() const {
        return root_;
    }

    PlanNode* tail() const {
        return tail_;
    }

    void setNoSpaceRequired() {
        noSpaceRequired_ = true;
    }

protected:
    Validator(Sentence* sentence, QueryContext* qctx);

    /**
     * Check if a space is chosen for this sentence.
     */
    virtual bool spaceChosen();

    /**
     * Validate the sentence.
     */
    virtual Status validateImpl() = 0;

    /**
     * Convert an ast to plan.
     */
    virtual Status toPlan() = 0;

    std::vector<std::string> deduceColNames(const YieldColumns* cols) const;

    std::string deduceColName(const YieldColumn* col) const;

    StatusOr<Value::Type> deduceExprType(const Expression* expr) const;

    Status deduceProps(const Expression* expr, ExpressionProps& exprProps);

    bool evaluableExpr(const Expression* expr) const;


    static StatusOr<size_t> checkPropNonexistOrDuplicate(const ColsDef& cols,
                                               folly::StringPiece prop,
                                               const std::string& validator);

    static Status appendPlan(PlanNode* plan, PlanNode* appended);

    // use for simple Plan only contain one node
    template <typename Node, typename... Args>
    Status genSingleNodePlan(Args... args) {
        auto *doNode = Node::make(qctx_, nullptr, std::forward<Args>(args)...);
        root_ = doNode;
        tail_ = root_;
        return Status::OK();
    }

protected:
    SpaceInfo                       space_;
    Sentence*                       sentence_{nullptr};
    QueryContext*                   qctx_{nullptr};
    ValidateContext*                vctx_{nullptr};
    // root and tail of a subplan.
    PlanNode*                       root_{nullptr};
    PlanNode*                       tail_{nullptr};
    // Admin sentences do not requires a space to be chosen.
    bool                            noSpaceRequired_{false};
};

}  // namespace graph
}  // namespace nebula
#endif
