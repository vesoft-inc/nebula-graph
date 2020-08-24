/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATOR_H_
#define VALIDATOR_VALIDATOR_H_

#include "common/base/Base.h"
#include "planner/ExecutionPlan.h"
#include "parser/Sentence.h"
#include "context/ValidateContext.h"
#include "context/QueryContext.h"
<<<<<<< HEAD
#include "validator/ExpressionProps.h"
=======
#include "util/GraphStatus.h"
>>>>>>> all use GraphStatus

namespace nebula {

class YieldColumns;

namespace graph {

class Validator {
public:
    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence,
                                                    QueryContext* context);

    static GraphStatus validate(Sentence* sentence, QueryContext* qctx);

    GraphStatus validate();

    MUST_USE_RESULT GraphStatus appendPlan(PlanNode* tail);

    void setInputVarName(std::string name) {
        inputVarName_ = std::move(name);
    }

    void setInputCols(ColsDef&& inputs) {
        inputs_ = std::move(inputs);
    }

    PlanNode* root() const {
        return root_;
    }

    PlanNode* tail() const {
        return tail_;
    }

    ColsDef outputCols() const {
        return outputs_;
    }

    ColsDef inputCols() const {
        return inputs_;
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
    virtual GraphStatus validateImpl() = 0;

    /**
     * Convert an ast to plan.
     */
    virtual GraphStatus toPlan() = 0;

    std::vector<std::string> deduceColNames(const YieldColumns* cols) const;

    std::string deduceColName(const YieldColumn* col) const;

    StatusOr<Value::Type> deduceExprType(const Expression* expr) const;

    Status deduceProps(const Expression* expr, ExpressionProps& exprProps);

    bool evaluableExpr(const Expression* expr) const;

    static GraphStatus checkPropNonexistOrDuplicate(const ColsDef& cols,
                                                    const folly::StringPiece& prop);

    static GraphStatus appendPlan(PlanNode* plan, PlanNode* appended);

    // use for simple Plan only contain one node
    template <typename Node, typename... Args>
<<<<<<< HEAD
    Status genSingleNodePlan(Args... args) {
        auto *doNode = Node::make(qctx_, nullptr, std::forward<Args>(args)...);
=======
    GraphStatus genSingleNodePlan(Args... args) {
        auto* plan = qctx_->plan();
        auto *doNode = Node::make(plan, nullptr, std::forward<Args>(args)...);
>>>>>>> all use GraphStatus
        root_ = doNode;
        tail_ = root_;
        return GraphStatus::OK();
    }

    // Check the variable or input property reference
    // return the input variable
    StatusOr<std::string> checkRef(const Expression *ref, const Value::Type type) const;

protected:
    SpaceDescription                space_;
    Sentence*                       sentence_{nullptr};
    QueryContext*                   qctx_{nullptr};
    ValidateContext*                vctx_{nullptr};
    // root and tail of a subplan.
    PlanNode*                       root_{nullptr};
    PlanNode*                       tail_{nullptr};
    // The input columns and output columns of a sentence.
    ColsDef                         outputs_;
    ColsDef                         inputs_;
    // The variable name of the input node.
    std::string                     inputVarName_;
    // Admin sentences do not requires a space to be chosen.
    bool                            noSpaceRequired_{false};
};

}  // namespace graph
}  // namespace nebula
#endif
