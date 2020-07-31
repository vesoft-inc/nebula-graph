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

namespace nebula {

class YieldColumns;

namespace graph {

class ExpressionProps final {
public:
    using TagIDMap =  std::unordered_map<TagID, std::set<std::string>>;
    using EdgeTypeMap = std::unordered_map<EdgeType, std::set<std::string>>;
    using VarMap = std::unordered_map<std::string, std::set<std::string>>;

    void insertInput(std::string prop);

    void insertVar(const std::string& varName, std::string prop);

    void insertSrcTag(TagID tagId, std::string prop);

    void insertDstTag(TagID tagId, std::string prop);

    void insertEdge(EdgeType edgeType, std::string prop);

    void insertTag(TagID tagId, std::string prop);

    std::set<std::string>& inputProps() {
        return inputProps_;
    }
    TagIDMap& srcTagProps() {
        return srcTagProps_;
    }
    TagIDMap& dstTagProps() {
        return dstTagProps_;
    }
    TagIDMap& tagProps() {
        return tagProps_;
    }
    EdgeTypeMap& edgeProps() {
        return edgeProps_;
    }
    VarMap& varProps() {
        return varProps_;
    }

    const TagIDMap& srcTagProps() const {
        return srcTagProps_;
        // return const_cast<ExpressionProps&>(*this).srcTagProps();
    }
    TagIDMap& dstTagProps() const {
        return const_cast<ExpressionProps&>(*this).dstTagProps();
    }
    TagIDMap& tagProps() const {
        return const_cast<ExpressionProps&>(*this).tagProps();
    }
    TagIDMap& edgeProps() const {
        return const_cast<ExpressionProps&>(*this).edgeProps();
    }
    std::set<std::string>& inputProps() const {
        return const_cast<ExpressionProps&>(*this).inputProps();
    }
    VarMap& varProps() const {
        return const_cast<ExpressionProps&>(*this).varProps();
    }

    bool isSubsetOfInput(std::set<std::string>& props);

    bool isSubsetOfVar(VarMap& props);

    void unionProps(ExpressionProps& exprProps);

private:
    std::set<std::string>      inputProps_;
    VarMap                     varProps_;
    TagIDMap                   srcTagProps_;
    TagIDMap                   dstTagProps_;
    EdgeTypeMap                edgeProps_;
    TagIDMap                   tagProps_;
};

class Validator {
public:
    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence,
                                                    QueryContext* context);

    MUST_USE_RESULT Status appendPlan(PlanNode* tail);

    Status validate();

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
    virtual Status validateImpl() = 0;

    /**
     * Convert an ast to plan.
     */
    virtual Status toPlan() = 0;

    std::vector<std::string> deduceColNames(const YieldColumns* cols) const;

    std::string deduceColName(const YieldColumn* col) const;

    StatusOr<Value::Type> deduceExprType(const Expression* expr) const;

    // Status deduceProps(const Expression* expr);

    Status deduceProps(const Expression* expr, ExpressionProps& exprProps);

    bool evaluableExpr(const Expression* expr) const;

    static Status checkPropNonexistOrDuplicate(const ColsDef& cols,
                                               const std::string& prop,
                                               const std::string &validatorName);

    static Status appendPlan(PlanNode* plan, PlanNode* appended);

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
