/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_QUERYVALIDATOR_H_
#define VALIDATOR_QUERYVALIDATOR_H_

#include "validator/Validator.h"

namespace nebula {
namespace graph {

// It's used for the sentence with piped input/ouput
// Guranteed by the parser rule traverse_sentence
class QueryValidator : public Validator {
public:
    ColsDef outputCols() const {
        return outputs_;
    }

    ColsDef inputCols() const {
        return inputs_;
    }

    void setInputVarName(std::string name) {
        inputVarName_ = std::move(name);
    }

    void setInputCols(ColsDef&& inputs) {
        inputs_ = std::move(inputs);
    }

protected:
    enum FromType {
        kInstantExpr,
        kVariable,
        kPipe,
    };

    struct Starts {
        FromType                fromType{kInstantExpr};
        Expression*             srcRef{nullptr};
        std::string             userDefinedVarName;
        std::string             firstBeginningSrcVidColName;
        std::vector<Value>      vids;
    };


    QueryValidator(Sentence* sentence, QueryContext* qctx) : Validator(sentence, qctx),
                                                             startVidList_(new ExpressionList()) {}

    // Check the variable or input property reference
    // return the input variable
    StatusOr<std::string> checkInputVarProperty(const Expression *ref,
                                                const Value::Type type) const;

    StatusOr<Value::Type> deduceExprType(const Expression* expr) const;

    Status validateVertices(const VerticesClause* clause, Starts& starts);

    std::string buildConstantInput(Expression* &src, Starts &starts);

protected:
    // The input columns and output columns of a sentence.
    ColsDef                         outputs_;
    ColsDef                         inputs_;
    // The variable name of the input node.
    std::string                     inputVarName_;

    std::unique_ptr<ExpressionList>  startVidList_;
};

}  // namespace graph
}  // namespace nebula
#endif
