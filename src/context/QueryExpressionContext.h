/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_QUERYEXPRESSIONCONTEXT_H_
#define CONTEXT_QUERYEXPRESSIONCONTEXT_H_

#include "common/context/ExpressionContext.h"

#include "context/ExecutionContext.h"
#include "context/Iterator.h"

namespace nebula {
namespace graph {

class QueryExpressionContext final : public ExpressionContext {
public:
    explicit QueryExpressionContext(ExecutionContext* ectx, Iterator* iter = nullptr) {
        ectx_ = ectx;
        iter_ = iter;
    }

    // Get the latest version value for the given variable name, such as $a, $b
    const Value& getVar(const std::string& var) const override;

    // Get the given version value for the given variable name, such as $a, $b
    const Value& getVersionedVar(const std::string& var,
                                 int64_t version) const override;

    // Get the specified property from a variable, such as $a.prop_name
    const Value& getVarProp(const std::string& var,
                            const std::string& prop) const override;

    // Get the specified property from the tag, such as tag.prop_name
    Value getTagProp(const std::string& tag,
                     const std::string& prop) const override;

    // Get the specified property from the edge, such as edge_type.prop_name
    Value getEdgeProp(const std::string& edge,
                      const std::string& prop) const override;

    // Get the specified property from the source vertex, such as $^.prop_name
    Value getSrcProp(const std::string& tag,
                     const std::string& prop) const override;

    // Get the specified property from the destination vertex, such as $$.prop_name
    const Value& getDstProp(const std::string& tag,
                            const std::string& prop) const override;

    // Get the specified property from the input, such as $-.prop_name
    const Value& getInputProp(const std::string& prop) const override;

    // Get Vertex
    Value getVertex() const override;

    // Get Edge
    Value getEdge() const override;

    void setVar(const std::string&, Value val) override;

    void setIter(Iterator* iter) {
        iter_ = iter;
    }

private:
    ExecutionContext*                 ectx_;
    Iterator*                         iter_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_QUERYEXPRESSIONCONTEXT_H_
