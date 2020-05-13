/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_EXPRESSIONCONTEXT_H_
#define UTIL_EXPRESSIONCONTEXT_H_

#include "context/ExpressionContext.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {
class ExpressionContextImpl final : public ExpressionContext {
public:
    explicit ExpressionContextImpl(QueryContext* qCtxt) {
        qCtxt_ = qCtxt;
    }

    // Get the latest version value for the given variable name, such as $a, $b
    const Value& getVar(const std::string& var) const override;

    // Get the given version value for the given variable name, such as $a, $b
    const Value& getVersionedVar(const std::string& var,
                                 int64_t version) const override;

    // Get the specified property from a variable, such as $a.prop_name
    const Value& getVarProp(const std::string& var,
                            const std::string& prop) const override;

    // Get the specified property from the edge, such as edge_type.prop_name
    const Value& getEdgeProp(const std::string& edgeType,
                             const std::string& prop) const override;

    // Get the specified property from the source vertex, such as $^.prop_name
    const Value& getSrcProp(const std::string& tag,
                            const std::string& prop) const override;

    // Get the specified property from the destination vertex, such as $$.prop_name
    const Value& getDstProp(const std::string& tag,
                            const std::string& prop) const override;

    // Get the specified property from the input, such as $-.prop_name
    const Value& getInputProp(const std::string& prop) const override;

    void setVar(const std::string&, Value val) override;

private:
    QueryContext*    qCtxt_;
};
}  // namespace graph
}  // namespace nebula
#endif
