/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datatypes/Value.h"
#include "util/ExpressionContextImpl.h"

namespace nebula {
namespace graph {
const Value& ExpressionContextImpl::getVar(const std::string& var) const {
    UNUSED(var);
    return NULL_VALUE;
}

const Value& ExpressionContextImpl::getVersionedVar(const std::string& var,
                                                    size_t version) const {
    UNUSED(var);
    UNUSED(version);
    return NULL_VALUE;
}

const Value& ExpressionContextImpl::getVarProp(const std::string& var,
                                               const std::string& prop) const {
    // TODO
    UNUSED(var);
    UNUSED(prop);
    return NULL_VALUE;
}

const Value& ExpressionContextImpl::getEdgeProp(const std::string& edgeType,
                                                const std::string& prop) const {
    // TODO
    UNUSED(edgeType);
    UNUSED(prop);
    return NULL_VALUE;
}

const Value& ExpressionContextImpl::getSrcProp(const std::string& tag,
                                               const std::string& prop) const {
    // TODO
    UNUSED(tag);
    UNUSED(prop);
    return NULL_VALUE;
}

const Value& ExpressionContextImpl::getDstProp(const std::string& tag,
                                               const std::string& prop) const {
    // TODO
    UNUSED(tag);
    UNUSED(prop);
    return NULL_VALUE;
}

const Value& ExpressionContextImpl::getInputProp(const std::string& prop) const {
    // TODO
    UNUSED(prop);
    return NULL_VALUE;
}
}  // namespace graph
}  // namespace nebula
