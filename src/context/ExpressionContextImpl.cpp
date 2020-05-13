/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datatypes/Value.h"
#include "context/ExpressionContextImpl.h"

namespace nebula {
namespace graph {
const Value& ExpressionContextImpl::getVar(const std::string& var) const {
    return qCtxt_->getValue(var);
}

const Value& ExpressionContextImpl::getVersionedVar(const std::string& var,
                                                    int64_t version) const {
    auto& val = qCtxt_->getHistory(var);
    if (version <= 0 && static_cast<size_t>(std::abs(version)) < val.size()) {
        return val[val.size() + version -1];
    } else if (version > 0 && static_cast<size_t>(version) <= val.size()) {
        return val[version - 1];
    } else {
        return kEmpty;
    }
    return kNullValue;
}

const Value& ExpressionContextImpl::getVarProp(const std::string& var,
                                               const std::string& prop) const {
    // TODO
    UNUSED(var);
    UNUSED(prop);
    return kNullValue;
}

const Value& ExpressionContextImpl::getEdgeProp(const std::string& edgeType,
                                                const std::string& prop) const {
    // TODO
    UNUSED(edgeType);
    UNUSED(prop);
    return kNullValue;
}

const Value& ExpressionContextImpl::getSrcProp(const std::string& tag,
                                               const std::string& prop) const {
    // TODO
    UNUSED(tag);
    UNUSED(prop);
    return kNullValue;
}

const Value& ExpressionContextImpl::getDstProp(const std::string& tag,
                                               const std::string& prop) const {
    // TODO
    UNUSED(tag);
    UNUSED(prop);
    return kNullValue;
}

const Value& ExpressionContextImpl::getInputProp(const std::string& prop) const {
    // TODO
    UNUSED(prop);
    return kNullValue;
}

void ExpressionContextImpl::setVar(const std::string& var, Value val) {
    qCtxt_->setValue(var, std::move(val));
}
}  // namespace graph
}  // namespace nebula
