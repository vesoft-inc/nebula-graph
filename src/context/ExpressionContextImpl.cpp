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
    if (ectx_ == nullptr) {
        return kEmpty;
    }
    return ectx_->getValue(var).value();
}

const Value& ExpressionContextImpl::getVersionedVar(const std::string& var,
                                                    int64_t version) const {
    if (ectx_ == nullptr) {
        return kEmpty;
    }
    auto& result = ectx_->getHistory(var);
    auto& val = result.value();
    if (version <= 0 && static_cast<size_t>(std::abs(version)) < val.size()) {
        return val[val.size() + version -1];
    } else if (version > 0 && static_cast<size_t>(version) <= val.size()) {
        return val[version - 1];
    } else {
        return kEmpty;
    }
}

const Value& ExpressionContextImpl::getVarProp(const std::string& var,
                                               const std::string& prop) const {
    if (iter_ != nullptr) {
        return iter_->getProp(prop);
    } else {
        // TODO
        return kEmpty;
    }
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
    return iter_->getProp(prop);
}

void ExpressionContextImpl::setVar(const std::string& var, Value val) {
    ectx_->setValue(var, std::move(val));
}
}  // namespace graph
}  // namespace nebula
