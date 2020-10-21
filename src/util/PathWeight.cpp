/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/PathWeight.h"

namespace nebula {
namespace graph {

double EdgeWeight::getWeight(Edge& edge, Value& src, const Value& dst) {
    UNUSED(src);
    UNUSED(dst);
    if (edge.name != edgeName_) {
        LOG(FATAL) << "Wrong Edge Name : " << edge.name << " should be " << edgeName_;
    }
    auto& props = edge.props;
    auto result = props[propName_];

    switch (result.type()) {
        case Value::Type::INT:
            return result.getInt();
        case Value::Type::FLOAT:
            return result.getFloat();
        default :
            LOG(FATAL) << "Wrong data type :" << result.typeName();
    }
}

}  // namespace graph
}  // namespace nebula
