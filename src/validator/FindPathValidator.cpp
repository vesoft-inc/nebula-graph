/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FindPathValidator.h"

namespace nebula {
namespace graph {
Status FindPathValidator::validateImpl() {
    return Status::OK();
}

Status FindPathValidator::toPlan() {
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
