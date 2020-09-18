/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_INDEXUTIL_H_
#define UTIL_INDEXUTIL_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {

class IndexUtil final {
public:
    IndexUtil() = delete;

    static Status validateColumns(const std::vector<std::string>& fields);
};

}  // namespace graph
}  // namespace nebula
#endif  // UTIL_INDEXUTIL_H_
