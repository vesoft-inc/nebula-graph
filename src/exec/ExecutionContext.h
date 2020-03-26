/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_EXECUTIONCONTEXT_H_
#define EXEC_EXECUTIONCONTEXT_H_

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/Status.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace graph {

class ExecutionContext final {
public:
    ExecutionContext() = default;

    ~ExecutionContext() = default;

    void setValueReservation(int64_t valueReservation) {
        valueReservation_ = valueReservation;
    }

    nebula::cpp2::Value getValue(const std::string& varName) const;

    // Add named or anonymous variables value
    // TODO: Consider variable reference implementation
    Status addValue(const std::string& varName, nebula::cpp2::Value value);

private:
    // Check whether values have reached the limitation
    Status checkValues() const;

    // Count all values of this plan fragment
    uint64_t valueCounts_;

    // Number of value reservation for all executors
    int64_t valueReservation_{-1};

    // Store all variable's values of query globally
    std::unordered_map<std::string, std::vector<nebula::cpp2::Value>> variables_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_EXECUTIONCONTEXT_H_
