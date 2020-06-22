/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_TEST_EXECUTORTESTBASE_H_
#define EXEC_QUERY_TEST_EXECUTORTESTBASE_H_

#include <algorithm>

#include <gtest/gtest.h>

#include "common/datatypes/DataSet.h"
#include "context/Iterator.h"
#include "context/QueryContext.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

class ExecutorTestBase : public ::testing::Test {
public:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        plan_ = qctx_->plan();
    }

    static DataSet iterateDataSet(const std::vector<std::string>& colNames, Iterator* iter) {
        DataSet ds;
        ds.colNames = colNames;

        for (; iter->valid(); iter->next()) {
            Row row;
            for (auto& col : colNames) {
                row.emplace_back(iter->getColumn(col));
            }
            ds.emplace_back(std::move(row));
        }
        return ds;
    }

    static bool diffDataSet(const DataSet& lhs, const DataSet& rhs) {
        if (lhs.colNames != rhs.colNames) return false;
        if (lhs.rows.size() != rhs.rows.size()) return false;

        auto comp = [](const Row& l, const Row& r) -> bool {
            for (size_t i = 0; i < l.columns.size(); ++i) {
                if (!(l.columns[i] < r.columns[i])) return false;
            }
            return true;
        };

        // Following sort will change the input data sets, so make the copies
        auto l = lhs;
        auto r = rhs;
        std::sort(l.rows.begin(), l.rows.end(), comp);
        std::sort(r.rows.begin(), r.rows.end(), comp);
        return l.rows == r.rows;
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    ExecutionPlan* plan_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_TEST_EXECUTORTESTBASE_H_
