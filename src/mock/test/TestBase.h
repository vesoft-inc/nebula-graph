/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_TEST_TESTBASE_H_
#define MOCK_TEST_TESTBASE_H_

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/interface/gen-cpp2/GraphService.h"

namespace nebula {
namespace graph {

class TestBase : public ::testing::Test {
protected:
    void SetUp() override;

    void TearDown() override;

    static ::testing::AssertionResult verifyDataSetWithoutOrder(const cpp2::ExecutionResponse &resp,
                                                                DataSet &expected) {
        if (resp.get_error_code() != cpp2::ErrorCode::SUCCEEDED) {
            return ::testing::AssertionFailure() << "query failed: "
                << cpp2::_ErrorCode_VALUES_TO_NAMES.at(resp.get_error_code());
        }
        if (!resp.__isset.data) {
            return ::testing::AssertionFailure() << "No data in response";
        }
        const auto &data = *resp.get_data();
        if (data.empty()) {
            return ::testing::AssertionFailure() << "No data in response";
        }
        auto dataSet = data.front();
        std::sort(dataSet.rows.begin(), dataSet.rows.end());
        std::sort(expected.rows.begin(), expected.rows.end());
        if (dataSet != expected) {
            return ::testing::AssertionFailure() << "Not match data set" << std::endl
                << "Resp: " << std::endl
                << dataSet
                << "Expected: " << std::endl
                << expected;
        } else {
            return ::testing::AssertionSuccess();
        }
    }
};

#define ASSERT_ERROR_CODE(resp, expected) ASSERT_EQ(resp.get_error_code(), expected) \
    << "Expect: " << cpp2::_ErrorCode_VALUES_TO_NAMES.at(expected) << ", " \
    << "In fact: " << cpp2::_ErrorCode_VALUES_TO_NAMES.at(resp.get_error_code())

}   // namespace graph
}   // namespace nebula

#endif  // MOCK_TEST_TESTBASE_H_
