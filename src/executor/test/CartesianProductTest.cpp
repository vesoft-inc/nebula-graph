/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Algo.h"
#include "executor/algo/CartesianProductExecutor.h"

namespace nebula {
namespace graph {
class CartesianProductTest : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        {
            DataSet ds1;
            ds1.colNames = {kSrc};
            for (auto i = 0; i < 5; ++i) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(i));
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->symTable()->newVariable("ds1");
            qctx_->ectx()->setResult("ds1",
                                     ResultBuilder()
                                         .value(Value(std::move(ds1)))
                                         .iter(Iterator::Kind::kSequential)
                                         .finish());

            DataSet ds2;
            ds2.colNames = {kDst};
            for (auto i = 0; i < 3; ++i) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(i + 'a'));
                ds2.rows.emplace_back(std::move(row));
            }

            qctx_->symTable()->newVariable("ds2");
            qctx_->ectx()->setResult("ds2",
                                     ResultBuilder()
                                         .value(Value(std::move(ds2)))
                                         .iter(Iterator::Kind::kSequential)
                                         .finish());
        }
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
};

TEST_F(CartesianProductTest, test) {
    auto* cp = CartesianProduct::make(qctx_.get(), nullptr);
    cp->addVars("ds1");
    cp->addVars("ds2");
    std::vector<std::string> colNames = {kSrc, kDst};
    cp->setColNames(colNames);

    auto cpExe = std::make_unique<CartesianProductExecutor>(cp, qctx_.get());
    auto future = cpExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(cp->outputVar());

    DataSet expected;
    expected.colNames = {kSrc, kDst};
    for (size_t i = 0; i < 5; ++i) {
        for (size_t j = 0; j < 3; ++j) {
            Row row;
            row.values.emplace_back(folly::to<std::string>(i));
            row.values.emplace_back(folly::to<std::string>(j + 'a'));
            expected.rows.emplace_back(std::move(row));
        }
    }

    DataSet resultDs;
    resultDs.colNames = colNames;
    auto iter = result.iter();
    for (; iter->valid(); iter->next()) {
        const auto& cols = *iter->row();
        Row row;
        for (size_t i = 0; i < cols.size(); ++i) {
            Value col = cols[i];
            row.values.emplace_back(std::move(col));
        }
        resultDs.rows.emplace_back(std::move(row));
    }

    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

}   // namespace graph
}   // namespace nebula
