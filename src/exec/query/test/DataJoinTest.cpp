/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/DataJoinExecutor.h"
#include "exec/query/test/QueryTestBase.h"

namespace nebula {
namespace graph {
class DataJoinTest : public QueryTestBase {
protected:
    static void SetUpTestCase() {
        qctx_ = std::make_unique<QueryContext>();
        {
            DataSet ds1;
            ds1.colNames = {kVid,
                            "_stats",
                            "_tag:tag1:prop1:prop2",
                            "_edge:+edge1:prop1:prop2:_dst:_rank",
                            "_expr"};
            for (auto i = 0; i < 5; ++i) {
                Row row;
                // _vid
                row.values.emplace_back(folly::to<std::string>(i));
                // _stats = empty
                row.values.emplace_back(Value());
                // tag
                List tag;
                tag.values.emplace_back(0);
                tag.values.emplace_back(1);
                row.values.emplace_back(Value(tag));
                // edges
                List edges;
                for (auto j = 0; j < 2; ++j) {
                    List edge;
                    for (auto k = 0; k < 2; ++k) {
                        // prop1, prop2
                        edge.values.emplace_back(k);
                    }
                    // _dst
                    edge.values.emplace_back(folly::to<std::string>(i + 5 + j));
                    // _rank
                    edge.values.emplace_back(j);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds1.rows.emplace_back(std::move(row));
            }

            List datasets;
            datasets.values.emplace_back(std::move(ds1));
            ResultBuilder builder;
            builder.value(Value(std::move(datasets))).iter(Iterator::Kind::kGetNeighbors);
            qctx_->ectx()->setResult("var1", builder.finish());
        }
        {
            DataSet ds;
            ds.colNames = {"src", "dst"};
            for (auto i = 0; i < 5; ++i) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(i + 11));
                row.values.emplace_back(folly::to<std::string>(i));
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("var2",
                                     ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {kVid,
                            "_stats",
                            "_tag:tag1:prop1:prop2",
                            "_edge:+edge1:prop1:prop2:_dst:_rank",
                            "_expr"};
            qctx_->ectx()->setResult("empty_var1",
                                     ResultBuilder()
                                         .value(Value(std::move(ds)))
                                         .iter(Iterator::Kind::kGetNeighbors)
                                         .finish());
        }
        {
            DataSet ds;
            ds.colNames = {"src", "dst"};
            qctx_->ectx()->setResult("empty_var2",
                                     ResultBuilder().value(Value(std::move(ds))).finish());
        }
    }

protected:
    static std::unique_ptr<QueryContext> qctx_;
};

std::unique_ptr<QueryContext> DataJoinTest::qctx_;

#define JOIN(LEFT, RIGHT)                                                     \
    VariablePropertyExpression key(new std::string(LEFT),                     \
                                   new std::string("dst"));                   \
    std::vector<Expression*> hashKeys = {&key};                               \
    VariablePropertyExpression probe(new std::string(RIGHT),                  \
                                     new std::string("_vid"));                \
    std::vector<Expression*> probeKeys = {&probe};                            \
    YieldColumns lhsCols;                                                     \
    auto* lhsCol =                                                            \
        new YieldColumn(new VariablePropertyExpression(                       \
                            new std::string("var2"), new std::string("src")), \
                        new std::string("src"));                              \
    lhsCols.addColumn(lhsCol);                                                \
    YieldColumns rhsCols;                                                     \
    auto* rhsCol =                                                            \
        new YieldColumn(new EdgePropertyExpression(new std::string("*"),      \
                                                   new std::string(kDst)),    \
                        new std::string("dst"));                              \
    rhsCols.addColumn(rhsCol);                                                \
                                                                              \
    auto* plan = qctx_->plan();                                               \
    auto* dataJoin =                                                          \
        DataJoin::make(plan, nullptr, {LEFT, RIGHT}, std::move(hashKeys),     \
                       std::move(probeKeys), &lhsCols, &rhsCols);             \
    dataJoin->setColNames(std::vector<std::string>{"src", "dst"});            \
                                                                              \
    auto dataJoinExe =                                                        \
        std::make_unique<DataJoinExecutor>(dataJoin, qctx_.get());            \
    auto future = dataJoinExe->execute();                                     \
    auto status = std::move(future).get();                                    \
    EXPECT_TRUE(status.ok());                                                 \
    auto& result = qctx_->ectx()->getResult(dataJoin->varName());

TEST_F(DataJoinTest, Join) {
    // $var1 inner join $var2 on $var2.dst = $var1._vid
    JOIN("var2", "var1")

    DataSet expected;
    expected.colNames = {"src", "dst"};
    for (auto i = 11; i < 16; ++i) {
        Row row1;
        row1.values.emplace_back(folly::to<std::string>(i));
        row1.values.emplace_back(folly::to<std::string>(i - 6));
        expected.rows.emplace_back(std::move(row1));

        Row row2;
        row2.values.emplace_back(folly::to<std::string>(i));
        row2.values.emplace_back(folly::to<std::string>(i - 5));
        expected.rows.emplace_back(std::move(row2));
    }

    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(DataJoinTest, JoinEmpty) {
    {
        JOIN("var2", "empty_var1")

        DataSet expected;
        expected.colNames = {"src", "dst"};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        JOIN("empty_var2", "var1")

        DataSet expected;
        expected.colNames = {"src", "dst"};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        JOIN("empty_var2", "empty_var1")

        DataSet expected;
        expected.colNames = {"src", "dst"};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
}
}  // namespace graph
}  // namespace nebula
