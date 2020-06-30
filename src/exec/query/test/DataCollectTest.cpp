/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/DataCollectExecutor.h"

namespace nebula {
namespace graph {
class DataCollectTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        qctx_ = std::make_unique<QueryContext>();
        {
            DataSet ds1;
            ds1.colNames = {"_vid",
                            "_stats",
                            "_tag:tag1:prop1:prop2",
                            "_edge:+edge1:prop1:prop2:_dst:_rank",
                            "_expr"};
            for (auto i = 0; i < 10; ++i) {
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
                        edge.values.emplace_back(k);
                    }
                    edge.values.emplace_back("2");
                    edge.values.emplace_back(j);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds1.rows.emplace_back(std::move(row));
            }

            DataSet ds2;
            ds2.colNames = {"_vid",
                            "_stats",
                            "_tag:tag2:prop1:prop2",
                            "_edge:-edge2:prop1:prop2:_dst:_rank",
                            "_expr"};
            for (auto i = 10; i < 20; ++i) {
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
                        edge.values.emplace_back(k);
                    }
                    edge.values.emplace_back("2");
                    edge.values.emplace_back(j);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds2.rows.emplace_back(std::move(row));
            }

            List datasets;
            datasets.values.emplace_back(std::move(ds1));
            datasets.values.emplace_back(std::move(ds2));

            qctx_->ectx()->setResult("input_datasets",
                        ExecResult::buildGetNeighbors(Value(std::move(datasets)), State()));
        }
        {
            DataSet ds;
            ds.colNames = {"_vid", "col2"};
            qctx_->ectx()->setResult("empty",
                        ExecResult::buildSequential(Value(std::move(ds)), State()));
        }
    }

protected:
    static std::unique_ptr<QueryContext> qctx_;
};

std::unique_ptr<QueryContext> DataCollectTest::qctx_;

TEST_F(DataCollectTest, CollectSubgraph) {
    auto* plan = qctx_->plan();
    auto* dc = DataCollect::make(plan, nullptr,
            DataCollect::CollectKind::kSubgraph, {"input_datasets"});
    dc->setColNames(std::vector<std::string>{"_vertices", "_edges"});

    auto dcExe = std::make_unique<DataCollectExecutor>(dc, qctx_.get());
    auto future = dcExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(dc->varName());

    DataSet expected;
    expected.colNames = {"_vertices", "_edges"};
    auto& input = qctx_->ectx()->getResult("input_datasets");
    auto iter = input.iter();
    auto* gNIter = static_cast<GetNeighborsIter*>(iter.get());
    Row row;
    row.values.emplace_back(gNIter->getVertices());
    row.values.emplace_back(gNIter->getEdges());
    expected.rows.emplace_back(std::move(row));

    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
}

TEST_F(DataCollectTest, EmptyResult) {
    auto* plan = qctx_->plan();
    auto* dc = DataCollect::make(plan, nullptr,
            DataCollect::CollectKind::kSubgraph, {"empty"});
    dc->setColNames(std::vector<std::string>{"_vertices", "_edges"});

    auto dcExe = std::make_unique<DataCollectExecutor>(dc, qctx_.get());
    auto future = dcExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(dc->varName());

    DataSet expected;
    expected.colNames = {"_vertices", "_edges"};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
}
}  // namespace graph
}  // namespace nebula
