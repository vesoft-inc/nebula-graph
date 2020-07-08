/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_TESTQUERYBASE_H
#define EXEC_QUERY_TESTQUERYBASE_H
#include "common/base/Base.h"

#include <gtest/gtest.h>
#include "parser/GQLParser.h"

namespace nebula {
namespace graph {

class QueryTestBase : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        // GetNeighbors
        {
            DataSet dataset({kVid, "_stats", "_tag:person:name:age",
                             "_edge:+study:_dst:start_year:end_year", "_expr"});
            {
                Row row;
                // vid
                row.values.emplace_back("Ann");

                // _stats
                row.values.emplace_back(Value::kEmpty);

                // vertices props
                List vertices({"Ann", 18});
                row.values.emplace_back(std::move(vertices));

                // edge props
                List edges;
                List edge1({"School1", 2010, 2014});
                List edge2({"School2", 2014, 2017});
                edges.values.emplace_back(std::move(edge1));
                edges.values.emplace_back(std::move(edge2));
                row.values.emplace_back(std::move(edges));

                // _expr
                row.values.emplace_back(Value::kEmpty);
                dataset.emplace_back(std::move(row));
            }
            {
                Row row;
                // vid
                row.values.emplace_back("Tom");

                // _stats
                row.values.emplace_back(Value::kEmpty);

                // vertices props
                List vertices({"Tom", 18});
                row.values.emplace_back(std::move(vertices));

                // edges props
                List edge1({"School1", 2008, 2012});
                List edge2({"School2", 2012, 2015});

                List edges;
                edges.values.emplace_back(std::move(edge1));
                edges.values.emplace_back(std::move(edge2));
                row.values.emplace_back(std::move(edges));

                // _expr
                row.values.emplace_back(Value::kEmpty);
                dataset.emplace_back(std::move(row));
            }
            List datasetList;
            datasetList.values.emplace_back(std::move(dataset));
            qctx_->ectx()->setResult("input_neighbor",
                                     ExecResult::buildGetNeighbors(Value(datasetList)));
        }
        // sequential
        {
            DataSet dataset({"vid", "v_name", "v_age", "v_dst", "e_start_year", "e_end_year"});
            dataset.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
            dataset.emplace_back(Row({"Joy", "Joy", Value::kNullValue, "School2", 2009, 2012}));
            dataset.emplace_back(Row({"Tom", "Tom", 20, "School2", 2008, 2012}));
            dataset.emplace_back(Row({"Kate", "Kate", 19, "School2", 2009, 2013}));
            dataset.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
            dataset.emplace_back(Row({"Lily", "Lily", 20, "School2", 2009, 2012}));
            qctx_->ectx()->setResult("input_sequential",
                                     ExecResult::buildSequential(Value(dataset)));
        }
        // sequential init by two sequentialIters
        {
            DataSet lds({"vid", "v_name", "v_age", "v_dst", "e_start_year", "e_end_year"});
            lds.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
            lds.emplace_back(Row({"Joy", "Joy", Value::kNullValue, "School2", 2009, 2012}));
            lds.emplace_back(Row({"Tom", "Tom", 20, "School2", 2008, 2012}));
            lds.emplace_back(Row({"Kate", "Kate", 19, "School2", 2009, 2013}));
            qctx_->ectx()->setResult("left_sequential",
                                     ExecResult::buildSequential(Value(lds)));

            DataSet rds({"vid", "v_name", "v_age", "v_dst", "e_start_year", "e_end_year"});
            rds.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
            rds.emplace_back(Row({"Lily", "Lily", 20, "School2", 2009, 2012}));
            qctx_->ectx()->setResult("right_sequential",
                                     ExecResult::buildSequential(Value(rds)));

            auto lIter = qctx_->ectx()->getResult("left_sequential").iter();
            auto rIter = qctx_->ectx()->getResult("right_sequential").iter();
            auto result = ExecResult::buildDefault(lIter->valuePtr());
            auto iter = std::make_unique<SequentialIter>(std::move(lIter), std::move(rIter));
            result.setIter(std::move(iter));
            qctx_->ectx()->setResult("union_sequential", std::move(result));
        }
        // empty
        {
            DataSet dataset({kVid, "_stats", "_tag:person:name:age",
                             "_edge:+study:_dst:start_year:end_year", "_expr"});
            qctx_->ectx()->setResult("empty",
                                     ExecResult::buildSequential(Value(dataset)));
        }
    }

    YieldSentence* getYieldSentence(const std::string &query) {
        auto ret = GQLParser().parse(query);
        CHECK(ret.ok()) << ret.status();
        sentences_ = std::move(ret).value();
        auto sens = sentences_->sentences();
        CHECK_EQ(sens.size(), 1);
        CHECK_EQ(sens[0]->kind(), Sentence::Kind::kYield);
        return static_cast<YieldSentence*>(sens[0]);
    }

    YieldColumns* getYieldColumns(const std::string &query) {
        auto yieldSentence = getYieldSentence(query);
        CHECK(yieldSentence);
        return yieldSentence->yieldColumns();
    }

    Expression* getYieldFilter(const std::string &query) {
        auto yieldSentence = getYieldSentence(query);
        CHECK(yieldSentence);
        auto where = yieldSentence->where();
        CHECK(where);
        return where->filter();
    }


protected:
    std::unique_ptr<QueryContext>         qctx_;
    std::unique_ptr<SequentialSentences>  sentences_;
};
}  // namespace graph
}  // namespace nebula
#endif  // EXEC_QUERY_TESTQUERYBASE_H

