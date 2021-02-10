/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "context/Iterator.h"
#include "common/datatypes/Vertex.h"
#include "common/datatypes/Edge.h"

namespace nebula {
namespace graph {

TEST(IteratorTest, Default) {
    auto constant = std::make_shared<Value>(1);
    DefaultIter iter(constant);
    EXPECT_EQ(iter.size(), 1);
    for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
        EXPECT_EQ(*iter.valuePtr(), *constant);
    }
}

TEST(IteratorTest, Sequential) {
    DataSet ds;
    ds.colNames = {"col1", "col2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(i);
        row.values.emplace_back(folly::to<std::string>(i));
        ds.rows.emplace_back(std::move(row));
    }
    {
        auto val = std::make_shared<Value>(ds);
        SequentialIter iter(val);
        EXPECT_EQ(iter.size(), 10);
        auto i = 0;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            EXPECT_EQ(cur->get()->getColumn("col1", &iter), i);
            EXPECT_EQ(cur->get()->getColumn("col2", &iter), folly::to<std::string>(i));
            ++i;
        }
    }
    {
        auto val = std::make_shared<Value>(ds);
        SequentialIter iter(val);
        auto copyIter1 = iter.copy();
        auto copyIter2 = copyIter1->copy();
        EXPECT_EQ(copyIter2->size(), 10);
        auto i = 0;
        for (auto cur = copyIter2->begin(); copyIter2->valid(cur); ++cur) {
            EXPECT_EQ(cur->get()->getColumn("col1", copyIter2.get()), i);
            EXPECT_EQ(cur->get()->getColumn("col2", copyIter2.get()), folly::to<std::string>(i));
            ++i;
        }
    }
    // erase
    {
        auto val = std::make_shared<Value>(std::move(ds));
        SequentialIter iter(val);
        EXPECT_EQ(iter.size(), 10);
        auto cur = iter.begin();
        while (iter.valid(cur)) {
            if (cur->get()->getColumn("col1", &iter).getInt() % 2 == 0) {
                cur = iter.erase(cur);
            } else {
                ++cur;
            }
        }
        int32_t count = 0;
        for (auto cur2 = iter.begin(); iter.valid(cur2); ++cur2) {
            EXPECT_NE((*cur2)->getColumn("col1", &iter).getInt() % 2, 0);
            count++;
        }

        for (auto cur3 = iter.begin() + 1; iter.valid(cur3); ++cur3) {
            count--;
        }
        EXPECT_EQ(count, 1);
    }
}

TEST(IteratorTest, GetNeighbor) {
    DataSet ds1;
    ds1.colNames = {kVid,
                    "_stats",
                    "_tag:tag1:prop1:prop2",
                    "_edge:+edge1:prop1:prop2:_dst:_type:_rank",
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
            edge.values.emplace_back(1);
            edge.values.emplace_back(j);
            edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        // _expr = empty
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
    }

    DataSet ds2;
    ds2.colNames = {kVid,
                    "_stats",
                    "_tag:tag2:prop1:prop2",
                    "_edge:-edge2:prop1:prop2:_dst:_type:_rank",
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
            edge.values.emplace_back(-2);
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
    auto val = std::make_shared<Value>(std::move(datasets));

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected =
            {"0", "0", "1", "1", "2", "2", "3", "3", "4", "4",
             "5", "5", "6", "6", "7", "7", "8", "8", "9", "9",
             "10", "10", "11", "11", "12", "12", "13", "13", "14", "14",
             "15", "15", "16", "16", "17", "17", "18", "18", "19", "19"};
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            result.emplace_back(cur->get()->getColumn(kVid, &iter));
        }
        EXPECT_EQ(expected, result);
    }

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, 0);
        expected.insert(expected.end(), 20, Value());
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            result.emplace_back(cur->get()->getTagProp("tag1", "prop1", &iter));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, Value());
        expected.insert(expected.end(), 20, 0);
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            result.emplace_back(cur->get()->getTagProp("tag2", "prop1", &iter));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, 0);
        expected.insert(expected.end(), 20, Value());
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            result.emplace_back(cur->get()->getEdgeProp("edge1", "prop1", &iter));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, Value());
        expected.insert(expected.end(), 20, 0);
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            result.emplace_back(cur->get()->getEdgeProp("edge2", "prop1", &iter));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }
    {
        GetNeighborsIter iter(val);
        auto copyIter1 = iter.copy();
        auto copyIter2 = copyIter1->copy();
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, Value());
        expected.insert(expected.end(), 20, 0);
        std::vector<Value> result;
        for (auto cur = copyIter2->begin(); copyIter2->valid(cur); ++cur) {
            result.emplace_back(cur->get()->getEdgeProp("edge2", "prop1", copyIter2.get()));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }
    // erase
    {
        GetNeighborsIter iter(val);
        size_t i = 0;
        auto cur = iter.begin();
        while (iter.valid(cur)) {
            ++i;
            if (i % 2 == 0) {
                cur = iter.erase(cur);
            } else {
                ++cur;
            }
        }
        std::vector<Value> expected =
                {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "10", "11", "12", "13", "14", "15", "16", "17", "18", "19"};
        std::vector<Value> result;

        int count = 0;
        for (auto cur2 = iter.begin(); iter.valid(cur2); ++cur2) {
            result.emplace_back((*cur2)->getColumn(kVid, &iter));
            count++;
        }
        EXPECT_EQ(result.size(), 20);
        EXPECT_EQ(expected, result);

        for (auto cur3 = iter.begin() + 10; iter.valid(cur3); ++cur3) {
            count--;
        }
        EXPECT_EQ(count, 10);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        Tag tag1;
        tag1.name = "tag1";
        tag1.props = {{"prop1", 0}, {"prop2", 1}};
        for (size_t i = 0; i < 10; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            vertex.tags.emplace_back(tag1);
            expected.emplace_back(vertex);
            expected.emplace_back(std::move(vertex));
        }
        Tag tag2;
        tag2.name = "tag2";
        tag2.props = {{"prop1", 0}, {"prop2", 1}};
        for (size_t i = 10; i < 20; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            vertex.tags.emplace_back(tag2);
            expected.emplace_back(vertex);
            expected.emplace_back(std::move(vertex));
        }
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            auto v = cur->get()->getVertex(&iter);
            result.emplace_back(std::move(v));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(result, expected);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        for (size_t i = 0; i < 10; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                EdgeRanking ranking = static_cast<int64_t>(j);
                Edge edge;
                edge.name = "edge1";
                edge.type = 1;
                edge.src = folly::to<std::string>(i);
                edge.dst = "2";
                edge.ranking = ranking;
                edge.props = {{"prop1", 0}, {"prop2", 1}};
                expected.emplace_back(std::move(edge));
            }
        }
        for (size_t i = 10; i < 20; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                EdgeRanking ranking = static_cast<int64_t>(j);
                Edge edge;
                edge.name = "edge2";
                edge.type = -2;
                edge.src = folly::to<std::string>(i);
                edge.dst = "2";
                edge.ranking = ranking;
                edge.props = {{"prop1", 0}, {"prop2", 1}};
                expected.emplace_back(std::move(edge));
            }
        }
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            auto e = cur->get()->getEdge(&iter);
            result.emplace_back(std::move(e));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(result, expected);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        Tag tag1;
        tag1.name = "tag1";
        tag1.props = {{"prop1", 0}, {"prop2", 1}};
        for (size_t i = 0; i < 10; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            vertex.tags.emplace_back(tag1);
            expected.emplace_back(vertex);
            expected.emplace_back(std::move(vertex));
        }
        Tag tag2;
        tag2.name = "tag2";
        tag2.props = {{"prop1", 0}, {"prop2", 1}};
        for (size_t i = 10; i < 20; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            vertex.tags.emplace_back(tag2);
            expected.emplace_back(vertex);
            expected.emplace_back(std::move(vertex));
        }
        List result = iter.getVertices();
        EXPECT_EQ(result.values.size(), 40);
        EXPECT_EQ(result.values, expected);

        result = iter.getVertices();
        EXPECT_EQ(result.values.size(), 40);
        EXPECT_EQ(result.values, expected);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        for (size_t i = 0; i < 10; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                EdgeRanking ranking = static_cast<int64_t>(j);
                Edge edge;
                edge.name = "edge1";
                edge.type = 1;
                edge.src = folly::to<std::string>(i);
                edge.dst = "2";
                edge.ranking = ranking;
                edge.props = {{"prop1", 0}, {"prop2", 1}};
                expected.emplace_back(std::move(edge));
            }
        }
        for (size_t i = 10; i < 20; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                EdgeRanking ranking = static_cast<int64_t>(j);
                Edge edge;
                edge.name = "edge2";
                edge.type = 2;
                edge.src = "2";
                edge.dst = folly::to<std::string>(i);
                edge.ranking = ranking;
                edge.props = {{"prop1", 0}, {"prop2", 1}};
                expected.emplace_back(std::move(edge));
            }
        }
        List result = iter.getEdges();
        EXPECT_EQ(result.values.size(), 40);
        EXPECT_EQ(result.values, expected);

        result = iter.getEdges();
        EXPECT_EQ(result.values.size(), 40);
        EXPECT_EQ(result.values, expected);
    }
    // unstableErase
    {
        GetNeighborsIter iter(val);
        size_t i = 0;
        auto cur = iter.begin();
        while (iter.valid(cur)) {
            ++i;
            if (i % 2 == 0) {
                cur = iter.unstableErase(cur);
            } else {
                ++cur;
            }
        }
        std::vector<Value> result;

        int count = 0;
        for (auto cur2 = iter.begin(); iter.valid(cur2); ++cur2) {
            result.emplace_back((*cur2)->getColumn(kVid, &iter));
            count++;
        }
        EXPECT_EQ(result.size(), 20);

        for (auto cur3 = iter.begin() + 10; iter.valid(cur3); ++cur3) {
            count--;
        }
        EXPECT_EQ(count, 10);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            auto v = cur->get()->getVertex(&iter);
            result.emplace_back(std::move(v));
        }
        EXPECT_EQ(result.size(), 40);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            auto e = cur->get()->getEdge(&iter);
            result.emplace_back(std::move(e));
        }
        EXPECT_EQ(result.size(), 40);
    }
    {
        GetNeighborsIter iter(val);
        List result = iter.getVertices();
        EXPECT_EQ(result.values.size(), 40);

        result = iter.getVertices();
        EXPECT_EQ(result.values.size(), 40);
    }
    {
        GetNeighborsIter iter(val);
        List result = iter.getEdges();
        EXPECT_EQ(result.values.size(), 40);

        result = iter.getEdges();
        EXPECT_EQ(result.values.size(), 40);
    }
}

TEST(IteratorTest, TestHead) {
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
    }

    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
    }
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
    }
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
    }
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1",
                        "_edge:+edge1:",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
    }

    {
        // no _vid
        DataSet ds;
        ds.colNames = {"_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
    }
    {
        // no _stats
        DataSet ds;
        ds.colNames = {kVid,
                        "_tag:tag1:prop1:prop2",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
    }
    {
        // no _expr
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:+edge1:prop1:prop2:_dst:_rank"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
    }
    {
        // no +/- before edge name
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
    }
    // no prop
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
    }
    // no prop
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
    }
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:::",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
    }
}

TEST(IteratorTest, EraseRange) {
    // Sequential iterator
    {
        DataSet ds({"col1", "col2"});
        for (auto i = 0; i < 10; ++i) {
            ds.rows.emplace_back(Row({i, folly::to<std::string>(i)}));
        }
        // erase out of range pos
        {
            auto val = std::make_shared<Value>(ds);
            SequentialIter iter(val);
            iter.eraseRange(5, 11);
            ASSERT_EQ(iter.size(), 5);
            auto i = 0;
            for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
                ASSERT_EQ(cur->get()->getColumn("col1", &iter), i);
                ASSERT_EQ(cur->get()->getColumn("col2", &iter), folly::to<std::string>(i));
                ++i;
            }
        }
        // erase in range
        {
            auto val = std::make_shared<Value>(ds);
            SequentialIter iter(val);
            iter.eraseRange(0, 10);
            ASSERT_EQ(iter.size(), 0);
        }
        // erase part
        {
            auto val = std::make_shared<Value>(ds);
            SequentialIter iter(val);
            iter.eraseRange(0, 5);
            EXPECT_EQ(iter.size(), 5);
            auto i = 5;
            for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
                ASSERT_EQ(cur->get()->getColumn("col1", &iter), i);
                ASSERT_EQ(cur->get()->getColumn("col2", &iter), folly::to<std::string>(i));
                ++i;
            }
        }
    }
}

TEST(IteratorTest, Join) {
    DataSet ds1;
    ds1.colNames = {kVid, "tag_prop", "edge_prop", kDst};
    auto val1 = std::make_shared<Value>(ds1);
    SequentialIter iter1(val1);

    DataSet ds2;
    ds2.colNames = {"src", "dst"};
    auto val2 = std::make_shared<Value>(ds2);
    SequentialIter iter2(val2);

    Row row1;
    row1.values = {"1", 1, 2, "2"};
    Row row2;
    row2.values = {"3", "4"};
    JoinIter joinIter({kVid, "tag_prop", "edge_prop", kDst, "src", "dst"});
    joinIter.joinIndex(&iter1, &iter2);
    EXPECT_EQ(joinIter.getColIdxIndices().size(), 6);
    EXPECT_EQ(joinIter.getColIdxIndices().size(), 6);
    joinIter.addRow(new JoinIter::JoinLogicalRow({&row1, &row2}, 6,
                                                &joinIter.getColIdxIndices()));
    joinIter.addRow(new JoinIter::JoinLogicalRow({&row1, &row2}, 6,
                                                &joinIter.getColIdxIndices()));

    for (auto cur = joinIter.begin(); joinIter.valid(cur); ++cur) {
        const auto& row = *cur->get();
        EXPECT_EQ(row.size(), 6);
        std::vector<Value> result;
        for (size_t i = 0; i < 6; ++i) {
            result.emplace_back(row[i]);
        }
        EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4"}));
    }

    for (auto cur = joinIter.begin(); joinIter.valid(cur); ++cur) {
        const auto& row = *cur->get();
        EXPECT_EQ(row.size(), 6);
        std::vector<Value> result;
        result.emplace_back(cur->get()->getColumn(kVid, &joinIter));
        result.emplace_back(cur->get()->getColumn("tag_prop", &joinIter));
        result.emplace_back(cur->get()->getColumn("edge_prop", &joinIter));
        result.emplace_back(cur->get()->getColumn(kDst, &joinIter));
        result.emplace_back(cur->get()->getColumn("src", &joinIter));
        result.emplace_back(cur->get()->getColumn("dst", &joinIter));
        EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4"}));
    }

    {
        // The iterator and executors will not handle the duplicate columns,
        // so the duplicate column will be covered by later one.
        JoinIter joinIter1({"src", "dst", kVid, "tag_prop", "edge_prop", kDst, "src", "dst"});
        joinIter1.joinIndex(&iter2, &joinIter);
        EXPECT_EQ(joinIter.getColIndices().size(), 6);
    }

    {
        DataSet ds3;
        ds3.colNames = {"tag_prop1", "edge_prop1"};
        auto val3 = std::make_shared<Value>(ds3);
        SequentialIter iter3(val3);

        Row row3;
        row3.values = {"5", "6"};

        JoinIter joinIter2(
            {"tag_prop1", "edge_prop1", kVid, "tag_prop", "edge_prop", kDst, "src", "dst"});
        joinIter2.joinIndex(&iter3, &joinIter);
        EXPECT_EQ(joinIter2.getColIndices().size(), 8);
        EXPECT_EQ(joinIter2.getColIdxIndices().size(), 8);
        joinIter2.addRow(new JoinIter::JoinLogicalRow({ &row3, &row1, &row2}, 8,
                                                &joinIter2.getColIdxIndices()));
        joinIter2.addRow(new JoinIter::JoinLogicalRow({ &row3, &row1, &row2}, 8,
                                                &joinIter2.getColIdxIndices()));

        for (auto cur = joinIter2.begin(); joinIter2.valid(cur); ++cur) {
            const auto& row = *cur->get();
            EXPECT_EQ(row.size(), 8);
            std::vector<Value> result;
            for (size_t i = 0; i < 8; ++i) {
                result.emplace_back(row[i]);
            }
            EXPECT_EQ(result, std::vector<Value>({"5", "6", "1", 1, 2, "2", "3", "4"}));
        }

        for (auto cur = joinIter2.begin(); joinIter2.valid(cur); ++cur) {
            const auto& row = *cur->get();
            EXPECT_EQ(row.size(), 8);
            std::vector<Value> result;
            result.emplace_back(cur->get()->getColumn(kVid, &joinIter2));
            result.emplace_back(cur->get()->getColumn("tag_prop", &joinIter2));
            result.emplace_back(cur->get()->getColumn("edge_prop", &joinIter2));
            result.emplace_back(cur->get()->getColumn(kDst, &joinIter2));
            result.emplace_back(cur->get()->getColumn("src", &joinIter2));
            result.emplace_back(cur->get()->getColumn("dst", &joinIter2));
            result.emplace_back(cur->get()->getColumn("tag_prop1", &joinIter2));
            result.emplace_back(cur->get()->getColumn("edge_prop1", &joinIter2));
            EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4", "5", "6"}));
        }
    }
    {
        DataSet ds3;
        ds3.colNames = {"tag_prop1", "edge_prop1"};
        auto val3 = std::make_shared<Value>(ds3);
        SequentialIter iter3(val3);

        Row row3;
        row3.values = {"5", "6"};

        JoinIter joinIter2(
            {kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "tag_prop1", "edge_prop1"});
        joinIter2.joinIndex(&joinIter, &iter3);
        EXPECT_EQ(joinIter2.getColIndices().size(), 8);
        EXPECT_EQ(joinIter2.getColIdxIndices().size(), 8);
        joinIter2.addRow(new JoinIter::JoinLogicalRow({ &row1, &row2, &row3 }, 8,
                                                &joinIter2.getColIdxIndices()));
        joinIter2.addRow(new JoinIter::JoinLogicalRow({ &row1, &row2, &row3 }, 8,
                                                &joinIter2.getColIdxIndices()));

        for (auto cur = joinIter2.begin(); joinIter2.valid(cur); ++cur) {
            const auto& row = *cur->get();
            EXPECT_EQ(row.size(), 8);
            std::vector<Value> result;
            for (size_t i = 0; i < 8; ++i) {
                result.emplace_back(row[i]);
            }
            EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4", "5", "6"}));
        }

        for (auto cur = joinIter2.begin(); joinIter2.valid(cur); ++cur) {
            const auto& row = *cur->get();
            EXPECT_EQ(row.size(), 8);
            std::vector<Value> result;
            result.emplace_back(cur->get()->getColumn(kVid, &joinIter2));
            result.emplace_back(cur->get()->getColumn("tag_prop", &joinIter2));
            result.emplace_back(cur->get()->getColumn("edge_prop", &joinIter2));
            result.emplace_back(cur->get()->getColumn(kDst, &joinIter2));
            result.emplace_back(cur->get()->getColumn("src", &joinIter2));
            result.emplace_back(cur->get()->getColumn("dst", &joinIter2));
            result.emplace_back(cur->get()->getColumn("tag_prop1", &joinIter2));
            result.emplace_back(cur->get()->getColumn("edge_prop1", &joinIter2));
            EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4", "5", "6"}));
        }
    }
}

TEST(IteratorTest, VertexProp) {
    DataSet ds;
    ds.colNames = {kVid, "tag1.prop1", "tag2.prop1", "tag2.prop2", "tag3.prop1", "tag3.prop2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        // _vid
        row.values.emplace_back(folly::to<std::string>(i));

        row.values.emplace_back(11);
        row.values.emplace_back(Value());
        row.values.emplace_back(Value());
        row.values.emplace_back(31);
        row.values.emplace_back(32);

        ds.rows.emplace_back(std::move(row));
    }
    auto val = std::make_shared<Value>(std::move(ds));
    {
        PropIter iter(val);
        std::vector<Value> expected =
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            result.emplace_back(cur->get()->getColumn(kVid, &iter));
        }
        EXPECT_EQ(expected, result);
    }
    {
        PropIter iter(val);
        std::vector<Value> expected;
        for (size_t i = 0; i < 10; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            Tag tag1;
            tag1.name = "tag1";
            tag1.props = {{"prop1", 11}};
            Tag tag3;
            tag3.name = "tag3";
            tag3.props = {{"prop1", 31}, {"prop2", 32}};
            vertex.tags.emplace_back(tag3);
            vertex.tags.emplace_back(tag1);
            expected.emplace_back(std::move(vertex));
        }
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            auto v = cur->get()->getVertex(&iter);
            result.emplace_back(std::move(v));
        }
        EXPECT_EQ(result.size(), 10);
        EXPECT_EQ(result, expected);
    }
}

TEST(IteratorTest, EdgeProp) {
    DataSet ds;
    ds.colNames = {"like._src",
                   "like._type",
                   "like._rank",
                   "like._dst",
                   "like.prop1",
                   "like.prop2",
                   "serve.prop1",
                   "serve.prop2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(folly::to<std::string>(i));
        row.values.emplace_back(2);
        row.values.emplace_back(0);
        row.values.emplace_back(folly::to<std::string>(i * 2 + 3));
        row.values.emplace_back("hello");
        row.values.emplace_back("world");
        row.values.emplace_back(Value());
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
    }
    auto val = std::make_shared<Value>(std::move(ds));
    {
        PropIter iter(val);
        std::vector<Value> expected =
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            result.emplace_back(cur->get()->getEdgeProp("like", kSrc, &iter));
        }
        EXPECT_EQ(expected, result);
    }
    {
        PropIter iter(val);
        std::vector<Value> expected;
        for (size_t i = 0; i < 10; ++i) {
            Edge edge;
            edge.src = folly::to<std::string>(i);
            edge.dst = folly::to<std::string>(i * 2 + 3);
            edge.type = 2;
            edge.ranking = 0;
            edge.name = "like";
            edge.props = {{"prop1", "hello"}, {"prop2", "world"}};
            expected.emplace_back(std::move(edge));
        }
        std::vector<Value> result;
        for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
            auto v = cur->get()->getEdge(&iter);
            result.emplace_back(std::move(v));
        }
        EXPECT_EQ(result.size(), 10);
        EXPECT_EQ(result, expected);
    }
}

TEST(IteratorTest, RowEqualTo) {
    DataSet ds;
    ds.colNames = {"col1", "col2"};
    for (auto i = 0; i < 2; ++i) {
        Row row;
        row.values.emplace_back(i);
        row.values.emplace_back(folly::to<std::string>(i));
        ds.rows.emplace_back(std::move(row));
    }

    Row row;
    row.values.emplace_back(0);
    row.values.emplace_back(folly::to<std::string>(0));
    ds.rows.emplace_back(std::move(row));

    SequentialIter::SeqLogicalRow row0(&ds.rows[0]);
    SequentialIter::SeqLogicalRow row1(&ds.rows[1]);

    EXPECT_FALSE(std::equal_to<const nebula::graph::LogicalRow*>()(&row0, &row1));

    SequentialIter::SeqLogicalRow row2(&ds.rows[2]);
    EXPECT_TRUE(std::equal_to<const nebula::graph::LogicalRow*>()(&row0, &row2));
    EXPECT_TRUE(std::equal_to<const nebula::graph::LogicalRow*>()(&row0, &row0));
}

TEST(IteratorTest, EraseBySwap) {
    DataSet ds;
    ds.colNames = {"col1", "col2"};
    for (auto i = 0; i < 3; ++i) {
        Row row;
        row.values.emplace_back(i);
        row.values.emplace_back(folly::to<std::string>(i));
        ds.rows.emplace_back(std::move(row));
    }
    auto val = std::make_shared<Value>(std::move(ds));
    SequentialIter iter(val);
    EXPECT_EQ(iter.size(), 3);
    iter.unstableErase(iter.begin());
    EXPECT_EQ(iter.size(), 2);


    std::vector<Row> expected;
    {
        Row row;
        row.values.emplace_back(2);
        row.values.emplace_back("2");
        expected.emplace_back(std::move(row));
    }
    {
        Row row;
        row.values.emplace_back(1);
        row.values.emplace_back("1");
        expected.emplace_back(std::move(row));
    }
    std::vector<Row> result;
    for (auto cur = iter.begin(); iter.valid(cur); ++cur) {
        Row row;
        row.values.emplace_back(cur->get()->getColumn("col1", &iter));
        row.values.emplace_back(cur->get()->getColumn("col2", &iter));
        result.emplace_back(std::move(row));
    }
    EXPECT_EQ(result, expected);
}
}  // namespace graph
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
