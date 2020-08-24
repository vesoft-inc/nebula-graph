/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/GraphStatus.h"
#include "common/base/Status.h"
#include <gtest/gtest.h>

namespace nebula {
namespace graph {

TEST(GraphStatus, init) {
    GraphStatus::init();
    EXPECT_EQ(cpp2::Language::L_EN, GraphStatus::language);
    EXPECT_EQ(cpp2::Encoding::E_UTF8, GraphStatus::encoding);
}

TEST(GraphStatus, setStatus) {
    auto graphStatus = GraphStatus::OK();
    EXPECT_EQ("", graphStatus.toString());

    graphStatus = GraphStatus::setNotUseSpace();
    EXPECT_EQ("Please choose a graph space with `USE spaceName' firstly.", graphStatus.toString());

    graphStatus = GraphStatus::setSpaceNotFound("testSpace");
    EXPECT_EQ("Space not found.", graphStatus.toString());

    graphStatus = GraphStatus::setTagNotFound("testTag");
    EXPECT_EQ("Tag not found.", graphStatus.toString());

    graphStatus = GraphStatus::setEdgeNotFound("testEdge");
    EXPECT_EQ("Edge not found.", graphStatus.toString());

    graphStatus = GraphStatus::setIndexNotFound("testIndex");
    EXPECT_EQ("Index not found.", graphStatus.toString());

    graphStatus = GraphStatus::setColumnNotFound("testColumn");
    EXPECT_EQ("Column name `testColumn' not found.", graphStatus.toString());

    graphStatus = GraphStatus::setSpaceExisted("testSpace");
    EXPECT_EQ("Space `testSpace' existed.", graphStatus.toString());

    graphStatus = GraphStatus::setTagExisted("testTag");
    EXPECT_EQ("Tag `testTag' existed.", graphStatus.toString());
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

}   // namespace graph
}   // namespace nebula
