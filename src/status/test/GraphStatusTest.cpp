/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "status/GraphStatus.h"
#include "base/Status.h"
#include <gtest/gtest.h>

namespace nebula {
namespace graph {

TEST(GraphStatus, init) {
    GraphStatus::init();
    ASSERT_EQ(cpp2::Language::L_EN, GraphStatus::language);
    ASSERT_EQ(cpp2::Encoding::E_UTF8, GraphStatus::encoding);
}

TEST(GraphStatus, setStatus) {
    auto graphStatus = GraphStatus::OK();
    ASSERT_EQ("", graphStatus.toString());

    graphStatus = GraphStatus::setStatus(Status::Error("Unknow error"));
    ASSERT_EQ("Internal error: Unknow error", graphStatus.toString());

    graphStatus = GraphStatus::setNotUseSpace();
    ASSERT_EQ("Please choose a graph space with `USE spaceName' firstly", graphStatus.toString());

    graphStatus = GraphStatus::setSpaceNotFound("testSpace");
    ASSERT_EQ("Space `testSpace' not found", graphStatus.toString());

    graphStatus = GraphStatus::setTagNotFound("testTag");
    ASSERT_EQ("Tag `testTag' not found", graphStatus.toString());

    graphStatus = GraphStatus::setEdgeNotFound("testEdge");
    ASSERT_EQ("Edge `testEdge' not found", graphStatus.toString());

    graphStatus = GraphStatus::setIndexNotFound("testIndex");
    ASSERT_EQ("Index `testIndex' not found", graphStatus.toString());

    graphStatus = GraphStatus::setUserNotFound("testUser");
    ASSERT_EQ("User `testUser' not found", graphStatus.toString());

    graphStatus = GraphStatus::setConfigNotFound("testConfig");
    ASSERT_EQ("Config `testConfig' not found", graphStatus.toString());

    graphStatus = GraphStatus::setColumnNotFound("testColumn");
    ASSERT_EQ("Column name `testColumn' not found", graphStatus.toString());

    graphStatus = GraphStatus::setSpaceExisted("testSpace");
    ASSERT_EQ("Space `testSpace' existed", graphStatus.toString());

    graphStatus = GraphStatus::setTagExisted("testTag");
    ASSERT_EQ("Tag `testTag' existed", graphStatus.toString());
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

}   // namespace graph
}   // namespace nebula
