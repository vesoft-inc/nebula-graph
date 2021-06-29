/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <memory>
#include <unordered_set>

#include "visitor/DeduceVertexEdgePropsVisitor.h"
#include "visitor/test/VisitorTestBase.h"

namespace nebula {
namespace graph {

class DeduceVertexEdgePropsVisitorTest : public ValidatorTestBase {};

TEST_F(DeduceVertexEdgePropsVisitorTest, Basic) {
    std::unordered_map<std::string, AliasType> aliases{{"v", AliasType::kNode},
                                                       {"e", AliasType::kEdge}};
    {
        // v.prop
        auto expr = laExpr("v", "prop");
        VertexEdgeProps props;
        DeduceVertexEdgePropsVisitor visitor(props, aliases);
        expr->accept(&visitor);
        EXPECT_EQ(
            props,
            VertexEdgeProps(
                {}, VertexEdgeProps::AliasProps{{"v", std::set<std::string>{"prop"}}}, {}, {}));
    }
    {
        // (v.prop + 3) > e.prop1
        auto expr = gtExpr(addExpr(laExpr("v", "prop"), constantExpr(3)), laExpr("e", "prop1"));
        VertexEdgeProps props;
        DeduceVertexEdgePropsVisitor visitor(props, aliases);
        expr->accept(&visitor);
        EXPECT_EQ(props,
                  VertexEdgeProps(
                      {},
                      VertexEdgeProps::AliasProps{{"v", std::set<std::string>{"prop"}}},
                      VertexEdgeProps::AliasProps{{"e", std::set<std::string>{"prop1"}}},
                      {}));
    }
    {
        // id(v) > v.prop
        auto expr = gtExpr(fnExpr("id", {labelExpr("v")}), laExpr("v", "prop"));
        VertexEdgeProps props;
        DeduceVertexEdgePropsVisitor visitor(props, aliases);
        expr->accept(&visitor);
        EXPECT_EQ(
            props,
            VertexEdgeProps(
                {},
                VertexEdgeProps::AliasProps{{"v", std::set<std::string>{"prop"}}}, {},
                {}));
    }
    {
        // v + e + v.prop
        auto expr = addExpr(addExpr(labelExpr("v"), labelExpr("e")), laExpr("v", "prop"));
        VertexEdgeProps props;
        DeduceVertexEdgePropsVisitor visitor(props, aliases);
        expr->accept(&visitor);
        EXPECT_EQ(props,
                  VertexEdgeProps({},
                                  VertexEdgeProps::AliasProps{{"v", VertexEdgeProps::AllProps()}},
                                  VertexEdgeProps::AliasProps{{"e", VertexEdgeProps::AllProps()}},
                                  {}));
    }
}

}   // namespace graph
}   // namespace nebula
