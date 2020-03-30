/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_QUERY_H_
#define PLANNER_QUERY_H_


#include "base/Base.h"
#include "planner/PlanNode.h"
#include "planner/ExecutionPlan.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "interface/gen-cpp2/storage_types.h"

/**
 * All query-related nodes would be put in this file,
 * and they are derived from PlanNode.
 */
namespace nebula {
namespace graph {

class StartNode final : public PlanNode {
public:
    static StartNode* make(ExecutionPlan* plan) {
        auto* node = new StartNode();
        plan->addPlanNode(node);
        return node;
    }

    std::string explain() const override {
        return "Start";
    }

private:
    StartNode() {
        kind_ = PlanNode::Kind::kStart;
    }
};

class SingleInputNode : public PlanNode {
public:
    PlanNode* input() const {
        return input_;
    }

    void setInput(PlanNode* input) {
        input_ = input;
    }

    std::string explain() const override {
        return "";
    }

protected:
    PlanNode* input_{nullptr};
};

class BiInputNode : public PlanNode {
public:
    void setLeft(PlanNode* left) {
        left_ = left;
    }

    void setRight(PlanNode* right) {
        right_ = right;
    }

    PlanNode* left() const {
        return left_;
    }

    PlanNode* right() const {
        return right_;
    }

    std::string explain() const override {
        return "";
    }

protected:
    PlanNode* left_{nullptr};
    PlanNode* right_{nullptr};
};

/**
 * This operator is used for multi output situation.
 */
class MultiOutputsNode final : public SingleInputNode {
public:
    static MultiOutputsNode* make(PlanNode* input, ExecutionPlan* plan) {
        auto* end = new MultiOutputsNode(input);
        plan->addPlanNode(end);
        return end;
    }

    std::string explain() const override {
        return "End";
    }

private:
    explicit MultiOutputsNode(PlanNode* input) {
        kind_ = PlanNode::Kind::kEnd;
        input_ = input;
    }
};

/**
 * Now we hava four kind of exploration nodes:
 *  GetNeighbors,
 *  GetVertices,
 *  GetEdges,
 *  ReadIndex
 */
class Explore : public SingleInputNode {
public:
    GraphSpaceID space() const {
        return space_;
    }

protected:
    GraphSpaceID        space_;
};

/**
 * Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    static GetNeighbors* make(PlanNode* input,
                              GraphSpaceID space,
                              std::vector<VertexID> vertices,
                              Expression* src,
                              std::vector<EdgeType> edgeTypes,
                              std::vector<storage::cpp2::VertexProp> vertexProps,
                              std::vector<storage::cpp2::EdgeProp> edgeProps,
                              std::vector<storage::cpp2::StatProp> statProps,
                              std::string filter,
                              ExecutionPlan* plan) {
        auto* node = new GetNeighbors(
                input,
                space,
                std::move(vertices),
                src,
                std::move(edgeTypes),
                std::move(vertexProps),
                std::move(edgeProps),
                std::move(statProps),
                std::move(filter));
        plan->addPlanNode(node);
        return node;
    }

    std::string explain() const override;

private:
    GetNeighbors(PlanNode* input,
                 GraphSpaceID space,
                 std::vector<VertexID> vertices,
                 Expression* src,
                 std::vector<EdgeType> edgeTypes,
                 std::vector<storage::cpp2::VertexProp> vertexProps,
                 std::vector<storage::cpp2::EdgeProp> edgeProps,
                 std::vector<storage::cpp2::StatProp> statProps,
                 std::string filter) {
        kind_ = PlanNode::Kind::kGetNeighbors;
        input_ = input;
        space_ = space;
        vertices_ = std::move(vertices);
        src_ = src;
        edgeTypes_ = std::move(edgeTypes);
        vertexProps_ = std::move(vertexProps);
        edgeProps_ = std::move(edgeProps);
        statProps_ = std::move(statProps);
        filter_ = std::move(filter);
    }

private:
    // vertices are parsing from query.
    std::vector<VertexID>                        vertices_;
    // vertices may be parsing from runtime.
    Expression*                                  src_;
    std::vector<EdgeType>                        edgeTypes_;
    std::vector<storage::cpp2::VertexProp>       vertexProps_;
    std::vector<storage::cpp2::EdgeProp>         edgeProps_;
    std::vector<storage::cpp2::StatProp>         statProps_;
    std::string                                  filter_;
};

/**
 * Get property with given vertex keys.
 */
class GetVertices final : public Explore {
public:
    static GetVertices* make(PlanNode* input,
                             GraphSpaceID space,
                             std::vector<VertexID> vertices,
                             Expression* src,
                             std::vector<storage::cpp2::VertexProp> props,
                             std::string filter,
                             ExecutionPlan* plan) {
        auto* node = new GetVertices(
                input,
                space,
                std::move(vertices),
                src,
                std::move(props),
                std::move(filter));
        plan->addPlanNode(node);
        return node;
    }

    std::string explain() const override;

private:
    GetVertices(PlanNode* input,
                GraphSpaceID space,
                std::vector<VertexID> vertices,
                Expression* src,
                std::vector<storage::cpp2::VertexProp> props,
                std::string filter) {
        kind_ = PlanNode::Kind::kGetVertices;
        input_ = input;
        space_ = space;
        vertices_ = std::move(vertices);
        src_ = src;
        props_ = std::move(props);
        filter = std::move(filter);
    }

private:
    // vertices are parsing from query.
    std::vector<VertexID>                    vertices_;
    // vertices may be parsing from runtime.
    Expression*                              src_;
    // props and filter are parsing from query.
    std::vector<storage::cpp2::VertexProp>   props_;
    std::string                              filter_;
};

/**
 * Get property with given edge keys.
 */
class GetEdges final : public Explore {
public:
    static GetEdges* make(PlanNode* input,
                          GraphSpaceID space,
                          std::vector<storage::cpp2::EdgeKey> edges,
                          Expression* src,
                          Expression* ranking,
                          Expression* dst,
                          std::vector<storage::cpp2::EdgeProp> props,
                          std::string filter,
                          ExecutionPlan* plan) {
        auto* node = new GetEdges(
                input,
                space,
                std::move(edges),
                src,
                ranking,
                dst,
                std::move(props),
                std::move(filter));
        plan->addPlanNode(node);
        return node;
    }

    std::string explain() const override;

private:
    GetEdges(PlanNode* input,
             GraphSpaceID space,
             std::vector<storage::cpp2::EdgeKey> edges,
             Expression* src,
             Expression* ranking,
             Expression* dst,
             std::vector<storage::cpp2::EdgeProp> props,
             std::string filter) {
        kind_ = PlanNode::Kind::kGetEdges;
        input_ = input;
        space_ = space;
        edges_ = std::move(edges);
        src_ = std::move(src);
        ranking_ = std::move(ranking);
        dst_ = std::move(dst);
        props_ = std::move(props);
        filter_ = std::move(filter);
    }

private:
    // edges_ are parsing from the query.
    std::vector<storage::cpp2::EdgeKey>      edges_;
    // edges_ may be parsed from runtime.
    Expression*                              src_;
    Expression*                              ranking_;
    Expression*                              dst_;
    // props and filter are parsing from query.
    std::vector<storage::cpp2::EdgeProp>     props_;
    std::string                              filter_;
};

/**
 * Read data through the index.
 */
class ReadIndex final : public Explore {
public:
    explicit ReadIndex(GraphSpaceID space) {
        kind_ = PlanNode::Kind::kReadIndex;
        space_ = space;
    }

    std::string explain() const override;
};

/**
 * A Filter node helps filt some records with condition.
 */
class Filter final : public SingleInputNode {
public:
    static Filter* make(PlanNode* input,
                        Expression* condition,
                        ExecutionPlan* plan) {
        auto* filter = new Filter(input, condition);
        plan->addPlanNode(filter);
        return filter;
    }

    const Expression* condition() const {
        return condition_;
    }

    std::string explain() const override;

private:
    Filter(PlanNode* input, Expression* condition) {
        kind_ = PlanNode::Kind::kFilter;
        input_ = input;
        condition_ = condition;
    }

private:
    Expression*                 condition_;
};

/**
 * Now we have three kind of set operations:
 *   UNION,
 *   INTERSECT,
 *   MINUS
 */
class SetOp : public BiInputNode {
public:
    SetOp() = default;

    SetOp(PlanNode* left, PlanNode* right) {
        left_ = left;
        right_ = right;
    }
};

/**
 * Combine two set of records.
 */
class Union final : public SetOp {
public:
    static Union* make(PlanNode* left, PlanNode* right, ExecutionPlan* plan) {
        auto* unionNode = new Union(left, right);
        plan->addPlanNode(unionNode);
        return unionNode;
    }

    std::string explain() const override;

private:
    Union(PlanNode* left, PlanNode* right) : SetOp(left, right) {
        kind_ = PlanNode::Kind::kUnion;
    }
};

/**
 * Return the intersected records between two sets.
 */
class Intersect final : public SetOp {
public:
    static Intersect* make(PlanNode* left, PlanNode* right, ExecutionPlan* plan) {
        auto* intersect = new Intersect(left, right);
        plan->addPlanNode(intersect);
        return intersect;
    }

    std::string explain() const override;

private:
    Intersect(PlanNode* left, PlanNode* right) : SetOp(left, right) {
        kind_ = PlanNode::Kind::kIntersect;
    }
};

/**
 * Do subtraction between two sets.
 */
class Minus final : public SetOp {
public:
    static Minus* make(PlanNode* left, PlanNode* right, ExecutionPlan* plan) {
        auto* minus = new Minus(left, right);
        plan->addPlanNode(minus);
        return minus;
    }

    std::string explain() const override;

private:
    Minus(PlanNode* left, PlanNode* right) : SetOp(left, right) {
        kind_ = PlanNode::Kind::kMinus;
    }
};

/**
 * Project is used to specify output vars or field.
 */
class Project final : public SingleInputNode {
public:
    static Project* make(PlanNode* input,
                         YieldColumns* cols,
                         ExecutionPlan* plan) {
        auto* project = new Project(input, cols);
        plan->addPlanNode(project);
        return project;
    }

    std::string explain() const override;

private:
    Project(PlanNode* input, YieldColumns* cols) {
        kind_ = PlanNode::Kind::kProject;
        input = input;
        cols_ = cols;
    }

private:
    YieldColumns*               cols_{nullptr};
};

/**
 * Sort the given record set.
 */
class Sort final : public SingleInputNode {
public:
    static Sort* make(PlanNode* input,
                      OrderFactors* factors,
                      ExecutionPlan* plan) {
        auto* sort = new Sort(input, factors);
        plan->addPlanNode(sort);
        return sort;
    }

    const OrderFactors* factors() {
        return factors_;
    }

    std::string explain() const override;

private:
    Sort(PlanNode* input, OrderFactors* factors) {
        kind_ = PlanNode::Kind::kSort;
        input_ = input;
        factors_ = factors;
    }

private:
    OrderFactors*   factors_{nullptr};
};

/**
 * Output the records with the given limitation.
 */
class Limit final : public SingleInputNode {
public:
    static Limit* make(PlanNode* input,
                       int64_t offset,
                       int64_t count,
                       ExecutionPlan* plan) {
        auto* limit = new Limit(input, offset, count);
        plan->addPlanNode(limit);
        return limit;
    }

    int64_t offset() const {
        return offset_;
    }

    int64_t count() const {
        return count_;
    }

    std::string explain() const override;

private:
    Limit(PlanNode* input, int64_t offset, int64_t count) {
        kind_ = PlanNode::Kind::kLimit;
        input_ = input;
        offset_ = offset;
        count_ = count;
    }

private:
    int64_t     offset_{-1};
    int64_t     count_{-1};
};

/**
 * Do Aggregation with the given set of records,
 * such as AVG(), COUNT()...
 */
class Aggregate : public SingleInputNode {
public:
    static Aggregate* make(PlanNode* input,
                           YieldColumns* groupCols,
                           ExecutionPlan* plan) {
        auto* agg = new Aggregate(input, groupCols);
        plan->addPlanNode(agg);
        return agg;
    }

    const YieldColumns* groups() const {
        return groupCols_;
    }

    std::string explain() const override;

private:
    Aggregate(PlanNode* input,
              YieldColumns* groupCols) {
        kind_ = PlanNode::Kind::kAggregate;
        input_ = input;
        groupCols_ = groupCols;
    }

private:
    YieldColumns*   groupCols_;
};

class BinarySelect : public SingleInputNode {
protected:
    Expression*  condition_{nullptr};
};

class Selector : public BinarySelect {
public:
    static Selector* make(PlanNode* input,
                          PlanNode* ifBranch,
                          PlanNode* elseBranch,
                          Expression* condition,
                          ExecutionPlan* plan) {
        auto* selector = new Selector(input, ifBranch, elseBranch, condition);
        plan->addPlanNode(selector);
        return selector;
    }

    void setIf(PlanNode* ifBranch) {
        if_ = ifBranch;
    }

    void setElse(PlanNode* elseBranch) {
        else_ = elseBranch;
    }

    std::string explain() const override;

private:
    Selector(PlanNode* input,
             PlanNode* ifBranch,
             PlanNode* elseBranch,
             Expression* condition) {
        kind_ = PlanNode::Kind::kSelector;
        input_ = input;
        if_ = ifBranch;
        else_ = elseBranch;
        condition_ = condition;
    }

private:
    PlanNode*   if_{nullptr};
    PlanNode*   else_{nullptr};
};

class Loop : public BinarySelect {
public:
    static Loop* make(PlanNode* input,
                      PlanNode* body,
                      Expression* condition,
                      ExecutionPlan* plan) {
        auto* loop = new Loop(input, body, condition);
        plan->addPlanNode(loop);
        return loop;
    }

    void setBody(PlanNode* body) {
        body_ = body;
    }

    std::string explain() const override;

private:
    Loop(PlanNode* input, PlanNode* body, Expression* condition) {
        kind_ = PlanNode::Kind::kLoop;
        input_ = input;
        body_ = body;
        condition_ = condition;
    }

private:
    PlanNode*   body_{nullptr};
};

class RegisterSpaceToSession : public SingleInputNode {
public:
    static RegisterSpaceToSession* make(PlanNode* input,
                                        GraphSpaceID space,
                                        ExecutionPlan* plan) {
        auto* regSpace = new RegisterSpaceToSession(input, space);
        plan->addPlanNode(regSpace);
        return regSpace;
    }

    void setSpace(GraphSpaceID space) {
        space_ = space;
    }

    std::string explain() const override;

private:
    RegisterSpaceToSession(PlanNode* input, GraphSpaceID space) {
        kind_ = PlanNode::Kind::kRegisterSpaceToSession;
        input_ = input;
        space_ = space;
    }

private:
    GraphSpaceID    space_{-1};
};

class Dedup : public SingleInputNode {
public:
    static Dedup* make(PlanNode* input,
                       Expression* expr,
                       ExecutionPlan* plan) {
        auto* dedup = new Dedup(input, expr);
        plan->addPlanNode(dedup);
        return dedup;
    }

    void setExpr(Expression* expr) {
        expr_ = expr;
    }

    std::string explain() const override;

private:
    Dedup(PlanNode* input, Expression* expr) {
        kind_ = PlanNode::Kind::kDedup;
        input_ = input;
        expr_ = expr;
    }

private:
    Expression*     expr_;
};

class ProduceSemiShortestPath : public PlanNode {
};

class ConjunctPath : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_QUERY_H_
