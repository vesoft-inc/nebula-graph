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
        return new StartNode(plan);
    }

    std::string explain() const override {
        return "Start";
    }

private:
    explicit StartNode(ExecutionPlan* plan) : PlanNode(plan) {
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
    explicit SingleInputNode(ExecutionPlan* plan) : PlanNode(plan) {}

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
    explicit BiInputNode(ExecutionPlan* plan) : PlanNode(plan) {}

protected:
    PlanNode* left_{nullptr};
    PlanNode* right_{nullptr};
};

/**
 * This operator is used for multi output situation.
 */
class MultiOutputsNode final : public SingleInputNode {
public:
    static MultiOutputsNode* make(ExecutionPlan* plan, PlanNode* input) {
        return new MultiOutputsNode(input, plan);
    }

    std::string explain() const override {
        return "End";
    }

private:
    explicit MultiOutputsNode(PlanNode* input, ExecutionPlan* plan) : SingleInputNode(plan) {
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
    explicit Explore(ExecutionPlan* plan) : SingleInputNode(plan) {}

protected:
    GraphSpaceID        space_;
};

/**
 * Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    static GetNeighbors* make(ExecutionPlan* plan,
                              PlanNode* input,
                              GraphSpaceID space,
                              std::vector<VertexID> vertices,
                              Expression* src,
                              std::vector<EdgeType> edgeTypes,
                              std::vector<storage::cpp2::VertexProp> vertexProps,
                              std::vector<storage::cpp2::EdgeProp> edgeProps,
                              std::vector<storage::cpp2::StatProp> statProps,
                              std::string filter) {
        return new GetNeighbors(
                plan,
                input,
                space,
                std::move(vertices),
                src,
                std::move(edgeTypes),
                std::move(vertexProps),
                std::move(edgeProps),
                std::move(statProps),
                std::move(filter));
    }

    std::string explain() const override;

private:
    GetNeighbors(ExecutionPlan* plan,
                 PlanNode* input,
                 GraphSpaceID space,
                 std::vector<VertexID> vertices,
                 Expression* src,
                 std::vector<EdgeType> edgeTypes,
                 std::vector<storage::cpp2::VertexProp> vertexProps,
                 std::vector<storage::cpp2::EdgeProp> edgeProps,
                 std::vector<storage::cpp2::StatProp> statProps,
                 std::string filter) : Explore(plan) {
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
    static GetVertices* make(ExecutionPlan* plan,
                             PlanNode* input,
                             GraphSpaceID space,
                             std::vector<VertexID> vertices,
                             Expression* src,
                             std::vector<storage::cpp2::VertexProp> props,
                             std::string filter) {
        return new GetVertices(
                plan,
                input,
                space,
                std::move(vertices),
                src,
                std::move(props),
                std::move(filter));
    }

    std::string explain() const override;

private:
    GetVertices(ExecutionPlan* plan,
                PlanNode* input,
                GraphSpaceID space,
                std::vector<VertexID> vertices,
                Expression* src,
                std::vector<storage::cpp2::VertexProp> props,
                std::string filter) : Explore(plan) {
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
    static GetEdges* make(ExecutionPlan* plan,
                          PlanNode* input,
                          GraphSpaceID space,
                          std::vector<storage::cpp2::EdgeKey> edges,
                          Expression* src,
                          Expression* ranking,
                          Expression* dst,
                          std::vector<storage::cpp2::EdgeProp> props,
                          std::string filter) {
        return new GetEdges(
                plan,
                input,
                space,
                std::move(edges),
                src,
                ranking,
                dst,
                std::move(props),
                std::move(filter));
    }

    std::string explain() const override;

private:
    GetEdges(ExecutionPlan* plan,
             PlanNode* input,
             GraphSpaceID space,
             std::vector<storage::cpp2::EdgeKey> edges,
             Expression* src,
             Expression* ranking,
             Expression* dst,
             std::vector<storage::cpp2::EdgeProp> props,
             std::string filter) : Explore(plan) {
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
    ReadIndex(ExecutionPlan* plan, GraphSpaceID space) : Explore(plan) {
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
    static Filter* make(ExecutionPlan* plan,
                        PlanNode* input,
                        Expression* condition) {
        return new Filter(plan, input, condition);
    }

    const Expression* condition() const {
        return condition_;
    }

    std::string explain() const override;

private:
    Filter(ExecutionPlan* plan, PlanNode* input, Expression* condition)
        : SingleInputNode(plan) {
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

    SetOp(ExecutionPlan* plan, PlanNode* left, PlanNode* right) : BiInputNode(plan) {
        left_ = left;
        right_ = right;
    }
};

/**
 * Combine two set of records.
 */
class Union final : public SetOp {
public:
    static Union* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new Union(plan, left, right);
    }

    std::string explain() const override;

private:
    Union(ExecutionPlan* plan, PlanNode* left, PlanNode* right) : SetOp(plan, left, right) {
        kind_ = PlanNode::Kind::kUnion;
    }
};

/**
 * Return the intersected records between two sets.
 */
class Intersect final : public SetOp {
public:
    static Intersect* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new Intersect(plan, left, right);
    }

    std::string explain() const override;

private:
    Intersect(ExecutionPlan* plan, PlanNode* left, PlanNode* right) : SetOp(plan, left, right) {
        kind_ = PlanNode::Kind::kIntersect;
    }
};

/**
 * Do subtraction between two sets.
 */
class Minus final : public SetOp {
public:
    static Minus* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new Minus(plan, left, right);
    }

    std::string explain() const override;

private:
    Minus(ExecutionPlan* plan, PlanNode* left, PlanNode* right) : SetOp(plan, left, right) {
        kind_ = PlanNode::Kind::kMinus;
    }
};

/**
 * Project is used to specify output vars or field.
 */
class Project final : public SingleInputNode {
public:
    static Project* make(ExecutionPlan* plan,
                         PlanNode* input,
                         YieldColumns* cols) {
        return new Project(plan, input, cols);
    }

    std::string explain() const override;

private:
    Project(ExecutionPlan* plan, PlanNode* input, YieldColumns* cols) : SingleInputNode(plan) {
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
    static Sort* make(ExecutionPlan* plan,
                      PlanNode* input,
                      OrderFactors* factors) {
        return new Sort(plan, input, factors);
    }

    const OrderFactors* factors() {
        return factors_;
    }

    std::string explain() const override;

private:
    Sort(ExecutionPlan* plan, PlanNode* input, OrderFactors* factors) : SingleInputNode(plan) {
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
    static Limit* make(ExecutionPlan* plan,
                       PlanNode* input,
                       int64_t offset,
                       int64_t count) {
        return new Limit(plan, input, offset, count);
    }

    int64_t offset() const {
        return offset_;
    }

    int64_t count() const {
        return count_;
    }

    std::string explain() const override;

private:
    Limit(ExecutionPlan* plan, PlanNode* input, int64_t offset, int64_t count)
        : SingleInputNode(plan) {
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
    static Aggregate* make(ExecutionPlan* plan,
                           PlanNode* input,
                           YieldColumns* groupCols) {
        return new Aggregate(plan, input, groupCols);
    }

    const YieldColumns* groups() const {
        return groupCols_;
    }

    std::string explain() const override;

private:
    Aggregate(ExecutionPlan* plan,
              PlanNode* input,
              YieldColumns* groupCols) : SingleInputNode(plan) {
        kind_ = PlanNode::Kind::kAggregate;
        input_ = input;
        groupCols_ = groupCols;
    }

private:
    YieldColumns*   groupCols_;
};

class BinarySelect : public SingleInputNode {
protected:
    explicit BinarySelect(ExecutionPlan* plan) : SingleInputNode(plan) {}

protected:
    Expression*  condition_{nullptr};
};

class Selector : public BinarySelect {
public:
    static Selector* make(ExecutionPlan* plan,
                          PlanNode* input,
                          PlanNode* ifBranch,
                          PlanNode* elseBranch,
                          Expression* condition) {
        return new Selector(plan, input, ifBranch, elseBranch, condition);
    }

    void setIf(PlanNode* ifBranch) {
        if_ = ifBranch;
    }

    void setElse(PlanNode* elseBranch) {
        else_ = elseBranch;
    }

    std::string explain() const override;

private:
    Selector(ExecutionPlan* plan,
             PlanNode* input,
             PlanNode* ifBranch,
             PlanNode* elseBranch,
             Expression* condition) : BinarySelect(plan) {
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
    static Loop* make(ExecutionPlan* plan,
                      PlanNode* input,
                      PlanNode* body,
                      Expression* condition) {
        return new Loop(plan, input, body, condition);
    }

    void setBody(PlanNode* body) {
        body_ = body;
    }

    std::string explain() const override;

private:
    Loop(ExecutionPlan* plan,
         PlanNode* input,
         PlanNode* body,
         Expression* condition) : BinarySelect(plan) {
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
    static RegisterSpaceToSession* make(ExecutionPlan* plan,
                                        PlanNode* input,
                                        GraphSpaceID space) {
        return new RegisterSpaceToSession(plan, input, space);
    }

    void setSpace(GraphSpaceID space) {
        space_ = space;
    }

    std::string explain() const override;

private:
    RegisterSpaceToSession(ExecutionPlan* plan,
                           PlanNode* input,
                           GraphSpaceID space) : SingleInputNode(plan) {
        kind_ = PlanNode::Kind::kRegisterSpaceToSession;
        input_ = input;
        space_ = space;
    }

private:
    GraphSpaceID    space_{-1};
};

class Dedup : public SingleInputNode {
public:
    static Dedup* make(ExecutionPlan* plan,
                       PlanNode* input,
                       Expression* expr) {
        return new Dedup(plan, input, expr);
    }

    void setExpr(Expression* expr) {
        expr_ = expr;
    }

    std::string explain() const override;

private:
    Dedup(ExecutionPlan* plan, PlanNode* input, Expression* expr)
        : SingleInputNode(plan) {
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
