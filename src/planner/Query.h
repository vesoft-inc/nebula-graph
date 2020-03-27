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
                          ExecutionPlan* plan) {
        auto* node = new GetNeighbors(input);
        plan->addPlanNode(node);
        return node;
    }

    std::string explain() const override;

private:
    explicit GetNeighbors(PlanNode* input) {
        kind_ = PlanNode::Kind::kGetNeighbors;
        input_ = input;
    }


private:
    // vertices are parsing from query.
    std::vector<VertexID>                        vertices_;
    // vertices may be parsing from runtime.
    std::unique_ptr<Expression>                  src_;
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
                          ExecutionPlan* plan) {
        auto* node = new GetVertices(input);
        plan->addPlanNode(node);
        return node;
    }

    std::string explain() const override;

private:
    explicit GetVertices(PlanNode* input) {
        kind_ = PlanNode::Kind::kGetVertices;
        input_ = input;
    }

private:
    // vertices are parsing from query.
    std::vector<VertexID>                    vertices_;
    // vertices may be parsing from runtime.
    std::unique_ptr<Expression>              src_;
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
                          ExecutionPlan* plan) {
        auto* node = new GetEdges(input);
        plan->addPlanNode(node);
        return node;
    }

    std::string explain() const override;

private:
    explicit GetEdges(PlanNode* input) {
        kind_ = PlanNode::Kind::kGetEdges;
        input_ = input;
    }

private:
    // edges_ are parsing from the query.
    std::vector<storage::cpp2::EdgeKey>      edges_;
    // edges_ may be parsed from runtime.
    std::unique_ptr<Expression>              src_;
    std::unique_ptr<Expression>              ranking_;
    std::unique_ptr<Expression>              dst_;
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
                        ExecutionPlan* plan) {
        auto* filter = new Filter(input);
        plan->addPlanNode(filter);
        return filter;
    }

    const Expression* condition() const {
        return condition_;
    }

    std::string explain() const override;

private:
    explicit Filter(PlanNode* input) {
        kind_ = PlanNode::Kind::kFilter;
        input_ = std::move(input);
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
                       ExecutionPlan* plan) {
        auto* project = new Project(input);
        plan->addPlanNode(project);
        return project;
    }

    std::string explain() const override;

private:
    explicit Project(PlanNode* input) {
        kind_ = PlanNode::Kind::kProject;
        input = std::move(input);
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
                      ExecutionPlan* plan) {
        auto* sort = new Sort(input);
        plan->addPlanNode(sort);
        return sort;
    }

    const OrderFactors* factors() {
        return factors_;
    }

    std::string explain() const override;

private:
    explicit Sort(PlanNode* input) {
        kind_ = PlanNode::Kind::kSort;
        input_ = input;
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
                          ExecutionPlan* plan) {
        auto* limit = new Limit(input);
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
    explicit Limit(PlanNode* input) {
        kind_ = PlanNode::Kind::kLimit;
        input_ = input;
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
                          ExecutionPlan* plan) {
        auto* agg = new Aggregate(input);
        plan->addPlanNode(agg);
        return agg;
    }

    const YieldColumns* groups() const {
        return groupCols_;
    }

    std::string explain() const override;

private:
    explicit Aggregate(PlanNode* input) {
        kind_ = PlanNode::Kind::kAggregate;
        input_ = input;
    }

private:
    YieldColumns*   groupCols_;
};

class BinarySelect : public SingleInputNode {
private:
    Expression*  condition_{nullptr};
};

class Selector : public BinarySelect {
public:
    static Selector* make(PlanNode* input,
                          ExecutionPlan* plan) {
        auto* selector = new Selector(input);
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
    explicit Selector(PlanNode* input) {
        kind_ = PlanNode::Kind::kSelector;
        input_ = input;
    }

private:
    PlanNode*   if_{nullptr};
    PlanNode*   else_{nullptr};
};

class Loop : public BinarySelect {
public:
    static Loop* make(PlanNode* input,
                          ExecutionPlan* plan) {
        auto* loop = new Loop(input);
        plan->addPlanNode(loop);
        return loop;
    }

    void setBody(PlanNode* body) {
        body_ = body;
    }

    std::string explain() const override;

private:
    explicit Loop(PlanNode* input) {
        kind_ = PlanNode::Kind::kLoop;
        input_ = input;
    }

private:
    PlanNode*   body_{nullptr};
};

class RegisterSpaceToSession : public SingleInputNode {
public:
    static RegisterSpaceToSession* make(PlanNode* input,
                                        ExecutionPlan* plan) {
        auto* regSpace = new RegisterSpaceToSession(input);
        plan->addPlanNode(regSpace);
        return regSpace;
    }

    void setSpace(GraphSpaceID space) {
        space_ = space;
    }

    std::string explain() const override;

private:
    explicit RegisterSpaceToSession(PlanNode* input) {
        kind_ = PlanNode::Kind::kRegisterSpaceToSession;
        input_ = input;
    }

private:
    GraphSpaceID    space_{-1};
};

class Dedup : public SingleInputNode {
public:
    static Dedup* make(PlanNode* input,
                       ExecutionPlan* plan) {
        auto* dedup = new Dedup(input);
        plan->addPlanNode(dedup);
        return dedup;
    }

    void setExpr(Expression* expr) {
        expr_ = expr;
    }

    std::string explain() const override;

private:
    explicit Dedup(PlanNode* input) {
        kind_ = PlanNode::Kind::kDedup;
        input_ = input;
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
