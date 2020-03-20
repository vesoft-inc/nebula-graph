/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_QUERY_H_
#define PLANNER_QUERY_H_

#include "base/Base.h"
#include "PlanNode.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "interface/gen-cpp2/storage_types.h"

/**
 * All query-related nodes would be put in this file,
 * and they are derived from PlanNode.
 */
namespace nebula {
namespace graph {
/**
 * Now we hava four kind of exploration nodes:
 *  GetNeighbors,
 *  GetVertices,
 *  GetEdges,
 *  ReadIndex
 */
class Explore : public PlanNode {
public:
    explicit Explore(GraphSpaceID space) {
        space_ = space;
    }

    GraphSpaceID space() const {
        return space_;
    }

private:
    GraphSpaceID        space_;
};

/**
 * Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    explicit GetNeighbors(GraphSpaceID space) : Explore(space) {
        kind_ = PlanNode::Kind::kGetNeighbors;
    }

    std::string explain() const override;

private:
    std::vector<VertexID>                        vertices_;
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
    explicit GetVertices(GraphSpaceID space) : Explore(space) {
        kind_ = PlanNode::Kind::kGetVertices;
    }

    std::string explain() const override;

private:
    std::vector<VertexID>                    vertices_;
    std::vector<storage::cpp2::VertexProp>   props_;
    std::string                              filter_;
};

/**
 * Get property with given edge keys.
 */
class GetEdges final : public Explore {
public:
    explicit GetEdges(GraphSpaceID space) : Explore(space) {
        kind_ = PlanNode::Kind::kGetEdges;
    }

    std::string explain() const override;

private:
    std::vector<storage::cpp2::EdgeKey>      edges_;
    std::vector<storage::cpp2::EdgeProp>     props_;
    std::string                              filter_;
};

/**
 * Read data through the index.
 */
class ReadIndex final : public Explore {
public:
    explicit ReadIndex(GraphSpaceID space) : Explore(space) {
        kind_ = PlanNode::Kind::kReadIndex;
    }

    std::string explain() const override;
};

/**
 * A Filter node helps filt some records with condition.
 */
class Filter final : public PlanNode {
public:
    explicit Filter(Expression* condition) {
        kind_ = PlanNode::Kind::kFilter;
        condition_ = condition;
    }

    Filter(std::shared_ptr<PlanNode>&& input,
           Expression* condition) {
        kind_ = PlanNode::Kind::kFilter;
        input_ = std::move(input);
        condition_ = condition;
    }

    void setInput(std::shared_ptr<PlanNode>&& input) {
        input_ = std::move(input);
    }

    const Expression* condition() const {
        return condition_;
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   input_;
    Expression*                 condition_;
};

/**
 * Now we have three kind of set operations:
 *   UNION,
 *   INTERSECT,
 *   MINUS
 */
class SetOp : public PlanNode {
public:
    SetOp() = default;

    SetOp(std::shared_ptr<PlanNode>&& left,
          std::shared_ptr<PlanNode>&& right) {
        left_ = std::move(left);
        right_ = std::move(right);
    }

    void setLeft(std::shared_ptr<PlanNode>&& left) {
        left_ = std::move(left);
    }

    void setRight(std::shared_ptr<PlanNode>&& right) {
        right_ = std::move(right);
    }

protected:
    std::shared_ptr<PlanNode>   left_;
    std::shared_ptr<PlanNode>   right_;
};

/**
 * Combine two set of records.
 */
class Union final : public SetOp {
public:
    explicit Union(bool distinct) {
        kind_ = PlanNode::Kind::kUnion;
        distinct_ = distinct;
    }

    Union(std::shared_ptr<PlanNode>&& left,
          std::shared_ptr<PlanNode>&& right) : SetOp(std::move(left), std::move(right)) {
    }

    std::string explain() const override;

private:
    bool    distinct_;
};

/**
 * Return the intersected records between two sets.
 */
class Intersect final : public SetOp {
public:
    Intersect() {
        kind_ = PlanNode::Kind::kIntersect;
    }

    Intersect(std::shared_ptr<PlanNode>&& left,
              std::shared_ptr<PlanNode>&& right) : SetOp(std::move(left), std::move(right)) {
    }

    std::string explain() const override;
};

/**
 * Do subtraction between two sets.
 */
class Minus final : public SetOp {
public:
    Minus() {
        kind_ = PlanNode::Kind::kMinus;
    }

    Minus(std::shared_ptr<PlanNode>&& left,
          std::shared_ptr<PlanNode>&& right) : SetOp(std::move(left), std::move(right)) {
    }

    std::string explain() const override;
};

/**
 * Project is used to specify output vars or field.
 */
class Project final : public PlanNode {
public:
    Project(YieldColumns* cols, bool distinct) {
        kind_ = PlanNode::Kind::kProject;
        cols_ = cols;
        distinct_ = distinct;
    }

    Project(std::shared_ptr<PlanNode>&& input, YieldColumns* cols) {
        kind_ = PlanNode::Kind::kProject;
        input = std::move(input);
        cols_ = cols;
        distinct_ = distinct;
    }

    void setInput(std::shared_ptr<PlanNode>&& input) {
        input = std::move(input);
    }

    bool distinct() const {
        return distinct_;
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   input_;
    YieldColumns*               cols_;
};

/**
 * Sort the given record set.
 */
class Sort final : public PlanNode {
public:
    explicit Sort(OrderFactors* factors) {
        kind_ = PlanNode::Kind::kSort;
        factors_ = factors;
    }

    Sort(std::shared_ptr<PlanNode>&& input, OrderFactors* factors) {
        kind_ = PlanNode::Kind::kSort;
        input_ = std::move(input);
        factors_ = factors;
    }

    void setInput(std::shared_ptr<PlanNode>&& input) {
        input_ = std::move(input);
    }

    const OrderFactors* factors() {
        return factors_;
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   input_;
    OrderFactors*   factors_;
};

/**
 * Output the records with the given limitation.
 */
class Limit final : public PlanNode {
public:
    Limit(int64_t offset, int64_t count) {
        kind_ = PlanNode::Kind::kLimit;
        offset_ = offset;
        count_ = count;
    }

    Limit(std::shared_ptr<PlanNode> input, int64_t offset, int64_t count) {
        kind_ = PlanNode::Kind::kLimit;
        input_ = std::move(input);
        offset_ = offset;
        count_ = count;
    }

    void setInput(std::shared_ptr<PlanNode> input) {
        input_ = std::move(input);
    }

    int64_t offset() const {
        return offset_;
    }

    int64_t count() const {
        return count_;
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   input_;
    int64_t     offset_{-1};
    int64_t     count_{-1};
};

/**
 * Do Aggregation with the given set of records,
 * such as AVG(), COUNT()...
 */
class Aggregate : public PlanNode {
public:
    Aggregate(YieldColumns* yieldCols,
              YieldColumns* groupCols) {
        kind_ = PlanNode::Kind::kAggregate;
        yieldCols_ = yieldCols;
        groupCols_ = groupCols;
    }

    explicit Aggregate(std::shared_ptr<PlanNode>&& input,
                       YieldColumns* groupCols) {
        kind_ = PlanNode::Kind::kAggregate;
        input_ = std::move(input);
        groupCols_ = groupCols;
    }

    void setInput(std::shared_ptr<PlanNode>&& input) {
        input_ = std::move(input);
    }

    const YieldColumns* groups() const {
        return groupCols_;
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   input_;
    YieldColumns*               groupCols_;
};

class BinarySelect : public PlanNode {
public:
    explicit BinarySelect(Expression* condition) {
        condition_ = condition;
    }

private:
    Expression*  condition_;
};

class Selector : public BinarySelect {
public:
    explicit Selector(Expression* condition)
        : BinarySelect(condition) {
        kind_ = PlanNode::Kind::kSelector;
    }

    Selector(std::shared_ptr<PlanNode>&& input,
             Expression* condition)
        : BinarySelect(condition) {
        input_ = std::move(input);
        kind_ = PlanNode::Kind::kSelector;
    }

    void setInput(std::shared_ptr<PlanNode>&& input) {
        input_ = std::move(input);
    }

    void setIf(std::shared_ptr<PlanNode>&& ifBranch) {
        if_ = std::move(ifBranch);
    }

    void setElse(std::shared_ptr<PlanNode>&& elseBranch) {
        else_ = std::move(elseBranch);
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   if_;
    std::shared_ptr<PlanNode>   else_;
    std::shared_ptr<PlanNode>   input_;
};

class Loop : public BinarySelect {
public:
    explicit Loop(Expression* condition)
        : BinarySelect(condition) {
        kind_ = PlanNode::Kind::kLoop;
    }

    Loop(std::shared_ptr<PlanNode>&& input, Expression* condition)
        : BinarySelect(condition) {
        input_ = std::move(input);
        kind_ = PlanNode::Kind::kLoop;
    }

    void setInput(std::shared_ptr<PlanNode>&& input) {
        input_ = std::move(input);
    }

    void setBody(std::shared_ptr<PlanNode>&& body) {
        body_ = std::move(body);
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   body_;
    std::shared_ptr<PlanNode>   input_;
};

class RegisterSpaceToSession : public PlanNode {
public:
    explicit RegisterSpaceToSession(GraphSpaceID space) {
        kind_ = PlanNode::Kind::kRegisterSpaceToSession;
        space_ = space;
    }

    RegisterSpaceToSession(std::shared_ptr<PlanNode>&& input,
            GraphSpaceID space) {
        kind_ = PlanNode::Kind::kRegisterSpaceToSession;
        input_ = std::move(input);
        space_ = space;
    }

    void setInput(std::shared_ptr<PlanNode>&& input) {
        input_ = std::move(input);
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   input_;
    GraphSpaceID                space_;
};

class Dedup : public PlanNode {
public:
    Dedup(std::shared_ptr<PlanNode>&& input, std::string&& var) {
        kind_ = PlanNode::Kind::kDedup;
        input_ = std::move(input);
        var_ = std::move(var);
    }

    std::string explain() const override;

private:
    std::shared_ptr<PlanNode>   input_;
    std::string                 var_;
};

class ProduceSemiShortestPath : public PlanNode {
};

class ConjunctPath : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_QUERY_H_
