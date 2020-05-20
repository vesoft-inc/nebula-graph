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
    explicit StartNode(ExecutionPlan* plan) : PlanNode(plan, Kind::kStart) {
    }
};


class EndNode final : public PlanNode {
public:
    static EndNode* make(ExecutionPlan* plan) {
        return new EndNode(plan);
    }

    std::string explain() const override {
        return "End";
    }

private:
    explicit EndNode(ExecutionPlan* plan) : PlanNode(plan, Kind::kEnd) {
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
    SingleInputNode(ExecutionPlan* plan, Kind kind, PlanNode* input)
        : PlanNode(plan, kind), input_(input) {
        // DCHECK_NOTNULL(input_);
    }

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
    BiInputNode(ExecutionPlan* plan, Kind kind, PlanNode* left, PlanNode* right)
        : PlanNode(plan, kind), left_(left), right_(right) {
        DCHECK_NOTNULL(left_);
        DCHECK_NOTNULL(right_);
    }

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
        return "MultiOutputsNode";
    }

private:
    MultiOutputsNode(PlanNode* input, ExecutionPlan* plan)
        : SingleInputNode(plan, Kind::kMultiOutputs, input) {}
};

/**
 * Now we hava four kind of exploration nodes:
 *  GetNeighborsNode,
 *  GetVerticesNode,
 *  GetEdgesNode,
 *  ReadIndexNode
 */
class Explore : public SingleInputNode {
public:
    GraphSpaceID space() const {
        return space_;
    }

    bool dedup() const {
        return dedup_;
    }

    int64_t limit() const {
        return limit_;
    }

    const std::string& filter() const {
        return filter_;
    }

    const std::vector<storage::cpp2::OrderBy>& orderBy() const {
        return orderBy_;
    }

protected:
    Explore(ExecutionPlan* plan,
            Kind kind,
            PlanNode* input,
            GraphSpaceID space,
            bool dedup,
            int64_t limit,
            std::string filter,
            std::vector<storage::cpp2::OrderBy> orderBy)
        : SingleInputNode(plan, kind, input),
          space_(space),
          dedup_(dedup),
          limit_(limit),
          filter_(std::move(filter)),
          orderBy_(std::move(orderBy)) {}

    Explore(ExecutionPlan* plan, Kind kind, PlanNode* input, GraphSpaceID space)
        : SingleInputNode(plan, kind, input), space_(space) {}

protected:
    GraphSpaceID        space_;
    bool dedup_{false};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::string filter_;
    std::vector<storage::cpp2::OrderBy> orderBy_;
};

/**
 * Get neighbors' property
 */
class GetNeighborsNode final : public Explore {
public:
    static GetNeighborsNode* make(ExecutionPlan* plan,
                              PlanNode* input,
                              GraphSpaceID space,
                              std::vector<Row> vertices,
                              Expression* src,
                              std::vector<EdgeType> edgeTypes,
                              storage::cpp2::EdgeDirection edgeDirection,
                              std::vector<storage::cpp2::PropExp> vertexProps,
                              std::vector<storage::cpp2::PropExp> edgeProps,
                              std::vector<storage::cpp2::StatProp> statProps,
                              bool dedup = false,
                              std::vector<storage::cpp2::OrderBy> orderBy = {},
                              int64_t limit = std::numeric_limits<int64_t>::max(),
                              std::string filter = "") {
        return new GetNeighborsNode(
                plan,
                input,
                space,
                std::move(vertices),
                src,
                std::move(edgeTypes),
                edgeDirection,
                std::move(vertexProps),
                std::move(edgeProps),
                std::move(statProps),
                dedup,
                std::move(orderBy),
                limit,
                std::move(filter));
    }

    std::string explain() const override;

    Expression* src() const {
        return src_;
    }

    const std::vector<Row>& vertices() const {
        return vertices_;
    }

    storage::cpp2::EdgeDirection edgeDirection() const {
        return edgeDirection_;
    }

    const std::vector<EdgeType>& edgeTypes() const {
        return edgeTypes_;
    }

    const std::vector<storage::cpp2::PropExp>& vertexProps() const {
        return vertexProps_;
    }

    const std::vector<storage::cpp2::PropExp>& edgeProps() const {
        return edgeProps_;
    }

    const std::vector<storage::cpp2::StatProp>& statProps() const {
        return statProps_;
    }

private:
    GetNeighborsNode(ExecutionPlan* plan,
                 PlanNode* input,
                 GraphSpaceID space,
                 std::vector<Row> vertices,
                 Expression* src,
                 std::vector<EdgeType> edgeTypes,
                 storage::cpp2::EdgeDirection edgeDirection,
                 std::vector<storage::cpp2::PropExp> vertexProps,
                 std::vector<storage::cpp2::PropExp> edgeProps,
                 std::vector<storage::cpp2::StatProp> statProps,
                 bool dedup,
                 std::vector<storage::cpp2::OrderBy> orderBy,
                 int64_t limit,
                 std::string filter)
        : Explore(plan,
                  Kind::kGetNeighbors,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)) {
        vertices_ = std::move(vertices);
        src_ = src;
        edgeTypes_ = std::move(edgeTypes);
        edgeDirection_ = edgeDirection;
        vertexProps_ = std::move(vertexProps);
        edgeProps_ = std::move(edgeProps);
        statProps_ = std::move(statProps);
    }

private:
    // vertices are parsing from query.
    std::vector<Row>                             vertices_;
    // vertices may be parsing from runtime.
    Expression*                                  src_{nullptr};
    std::vector<EdgeType>                        edgeTypes_;
    storage::cpp2::EdgeDirection                 edgeDirection_;
    std::vector<storage::cpp2::PropExp>          vertexProps_;
    std::vector<storage::cpp2::PropExp>          edgeProps_;
    std::vector<storage::cpp2::StatProp>         statProps_;
};

/**
 * Get property with given vertex keys.
 */
class GetVerticesNode final : public Explore {
public:
    static GetVerticesNode* make(ExecutionPlan* plan,
                             PlanNode* input,
                             GraphSpaceID space,
                             std::vector<Row> vertices,
                             Expression* src,
                             std::vector<std::string> props,
                             bool dedup = false,
                             std::vector<storage::cpp2::OrderBy> orderBy = {},
                             int64_t limit = std::numeric_limits<int64_t>::max(),
                             std::string filter = "") {
        return new GetVerticesNode(
                plan,
                input,
                space,
                std::move(vertices),
                src,
                std::move(props),
                dedup,
                std::move(orderBy),
                limit,
                std::move(filter));
    }

    std::string explain() const override;

    const std::vector<Row>& vertices() const {
        return vertices_;
    }

    Expression* src() const {
        return src_;
    }

    const std::vector<std::string>& props() const {
        return props_;
    }

private:
    GetVerticesNode(ExecutionPlan* plan,
                PlanNode* input,
                GraphSpaceID space,
                std::vector<Row> vertices,
                Expression* src,
                std::vector<std::string> props,
                bool dedup,
                std::vector<storage::cpp2::OrderBy> orderBy,
                int64_t limit,
                std::string filter)
        : Explore(plan,
                  Kind::kGetVertices,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)) {
        vertices_ = std::move(vertices);
        src_ = src;
        props_ = std::move(props);
    }

private:
    // vertices are parsing from query.
    std::vector<Row>                         vertices_;
    // vertices may be parsing from runtime.
    Expression*                              src_{nullptr};
    // props and filter are parsing from query.
    std::vector<std::string>                 props_;
};

/**
 * Get property with given edge keys.
 */
class GetEdgesNode final : public Explore {
public:
    static GetEdgesNode* make(ExecutionPlan* plan,
                          PlanNode* input,
                          GraphSpaceID space,
                          std::vector<Row> edges,
                          Expression* src,
                          Expression* ranking,
                          Expression* dst,
                          std::vector<std::string> props,
                          bool dedup = false,
                          int64_t limit = std::numeric_limits<int64_t>::max(),
                          std::vector<storage::cpp2::OrderBy> orderBy = {},
                          std::string filter = "") {
        return new GetEdgesNode(
                plan,
                input,
                space,
                std::move(edges),
                src,
                ranking,
                dst,
                std::move(props),
                dedup,
                limit,
                std::move(orderBy),
                std::move(filter));
    }

    std::string explain() const override;

    const std::vector<Row>& edges() const {
        return edges_;
    }

    Expression* src() const {
        return src_;
    }

    Expression* ranking() const {
        return ranking_;
    }

    Expression* dst() const {
        return dst_;
    }

    const std::vector<std::string>& props() const {
        return props_;
    }

private:
    GetEdgesNode(ExecutionPlan* plan,
             PlanNode* input,
             GraphSpaceID space,
             std::vector<Row> edges,
             Expression* src,
             Expression* ranking,
             Expression* dst,
             std::vector<std::string> props,
             bool dedup,
             int64_t limit,
             std::vector<storage::cpp2::OrderBy> orderBy,
             std::string filter)
        : Explore(plan,
                  Kind::kGetEdges,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)) {
        edges_ = std::move(edges);
        src_ = std::move(src);
        ranking_ = std::move(ranking);
        dst_ = std::move(dst);
        props_ = std::move(props);
    }

private:
    // edges_ are parsing from the query.
    std::vector<Row>                         edges_;
    // edges_ may be parsed from runtime.
    Expression*                              src_{nullptr};
    Expression*                              ranking_{nullptr};
    Expression*                              dst_{nullptr};
    // props and filter are parsing from query.
    std::vector<std::string>                 props_;
};

/**
 * Read data through the index.
 */
class ReadIndexNode final : public Explore {
public:
    ReadIndexNode(ExecutionPlan* plan, PlanNode* input, GraphSpaceID space)
        : Explore(plan, Kind::kReadIndex, input, space) {}

    std::string explain() const override;
};

/**
 * A FilterNode node helps filt some records with condition.
 */
class FilterNode final : public SingleInputNode {
public:
    static FilterNode* make(ExecutionPlan* plan,
                        PlanNode* input,
                        Expression* condition) {
        return new FilterNode(plan, input, condition);
    }

    const Expression* condition() const {
        return condition_;
    }

    std::string explain() const override;

private:
    FilterNode(ExecutionPlan* plan, PlanNode* input, Expression* condition)
      : SingleInputNode(plan, Kind::kFilter, input) {
        condition_ = condition;
    }

private:
    Expression*                 condition_{nullptr};
};

/**
 * Now we have three kind of set operations:
 *   UNION,
 *   INTERSECT,
 *   MINUS
 */
class SetOp : public BiInputNode {
protected:
    SetOp(ExecutionPlan* plan, Kind kind, PlanNode* left, PlanNode* right)
        : BiInputNode(plan, kind, left, right) {
        DCHECK(kind == Kind::kUnion || kind == Kind::kIntersect || kind == Kind::kMinus);
    }
};

/**
 * Combine two set of records.
 */
class UnionNode final : public SetOp {
public:
    static UnionNode* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new UnionNode(plan, left, right);
    }

    std::string explain() const override;

private:
    UnionNode(ExecutionPlan* plan, PlanNode* left, PlanNode* right)
        : SetOp(plan, Kind::kUnion, left, right) {}
};

/**
 * Return the intersected records between two sets.
 */
class IntersectNode final : public SetOp {
public:
    static IntersectNode* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new IntersectNode(plan, left, right);
    }

    std::string explain() const override;

private:
    IntersectNode(ExecutionPlan* plan, PlanNode* left, PlanNode* right)
        : SetOp(plan, Kind::kIntersect, left, right) {}
};

/**
 * Do subtraction between two sets.
 */
class MinusNode final : public SetOp {
public:
    static MinusNode* make(ExecutionPlan* plan, PlanNode* left, PlanNode* right) {
        return new MinusNode(plan, left, right);
    }

    std::string explain() const override;

private:
    MinusNode(ExecutionPlan* plan, PlanNode* left, PlanNode* right)
        : SetOp(plan, Kind::kMinus, left, right) {}
};

/**
 * ProjectNode is used to specify output vars or field.
 */
class ProjectNode final : public SingleInputNode {
public:
    static ProjectNode* make(ExecutionPlan* plan,
                         PlanNode* input,
                         YieldColumns* cols) {
        return new ProjectNode(plan, input, cols);
    }

    std::string explain() const override;

    const YieldColumns* columns() const {
        return cols_;
    }

private:
    ProjectNode(ExecutionPlan* plan, PlanNode* input, YieldColumns* cols)
      : SingleInputNode(plan, Kind::kProject, input) {
        cols_ = cols;
    }

private:
    YieldColumns*               cols_{nullptr};
};

/**
 * SortNode the given record set.
 */
class SortNode final : public SingleInputNode {
public:
    static SortNode* make(ExecutionPlan* plan,
                      PlanNode* input,
                      OrderFactors* factors) {
        return new SortNode(plan, input, factors);
    }

    const OrderFactors* factors() {
        return factors_;
    }

    std::string explain() const override;

private:
    SortNode(ExecutionPlan* plan, PlanNode* input, OrderFactors* factors)
      : SingleInputNode(plan, Kind::kSort, input) {
        factors_ = factors;
    }

private:
    OrderFactors*   factors_{nullptr};
};

/**
 * Output the records with the given limitation.
 */
class LimitNode final : public SingleInputNode {
public:
    static LimitNode* make(ExecutionPlan* plan,
                       PlanNode* input,
                       int64_t offset,
                       int64_t count) {
        return new LimitNode(plan, input, offset, count);
    }

    int64_t offset() const {
        return offset_;
    }

    int64_t count() const {
        return count_;
    }

    std::string explain() const override;

private:
    LimitNode(ExecutionPlan* plan, PlanNode* input, int64_t offset, int64_t count)
        : SingleInputNode(plan, Kind::kLimit, input) {
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
class AggregateNode : public SingleInputNode {
public:
    static AggregateNode* make(ExecutionPlan* plan,
                           PlanNode* input,
                           YieldColumns* groupCols) {
        return new AggregateNode(plan, input, groupCols);
    }

    const YieldColumns* groups() const {
        return groupCols_;
    }

    std::string explain() const override;

private:
    AggregateNode(ExecutionPlan* plan, PlanNode* input, YieldColumns* groupCols)
        : SingleInputNode(plan, Kind::kAggregate, input) {
        groupCols_ = groupCols;
    }

private:
    YieldColumns*   groupCols_;
};

class BinarySelect : public SingleInputNode {
public:
    const Expression* condition() const {
        return condition_;
    }

protected:
    BinarySelect(ExecutionPlan* plan, Kind kind, PlanNode* input, Expression* condition)
        : SingleInputNode(plan, kind, input), condition_(condition) {}

    Expression*  condition_{nullptr};
};

class SelectorNode : public BinarySelect {
public:
    static SelectorNode* make(ExecutionPlan* plan,
                          PlanNode* input,
                          PlanNode* ifBranch,
                          PlanNode* elseBranch,
                          Expression* condition) {
        return new SelectorNode(plan, input, ifBranch, elseBranch, condition);
    }

    void setIf(PlanNode* ifBranch) {
        if_ = ifBranch;
    }

    void setElse(PlanNode* elseBranch) {
        else_ = elseBranch;
    }

    std::string explain() const override;

    const PlanNode* then() const {
        return if_;
    }

    const PlanNode* otherwise() const {
        return else_;
    }

private:
    SelectorNode(ExecutionPlan* plan,
             PlanNode* input,
             PlanNode* ifBranch,
             PlanNode* elseBranch,
             Expression* condition)
        : BinarySelect(plan, Kind::kSelector, input, condition) {
        if_ = ifBranch;
        else_ = elseBranch;
    }

private:
    PlanNode*   if_{nullptr};
    PlanNode*   else_{nullptr};
};

class LoopNode : public BinarySelect {
public:
    static LoopNode* make(ExecutionPlan* plan,
                      PlanNode* input,
                      PlanNode* body,
                      Expression* condition) {
        return new LoopNode(plan, input, body, condition);
    }

    void setBody(PlanNode* body) {
        body_ = body;
    }

    std::string explain() const override;

    const PlanNode* body() const {
        return body_;
    }

private:
    LoopNode(ExecutionPlan* plan, PlanNode* input, PlanNode* body, Expression* condition);

    PlanNode*   body_{nullptr};
};

class SwitchSpaceNode : public SingleInputNode {
public:
    static SwitchSpaceNode* make(ExecutionPlan* plan,
                                        PlanNode* input,
                                        std::string spaceName,
                                        GraphSpaceID spaceId) {
        return new SwitchSpaceNode(plan, input, spaceName, spaceId);
    }

    const std::string& getSpaceName() const {
        return spaceName_;
    }

    GraphSpaceID getSpaceId() const {
        return spaceId_;
    }

    std::string explain() const override;

private:
    SwitchSpaceNode(ExecutionPlan* plan,
                           PlanNode* input,
                           std::string spaceName,
                           GraphSpaceID spaceId)
        : SingleInputNode(plan, Kind::kSwitchSpace, input) {
        spaceName_ = std::move(spaceName);
        spaceId_ = spaceId;
    }

private:
    std::string     spaceName_;
    GraphSpaceID    spaceId_{-1};
};

class DedupNode : public SingleInputNode {
public:
    static DedupNode* make(ExecutionPlan* plan,
                       PlanNode* input,
                       Expression* expr) {
        return new DedupNode(plan, input, expr);
    }

    void setExpr(Expression* expr) {
        expr_ = expr;
    }

    std::string explain() const override;

private:
    DedupNode(ExecutionPlan* plan, PlanNode* input, Expression* expr)
        : SingleInputNode(plan, Kind::kDedup, input) {
        expr_ = expr;
    }

private:
    Expression*     expr_{nullptr};
};

class ProduceSemiShortestPath : public PlanNode {
};

class ConjunctPath : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_QUERY_H_
