/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/DataCollectExecutor.h"

#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"
#include "common/datatypes/List.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataCollectExecutor::execute() {
    return doCollect().ensure([this] () {
        result_ = Value::kEmpty;
        colNames_.clear();
    });
}

folly::Future<Status> DataCollectExecutor::doCollect() {
    SCOPED_TIMER(&execTime_);

    auto* dc = asNode<DataCollect>(node());
    colNames_ = dc->colNames();
    auto vars = dc->vars();
    switch (dc->kind()) {
        case DataCollect::DCKind::kSubgraph: {
            return collectSubgraph(vars);
        }
        case DataCollect::DCKind::kRowBasedMove: {
            NG_RETURN_IF_ERROR(rowBasedMove(vars));
            break;
        }
        case DataCollect::DCKind::kMToN: {
            NG_RETURN_IF_ERROR(collectMToN(vars, dc->step(), dc->distinct()));
            break;
        }
        case DataCollect::DCKind::kBFSShortest: {
            NG_RETURN_IF_ERROR(collectBFSShortest(vars));
            break;
        }
        case DataCollect::DCKind::kAllPaths: {
            NG_RETURN_IF_ERROR(collectAllPaths(vars));
            break;
        }
        case DataCollect::DCKind::kMultiplePairShortest: {
            NG_RETURN_IF_ERROR(collectMultiplePairShortestPath(vars));
            break;
        }
        case DataCollect::DCKind::kPathProp: {
            NG_RETURN_IF_ERROR(collectPathProp(vars));
            break;
        }
        default:
            LOG(FATAL) << "Unknown data collect type: " << static_cast<int64_t>(dc->kind());
    }
    ResultBuilder builder;
    builder.value(Value(std::move(result_))).iter(Iterator::Kind::kSequential);
    return finish(builder.finish());
}

folly::Future<Status> DataCollectExecutor::collectSubgraph(const std::vector<std::string>& vars) {
    ds_.colNames = std::move(colNames_);

    const auto& hist = ectx_->getHistory(vars[0]);
    ds_.rows.reserve(hist.size());
    return std::move(dedupByMultiJobs(hist.begin(), hist.end()))
        .via(runner())
        .thenValue([vars = vars, this] (auto&& status) {
            if (!status.ok()) {
                return status;
            }
            if (vars.size() < 2) {
                return Status::OK();
            }
            // latestVersion subgraph->outputVar() OR filter->outputVar()
            const auto& res = ectx_->getResult(vars[1]);
            auto iter = res.iter();
            if (iter->isPropIter()) {
                auto* pIter = static_cast<PropIter*>(iter.get());
                List vertices = pIter->getVertices();
                List edges;
                ds_.rows.emplace_back(Row({std::move(vertices), std::move(edges)}));
            }
            result_.setDataSet(std::move(ds_));
            ResultBuilder builder;
            builder.value(Value(std::move(result_))).iter(Iterator::Kind::kSequential);
            return finish(builder.finish());
        });
}

folly::Future<Status> DataCollectExecutor::dedupByMultiJobs(
    std::vector<Result>::const_iterator i,
    std::vector<Result>::const_iterator end) {
    Row row;
    row.values.resize(2);
    ds_.rows.emplace_back(std::move(row));
	auto iter = (*i).iter();
	auto* gnIter = static_cast<GetNeighborsIter*>(iter.get());
	vertices_ = gnIter->getVertices();
    std::vector<folly::Future<std::vector<Value>>> vFutures;
	for (size_t j = 0; j < vertices_.values.size();) {
		auto lower = j;
		auto upper = j + FLAGS_subgraph_dedup_batch;
		if (upper >= vertices_.size()) {
			upper = vertices_.size();
		}
		auto dedupVertices = [lower, upper, this] (auto&& r) {
            UNUSED(r);
            std::vector<Value> vertices;
            vertices.reserve(upper - lower);
			for (auto k = lower; k < upper; ++k) {
				auto& v = vertices_.values[k];
				if (!v.isVertex()) {
					continue;
				}
				if (uniqueVids_.emplace(v.getVertex().vid, 0).second) {
                    vertices.emplace_back(std::move(v));
                }
			}
            return vertices;
		};
		auto f = folly::makeFuture().via(runner()).then(dedupVertices);
		vFutures.emplace_back(std::move(f));
		j = upper;
	}
    auto vF = folly::collect(vFutures).via(runner()).thenValue([this] (auto&& vertices) {
                List resultVertices;
                for (auto& vs : vertices) {
                    resultVertices.values.insert(resultVertices.values.end(),
                                           std::make_move_iterator(vs.begin()),
                                           std::make_move_iterator(vs.end()));
                }
                VLOG(1) << "collect vs:" << resultVertices;
                ds_.rows.back().values[0].setList(std::move(resultVertices));
                return Status::OK();
            });

	edges_ = gnIter->getEdges();
    std::vector<folly::Future<std::vector<Value>>> eFutures;
	for (size_t j = 0; j < edges_.size();) {
		auto lower = j;
		auto upper = j + FLAGS_subgraph_dedup_batch;
		if (upper >= edges_.size()) {
			upper = edges_.size();
		}
		auto dedupEdges = [lower, upper, this] (auto&& r) {
            UNUSED(r);
            std::vector<Value> edges;
            edges.reserve(upper - lower);
            for (auto k = lower; k < upper; ++k) {
                auto& edge = edges_.values[k];
                if (!edge.isEdge()) {
                    continue;
                }
                const auto& e = edge.getEdge();
                auto edgeKey = std::make_tuple(e.src, e.type, e.ranking, e.dst);
                if (uniqueEdges_.emplace(std::move(edgeKey), 0).second) {
                    edges.emplace_back(std::move(edge));
                }
            }
            return edges;
		};
		auto f = folly::makeFuture().via(runner()).then(dedupEdges);
		eFutures.emplace_back(std::move(f));
		j = upper;
	}
    auto eF = folly::collect(eFutures).via(runner()).thenValue([this] (auto&& edges) mutable {
                List resultEdges;
                for (auto& es : edges) {
                    resultEdges.values.insert(resultEdges.values.end(),
                            std::make_move_iterator(es.begin()),
                            std::make_move_iterator(es.end()));
                }
                VLOG(1) << "collect es: " << resultEdges;
                ds_.rows.back().values[1].setList(std::move(resultEdges));
                return Status::OK();
            });

    std::vector<folly::Future<Status>> futures;
    futures.emplace_back(std::move(vF));
    futures.emplace_back(std::move(eF));
    return folly::collect(futures).via(runner()).thenValue([i, end, this] (auto&& s) mutable {
        UNUSED(s);
        if (!(++i < end)) {
            return Status::OK();
        } else {
            dedupByMultiJobs(i, end);
        }
        return Status::OK();
    });
}

Status DataCollectExecutor::rowBasedMove(const std::vector<std::string>& vars) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    DCHECK(!ds.colNames.empty());
    for (auto& var : vars) {
        auto& result = ectx_->getResult(var);
        auto iter = result.iter();
        ds.rows.reserve(ds.rows.size() + iter->size());
        if (iter->isSequentialIter() || iter->isPropIter()) {
            auto* seqIter = static_cast<SequentialIter*>(iter.get());
            for (; seqIter->valid(); seqIter->next()) {
                ds.rows.emplace_back(seqIter->moveRow());
            }
        } else {
            return Status::Error("Iterator should be kind of SequentialIter.");
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}

Status DataCollectExecutor::collectMToN(const std::vector<std::string>& vars,
                                        const StepClause& mToN,
                                        bool distinct) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    DCHECK(!ds.colNames.empty());
    std::unordered_set<const Row*> unique;
    // itersHolder keep life cycle of iters util this method return.
    std::vector<std::unique_ptr<Iterator>> itersHolder;
    for (auto& var : vars) {
        auto& hist = ectx_->getHistory(var);
        std::size_t histSize = hist.size();
        DCHECK_GE(mToN.mSteps(), 1);
        std::size_t n = mToN.nSteps() > histSize ? histSize : mToN.nSteps();
        for (auto i = mToN.mSteps() - 1; i < n; ++i) {
            auto iter = hist[i].iter();
            if (iter->isSequentialIter()) {
                auto* seqIter = static_cast<SequentialIter*>(iter.get());
                while (seqIter->valid()) {
                    if (distinct && !unique.emplace(seqIter->row()).second) {
                        seqIter->unstableErase();
                    } else {
                        seqIter->next();
                    }
                }
            } else {
                std::stringstream msg;
                msg << "Iterator should be kind of SequentialIter, but was: " << iter->kind();
                return Status::Error(msg.str());
            }
            itersHolder.emplace_back(std::move(iter));
        }
    }

    for (auto& iter : itersHolder) {
        if (iter->isSequentialIter()) {
            auto* seqIter = static_cast<SequentialIter*>(iter.get());
            for (seqIter->reset(); seqIter->valid(); seqIter->next()) {
                ds.rows.emplace_back(seqIter->moveRow());
            }
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}

Status DataCollectExecutor::collectBFSShortest(const std::vector<std::string>& vars) {
    // Will rewrite this method once we implement returning the props for the path.
    return rowBasedMove(vars);
}

Status DataCollectExecutor::collectAllPaths(const std::vector<std::string>& vars) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    DCHECK(!ds.colNames.empty());

    for (auto& var : vars) {
        auto& hist = ectx_->getHistory(var);
        for (auto& result : hist) {
            auto iter = result.iter();
            if (iter->isSequentialIter()) {
                auto* seqIter = static_cast<SequentialIter*>(iter.get());
                for (; seqIter->valid(); seqIter->next()) {
                    ds.rows.emplace_back(seqIter->moveRow());
                }
            } else {
                std::stringstream msg;
                msg << "Iterator should be kind of SequentialIter, but was: " << iter->kind();
                return Status::Error(msg.str());
            }
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}

Status DataCollectExecutor::collectMultiplePairShortestPath(const std::vector<std::string>& vars) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    DCHECK(!ds.colNames.empty());

    // src : {dst : <cost, {path}>}
    std::unordered_map<Value, std::unordered_map<Value, std::pair<Value, std::vector<Path>>>>
        shortestPath;

    for (auto& var : vars) {
        auto& hist = ectx_->getHistory(var);
        for (auto& result : hist) {
            auto iter = result.iter();
            if (!iter->isSequentialIter()) {
                std::stringstream msg;
                msg << "Iterator should be kind of SequentialIter, but was: " << iter->kind();
                return Status::Error(msg.str());
            }
            auto* seqIter = static_cast<SequentialIter*>(iter.get());
            for (; seqIter->valid(); seqIter->next()) {
                auto& pathVal = seqIter->getColumn(kPathStr);
                auto cost = seqIter->getColumn(kCostStr);
                if (!pathVal.isPath()) {
                    return Status::Error("Type error `%s', should be PATH",
                                         pathVal.typeName().c_str());
                }
                auto& path = pathVal.getPath();
                auto& src = path.src.vid;
                auto& dst = path.steps.back().dst.vid;
                if (shortestPath.find(src) == shortestPath.end() ||
                    shortestPath[src].find(dst) == shortestPath[src].end()) {
                    auto& dstHist = shortestPath[src];
                    std::vector<Path> tempPaths = {std::move(path)};
                    dstHist.emplace(dst, std::make_pair(cost, std::move(tempPaths)));
                } else {
                    auto oldCost = shortestPath[src][dst].first;
                    if (cost < oldCost) {
                        std::vector<Path> tempPaths = {std::move(path)};
                        shortestPath[src][dst].second.swap(tempPaths);
                    } else if (cost == oldCost) {
                        shortestPath[src][dst].second.emplace_back(std::move(path));
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    // collect result
    for (auto& srcPath : shortestPath) {
        for (auto& dstPath : srcPath.second) {
            for (auto& path : dstPath.second.second) {
                Row row;
                row.values.emplace_back(std::move(path));
                ds.rows.emplace_back(std::move(row));
            }
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}

Status DataCollectExecutor::collectPathProp(const std::vector<std::string>& vars) {
    DataSet ds;
    ds.colNames = colNames_;
    DCHECK(!ds.colNames.empty());
    // 0: vertices's props, 1: Edges's props 2: paths without prop
    DCHECK_EQ(vars.size(), 3);

    auto vIter = ectx_->getResult(vars[0]).iter();
    std::unordered_map<Value, Vertex> vertexMap;
    vertexMap.reserve(vIter->size());
    DCHECK(vIter->isPropIter());
    for (; vIter->valid(); vIter->next()) {
        const auto& vertexVal = vIter->getVertex();
        if (!vertexVal.isVertex()) {
            continue;
        }
        const auto& vertex = vertexVal.getVertex();
        vertexMap.insert(std::make_pair(vertex.vid, std::move(vertex)));
    }

    auto eIter = ectx_->getResult(vars[1]).iter();
    std::unordered_map<std::tuple<Value, EdgeType, EdgeRanking, Value>, Edge> edgeMap;
    edgeMap.reserve(eIter->size());
    DCHECK(eIter->isPropIter());
    for (; eIter->valid(); eIter->next()) {
        auto edgeVal = eIter->getEdge();
        if (!edgeVal.isEdge()) {
            continue;
        }
        auto& edge = edgeVal.getEdge();
        auto edgeKey = std::make_tuple(edge.src, edge.type, edge.ranking, edge.dst);
        edgeMap.insert(std::make_pair(std::move(edgeKey), std::move(edge)));
    }

    auto pIter = ectx_->getResult(vars[2]).iter();
    DCHECK(pIter->isSequentialIter());
    for (; pIter->valid(); pIter->next()) {
        auto& pathVal = pIter->getColumn(0);
        if (!pathVal.isPath()) {
            continue;
        }
        auto path = pathVal.getPath();
        auto src = path.src.vid;
        auto found = vertexMap.find(src);
        if (found != vertexMap.end()) {
            path.src = found->second;
        }
        for (auto& step : path.steps) {
            auto dst = step.dst.vid;
            step.dst = vertexMap[dst];

            auto type = step.type;
            auto ranking = step.ranking;
            if (type < 0) {
                dst = src;
                src = step.dst.vid;
                type = -type;
            }
            auto edgeKey = std::make_tuple(src, type, ranking, dst);
            auto edge = edgeMap[edgeKey];
            step.props = edge.props;
            src = step.dst.vid;
        }
        ds.rows.emplace_back(Row({std::move(path)}));
    }
    VLOG(2) << "Path with props : \n" << ds;
    result_.setDataSet(std::move(ds));
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
