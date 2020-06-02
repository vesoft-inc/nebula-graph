/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/MetaCache.h"

namespace nebula {
namespace graph {

#define CHECK_SPACE_ID(spaceId) \
    auto spaceIter = cache_.find(spaceId); \
        if (spaceIter == cache_.end()) { \
        return Status::Error("SpaceID `%d' not found", spaceId); \
    }

Status MetaCache::createSpace(const meta::cpp2::CreateSpaceReq &req, GraphSpaceID &spaceId) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto ifNotExists = req.get_if_not_exists();
    auto properties = req.get_properties();
    auto spaceName = properties.get_space_name();
    auto findIter = spaces_.find(spaceName);
    if (ifNotExists && findIter != spaces_.end()) {
        spaceId = findIter->second.get_space_id();
        return Status::OK();
    }
    if (findIter != spaces_.end()) {
        return Status::Error("Space `%s' existed", spaceName.c_str());
    }
    spaceId = ++id_;
    meta::cpp2::SpaceItem space;
    space.set_space_id(spaceId);
    space.set_properties(std::move(properties));
    spaces_[spaceName] = space;
    VLOG(1) << "space name: " << space.get_properties().get_space_name()
            << ", partition_num: " << space.get_properties().get_partition_num()
            << ", replica_factor: " << space.get_properties().get_replica_factor()
            << ", rvid_size: " << space.get_properties().get_vid_size();
    cache_[spaceId] = SpaceInfoCache();
    return Status::OK();
}

StatusOr<meta::cpp2::SpaceItem> MetaCache::getSpace(const meta::cpp2::GetSpaceReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    auto findIter = spaces_.find(req.get_space_name());
    if (findIter == spaces_.end()) {
        return Status::Error("Space `%s' not found", req.get_space_name().c_str());
    }
    VLOG(1) << "space name: " << findIter->second.get_properties().get_space_name()
            << ", partition_num: " << findIter->second.get_properties().get_partition_num()
            << ", replica_factor: " << findIter->second.get_properties().get_replica_factor()
            << ", rvid_size: " << findIter->second.get_properties().get_vid_size();
    return findIter->second;
}

StatusOr<std::vector<meta::cpp2::IdName>> MetaCache::listSpaces() {
    folly::RWSpinLock::ReadHolder holder(lock_);
    std::vector<meta::cpp2::IdName> spaces;
    for (auto &item : spaces_) {
        meta::cpp2::IdName idName;
        idName.set_id(to(item.second.get_space_id(), EntryType::SPACE));
        idName.set_name(item.first);
        spaces.emplace_back(idName);
    }
    return spaces;
}

Status MetaCache::dropSpace(const meta::cpp2::DropSpaceReq &req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto spaceName  = req.get_space_name();
    auto findIter = spaces_.find(spaceName);
    auto ifExists = req.get_if_exists();

    if (ifExists && findIter == spaces_.end()) {
        Status::OK();
    }

    if (findIter == spaces_.end()) {
        return Status::Error("Space `%s' not existed", req.get_space_name().c_str());
    }
    auto id = findIter->second.get_space_id();
    spaces_.erase(spaceName);
    cache_.erase(id);
    return Status::OK();
}

Status MetaCache::createTag(const meta::cpp2::CreateTagReq &req, TagID &tagId) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifNotExists = req.get_if_not_exists();
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (ifNotExists && findIter != tagSchemas.end()) {
        tagId = findIter->second.get_tag_id();
        return Status::OK();
    }

    tagId = ++id_;
    meta::cpp2::TagItem tagItem;
    tagItem.set_tag_id(tagId);
    tagItem.set_tag_name(tagName);
    tagItem.set_version(0);
    tagItem.set_schema(req.get_schema());
    tagSchemas[tagName] = std::move(tagItem);
    return Status::OK();
}

StatusOr<meta::cpp2::Schema> MetaCache::getTag(const meta::cpp2::GetTagReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (findIter == tagSchemas.end()) {
        LOG(ERROR) << "Tag name: " << tagName << " not found";
        return Status::Error("Not found");
    }
    return findIter->second.get_schema();
}

StatusOr<std::vector<meta::cpp2::TagItem>>
MetaCache::listTags(const meta::cpp2::ListTagsReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    std::vector<meta::cpp2::TagItem> tagItems;
    for (const auto& item : spaceIter->second.tagSchemas_) {
        tagItems.emplace_back(item.second);
    }
    return tagItems;
}

Status MetaCache::dropTag(const meta::cpp2::DropTagReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifExists = req.get_if_exists();
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (ifExists && findIter == tagSchemas.end()) {
        return Status::OK();
    }
    if (findIter == tagSchemas.end()) {
        return Status::Error("Tag `%s' not existed", req.get_tag_name().c_str());
    }

    tagSchemas.erase(findIter);
    return Status::OK();
}

Status MetaCache::createEdge(const meta::cpp2::CreateEdgeReq &req, EdgeType &edgeType) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifNotExists = req.get_if_not_exists();
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (ifNotExists && findIter != edgeSchemas.end()) {
        edgeType = findIter->second.get_edge_type();
        return Status::OK();
    }

    edgeType = ++id_;
    meta::cpp2::EdgeItem edgeItem;
    edgeItem.set_edge_type(edgeType);
    edgeItem.set_edge_name(edgeName);
    edgeItem.set_version(0);
    edgeItem.set_schema(req.get_schema());
    edgeSchemas[edgeName] = std::move(edgeItem);
    return Status::OK();
}

StatusOr<meta::cpp2::Schema> MetaCache::getEdge(const meta::cpp2::GetEdgeReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (findIter == edgeSchemas.end()) {
        return Status::Error("Not found");
    }
    return findIter->second.get_schema();
}

StatusOr<std::vector<meta::cpp2::EdgeItem>>
MetaCache::listEdges(const meta::cpp2::ListEdgesReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    std::vector<meta::cpp2::EdgeItem> edgeItems;
    for (const auto& item : spaceIter->second.edgeSchemas_) {
        edgeItems.emplace_back(item.second);
    }
    return edgeItems;
}

Status MetaCache::dropEdge(const meta::cpp2::DropEdgeReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifExists = req.get_if_exists();
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (ifExists && findIter == edgeSchemas.end()) {
        return Status::OK();
    }

    if (findIter == edgeSchemas.end()) {
        return Status::Error("Edge `%s' not existed", req.get_edge_name().c_str());
    }

    edgeSchemas.erase(findIter);
    return Status::OK();
}

Status MetaCache::createTagIndex(const meta::cpp2::CreateTagIndexReq&) {
    return Status::OK();
}

Status MetaCache::createEdgeIndex(const meta::cpp2::CreateEdgeIndexReq&) {
    return Status::OK();
}

Status MetaCache::dropTagIndex(const meta::cpp2::DropTagIndexReq&) {
    return Status::OK();
}

Status MetaCache::dropTagIndex(const meta::cpp2::DropEdgeIndexReq&) {
    return Status::OK();
}

Status MetaCache::regConfigs(const std::vector<meta::cpp2::ConfigItem>&) {
    return Status::OK();
}

Status MetaCache::setConfig(const meta::cpp2::ConfigItem&) {
    return Status::OK();
}

Status MetaCache::heartBeat(const meta::cpp2::HBReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto host = req.get_host();
    if (host.port == 0) {
        return Status::OK();
    }
    hostSet_.emplace(std::move(host));
    return Status::OK();
}

Status MetaCache::listUsers(const meta::cpp2::ListUsersReq&) {
    return Status::OK();
}

std::vector<meta::cpp2::HostItem> MetaCache::listHosts() {
    folly::RWSpinLock::WriteHolder holder(lock_);
    std::vector<meta::cpp2::HostItem> hosts;
    for (auto& spaceIdIt : spaces_) {
        auto spaceName = spaceIdIt.first;
        for (auto &h : hostSet_) {
            meta::cpp2::HostItem host;
            host.set_hostAddr(h);
            host.set_status(meta::cpp2::HostStatus::ONLINE);
            std::unordered_map<std::string, std::vector<PartitionID>> leaderParts;
            std::vector<PartitionID> parts = {1};
            leaderParts.emplace(spaceName, parts);
            host.set_leader_parts(leaderParts);
            host.set_all_parts(std::move(leaderParts));
        }
    }
    return hosts;
}

std::unordered_map<PartitionID, std::vector<HostAddr>> MetaCache::getParts() {
    folly::RWSpinLock::WriteHolder holder(lock_);
    std::unordered_map<PartitionID, std::vector<HostAddr>> parts;
    parts[1] = {};
    for (auto &h : hostSet_) {
        parts[1].emplace_back(h);
    }
    return parts;
}

#define JOB_CHECK_ID() \
    const auto &params = req.get_paras(); \
    if (params.empty()) { \
        return meta::cpp2::ErrorCode::E_INVALID_PARM; \
    } \
    int64_t jobId; \
    try { \
        jobId = folly::to<int64_t>(params.front()); \
    } catch (std::exception &e) { \
        LOG(ERROR) << e.what(); \
        return meta::cpp2::ErrorCode::E_INVALID_PARM; \
    } \
    const auto job = jobs_.find(jobId); \
    if (job == jobs_.end()) { \
        return meta::cpp2::ErrorCode::E_INVALID_PARM; \
    }

ErrorOr<meta::cpp2::ErrorCode, meta::cpp2::AdminJobResult>
MetaCache::runAdminJob(const meta::cpp2::AdminJobReq& req) {
    meta::cpp2::AdminJobResult result;
    switch (req.get_op()) {
    case meta::cpp2::AdminJobOp::ADD: {
        folly::RWSpinLock::WriteHolder wh(jobLock_);
        auto jobId = incId();
        jobs_.emplace(jobId, JobDesc{
            req.get_cmd(),
            req.get_paras(),
            meta::cpp2::JobStatus::QUEUE,
            0,
            0
        });
        std::vector<TaskDesc> descs;
        int32_t iTask = 0;
        for (const auto &host : hostSet_) {
            descs.reserve(hostSet_.size());
            descs.emplace_back(TaskDesc{
                ++iTask,
                host,
                meta::cpp2::JobStatus::QUEUE,
                0,
                0
            });
        }
        tasks_.emplace(jobId, std::move(descs));
        result.set_job_id(jobId);
        return result;
    }
    case meta::cpp2::AdminJobOp::RECOVER: {
        uint32_t jobNum = 0;
        folly::RWSpinLock::WriteHolder wh(jobLock_);
        for (auto &job : jobs_) {
            if (job.second.status_ == meta::cpp2::JobStatus::FAILED) {
                job.second.status_ = meta::cpp2::JobStatus::QUEUE;
                ++jobNum;
            }
        }
        result.set_recovered_job_num(jobNum);
        return result;
    }
    case meta::cpp2::AdminJobOp::SHOW: {
        folly::RWSpinLock::ReadHolder rh(jobLock_);
        JOB_CHECK_ID();
        result.set_job_id(job->first);
        std::vector<meta::cpp2::JobDesc> jobsDesc;
        meta::cpp2::JobDesc jobDesc;
        jobDesc.set_id(job->first);
        jobDesc.set_cmd(job->second.cmd_);
        jobDesc.set_status(job->second.status_);
        jobDesc.set_start_time(job->second.startTime_);
        jobDesc.set_stop_time(job->second.stopTime_);
        jobsDesc.emplace_back(std::move(jobDesc));
        result.set_job_desc(std::move(jobsDesc));

        // tasks
        const auto tasks = tasks_.find(job->first);
        if (tasks == tasks_.end()) {
            LOG(FATAL) << "Impossible not find tasks of job id " << job->first;
        }
        std::vector<meta::cpp2::TaskDesc> tasksDesc;
        for (const auto &task : tasks->second) {
            meta::cpp2::TaskDesc taskDesc;
            taskDesc.set_job_id(job->first);
            taskDesc.set_task_id(task.iTask_);
            taskDesc.set_host(task.dest_);
            taskDesc.set_status(task.status_);
            taskDesc.set_start_time(task.startTime_);
            taskDesc.set_stop_time(task.stopTime_);
            tasksDesc.emplace_back(std::move(taskDesc));
        }
        result.set_task_desc(std::move(tasksDesc));
        return result;
    }
    case meta::cpp2::AdminJobOp::SHOW_All: {
        std::vector<meta::cpp2::JobDesc> jobsDesc;
        folly::RWSpinLock::ReadHolder rh(jobLock_);
        for (const auto &job : jobs_) {
            meta::cpp2::JobDesc jobDesc;
            jobDesc.set_id(job.first);
            jobDesc.set_cmd(job.second.cmd_);
            jobDesc.set_status(job.second.status_);
            jobDesc.set_start_time(job.second.startTime_);
            jobDesc.set_stop_time(job.second.stopTime_);
            jobsDesc.emplace_back(std::move(jobDesc));
        }
        result.set_job_desc(std::move(jobsDesc));
        return result;
    }
    case meta::cpp2::AdminJobOp::STOP: {
        folly::RWSpinLock::WriteHolder wh(jobLock_);
        JOB_CHECK_ID();
        if (job->second.status_ != meta::cpp2::JobStatus::QUEUE &&
            job->second.status_ != meta::cpp2::JobStatus::RUNNING) {
            return meta::cpp2::ErrorCode::E_CONFLICT;
        }
        job->second.status_ = meta::cpp2::JobStatus::STOPPED;
        return result;
    }
    }
}

}  // namespace graph
}  // namespace nebula
