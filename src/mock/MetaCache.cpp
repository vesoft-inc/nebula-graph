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
    roles_.emplace(spaceId, UserRoles());
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
    roles_.erase(id);
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

////////////////////////////////////////////// ACL related mock ////////////////////////////////////
meta::cpp2::ExecResp MetaCache::createUser(const meta::cpp2::CreateUserReq& req) {
    meta::cpp2::ExecResp resp;
    folly::RWSpinLock::WriteHolder wh(userLock_);
    const auto user = users_.find(req.get_account());
    if (user != users_.end()) {  // already exists
        resp.set_code(req.get_if_not_exists() ?
                    meta::cpp2::ErrorCode::SUCCEEDED :
                    meta::cpp2::ErrorCode::E_EXISTED);
        return resp;
    }

    auto result = users_.emplace(req.get_account(), UserInfo{req.get_encoded_pwd()});
    resp.set_code(result.second ?
                  meta::cpp2::ErrorCode::SUCCEEDED :
                  meta::cpp2::ErrorCode::E_UNKNOWN);
    return resp;
}

meta::cpp2::ExecResp MetaCache::dropUser(const meta::cpp2::DropUserReq& req) {
    meta::cpp2::ExecResp resp;
    folly::RWSpinLock::WriteHolder wh(userLock_);
    const auto user = users_.find(req.get_account());
    if (user == users_.end()) {  // not exists
        resp.set_code(req.get_if_exists() ?
                    meta::cpp2::ErrorCode::SUCCEEDED :
                    meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }

    auto result = users_.erase(req.get_account());
    resp.set_code(result == 1 ?
                  meta::cpp2::ErrorCode::SUCCEEDED :
                  meta::cpp2::ErrorCode::E_UNKNOWN);
    return resp;
}

meta::cpp2::ExecResp MetaCache::alterUser(const meta::cpp2::AlterUserReq& req) {
    meta::cpp2::ExecResp resp;
    folly::RWSpinLock::WriteHolder wh(userLock_);
    auto user = users_.find(req.get_account());
    if (user == users_.end()) {  // not exists
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    user->second.password = req.get_encoded_pwd();
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    return resp;
}

meta::cpp2::ExecResp MetaCache::grantRole(const meta::cpp2::GrantRoleReq& req) {
    meta::cpp2::ExecResp resp;
    const auto &item = req.get_role_item();
    {
        folly::RWSpinLock::ReadHolder spaceRH(roleLock_);
        folly::RWSpinLock::ReadHolder userRH(userLock_);
        // find space
        auto space = roles_.find(item.get_space_id());
        if (space == roles_.end()) {
            resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
            return resp;
        }
        // find user
        auto user = users_.find(item.get_user_id());
        if (user == users_.end()) {
            resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
            return resp;
        }
    }
    folly::RWSpinLock::WriteHolder roleWH(roleLock_);
    // space
    auto space = roles_.find(item.get_space_id());
    // user
    auto user = space->second.find(item.get_user_id());
    if (user == space->second.end()) {
        space->second.emplace(item.get_user_id(),
                              std::unordered_set<meta::cpp2::RoleType>{item.get_role_type()});
    } else {
        user->second.emplace(item.get_role_type());
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    return resp;
}

meta::cpp2::ExecResp MetaCache::revokeRole(const meta::cpp2::RevokeRoleReq& req) {
    meta::cpp2::ExecResp resp;
    const auto &item = req.get_role_item();
    folly::RWSpinLock::WriteHolder rolesWH(roleLock_);
    // find space
    auto space = roles_.find(item.get_space_id());
    if (space == roles_.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    // find user
    auto user = space->second.find(item.get_user_id());
    if (user == space->second.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    // find role
    auto role = user->second.find(item.get_role_type());
    if (role == user->second.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    user->second.erase(item.get_role_type());
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    return resp;
}

meta::cpp2::ListUsersResp MetaCache::listUsers(const meta::cpp2::ListUsersReq&) {
    meta::cpp2::ListUsersResp resp;
    folly::RWSpinLock::ReadHolder rh(userLock_);
    std::unordered_map<std::string, std::string> users;
    for (const auto &user : users_) {
        users.emplace(user.first, user.second.password);
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_users(std::move(users));
    return resp;
}

meta::cpp2::ListRolesResp MetaCache::listRoles(const meta::cpp2::ListRolesReq& req) {
    meta::cpp2::ListRolesResp resp;
    folly::RWSpinLock::ReadHolder rh(roleLock_);
    std::vector<meta::cpp2::RoleItem> items;
    const auto space = roles_.find(req.get_space_id());
    if (space == roles_.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    for (const auto &user : space->second) {
        for (const auto &role : user.second) {
            meta::cpp2::RoleItem item;
            item.set_space_id(space->first);
            item.set_user_id(user.first);
            item.set_role_type(role);
            items.emplace_back(std::move(item));
        }
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_roles(std::move(items));
    return resp;
}

meta::cpp2::ExecResp MetaCache::changePassword(const meta::cpp2::ChangePasswordReq& req) {
    meta::cpp2::ExecResp resp;
    folly::RWSpinLock::WriteHolder wh(userLock_);
    auto user = users_.find(req.get_account());
    if (user == users_.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    if (user->second.password != req.get_old_encoded_pwd()) {
        resp.set_code(meta::cpp2::ErrorCode::E_INVALID_PASSWORD);
    }
    user->second.password = req.get_new_encoded_pwd();
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    return resp;
}

meta::cpp2::ListRolesResp MetaCache::getUserRoles(const meta::cpp2::GetUserRolesReq& req) {
    meta::cpp2::ListRolesResp resp;
    {
        folly::RWSpinLock::ReadHolder userRH(userLock_);
        // find user
        auto user = users_.find(req.get_account());
        if (user == users_.end()) {
            resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
            return resp;
        }
    }

    folly::RWSpinLock::ReadHolder roleRH(roleLock_);
    std::vector<meta::cpp2::RoleItem> items;
    for (const auto& space : roles_) {
        const auto& user = space.second.find(req.get_account());
        if (user != space.second.end()) {
            for (const auto & role : user->second) {
                meta::cpp2::RoleItem item;
                item.set_space_id(space.first);
                item.set_user_id(user->first);
                item.set_role_type(role);
                items.emplace_back(std::move(item));
            }
        }
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_roles(std::move(items));
    return resp;
}

}  // namespace graph
}  // namespace nebula
