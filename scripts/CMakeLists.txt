install(
    FILES
        nebula-graphd.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        graph
)

install(
    FILES
        nebula.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        common
)

install(
    FILES
        utils.sh
    PERMISSIONS
        OWNER_WRITE OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        common
)
