# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


class Column:
    def __init__(self, index: int):
        self.index = index

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, index: int):
        if index < 0:
            raise ValueError(f"Invalid index of vid: {index}")
        self._index = index


class VID(Column):
    def __init__(self, index: int, vtype: str):
        super().__init__(index)
        self.id_type = vtype

    @property
    def id_type(self):
        return self._type

    @id_type.setter
    def id_type(self, vtype: str):
        if vtype not in ['int', 'string']:
            raise ValueError(f'Invalid vid type: {vtype}')
        self._type = vtype


class Prop(Column):
    def __init__(self, name: str, index: int, ptype: str):
        super().__init__(index)
        self.name = name
        self.ptype = ptype

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def ptype(self):
        return self._type

    @ptype.setter
    def ptype(self, ptype: str):
        if ptype not in ['string', 'int']:
            raise ValueError(f'Invalid prop type: {ptype}')
        self._type = ptype


class Properties:
    def __init__(self, name: str, props: list):
        self.name = name
        self.props = props

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def props(self):
        return self._props

    @props.setter
    def props(self, props: list):
        for prop in props:
            if not isinstance(prop, Prop):
                raise ValueError(f"Invalid prop type in tag: {prop}")
        self._props = props


class Tag(Properties):
    def __init__(self, name, props):
        super().__init__(name, props)


class Edge(Properties):
    def __init__(self,
                 name: str,
                 src: VID,
                 dst: VID,
                 rank: int,
                 props: list):
        super().__init__(name, props)
        self.src = src
        self.dst = dst
        self.rank = rank

    @property
    def src(self):
        return self._src

    @src.setter
    def src(self, src: VID):
        self._src = src

    @property
    def dst(self):
        return self._dst

    @dst.setter
    def dst(self, dst: VID):
        self._dst = dst

    @property
    def rank(self):
        return self._rank

    @rank.setter
    def rank(self, rank: VID):
        self._rank = rank


class Vertex:
    def __init__(self, vid: VID, tags: list):
        self.vid = vid
        self.tags = tags

    @property
    def vid(self):
        return self._vid

    @vid.setter
    def vid(self, vid: VID):
        self._vid = vid

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags: list):
        for tag in tags:
            if not isinstance(tag, Tag):
                raise ValueError(f'Invalid tag type of vertex: {tag}')
        self._tags = tags
