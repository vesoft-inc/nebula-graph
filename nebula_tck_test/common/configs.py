# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

all_configs = {'--address'        : ['address', '', 'Address of the Nebula'],
               '--rm_dir': ['rm_dir', 'true', 'Remove the temp test dir'],
               '--debug_log'      : ['debug_log', 'true', 'Set nebula service --v=4']}


def init_configs():
    from optparse import OptionParser
    opt_parser = OptionParser()
    for config in all_configs:
        opt_parser.add_option(config,
                              dest = all_configs[config][0],
                              default = all_configs[config][1],
                              help = all_configs[config][2])
    return opt_parser

