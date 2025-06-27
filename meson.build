# -*- meson-indent-basic: 8 -*-

project('beadm',
        'rust',
        version : '0.1.0',
        meson_version : '>=0.60.0',
        default_options : ['warning_level=2'])

prefix = get_option('prefix')
bindir = prefix / get_option('bindir')
profile = get_option('profile')

cargo = find_program('cargo', required : true)
libzfs_dep = dependency('libzfs', required : true)

subdir('src')
