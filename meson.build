# Build file for jitfs.

project('jitfs', 'cpp', 'c', version: '0.0.1',
        meson_version: '>= 0.38',
        default_options: [ 'buildtype=plain' ])

libfuse = dependency('fuse3')

add_global_arguments('-std=c++11', language : 'cpp')
add_global_link_arguments('-ldl', language: 'cpp')

includes = include_directories([
    './easyloggingpp/src',
    './sqlite',
])

sources = [
    'jitfs.cpp', 
    './easyloggingpp/src/easylogging++.cc',
    './sqlite/sqlite3.c'
]

executable(
    'jitfsd', 
    sources,
    include_directories: includes,
    dependencies : [libfuse],
)
