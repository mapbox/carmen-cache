{
    'includes': [ 'common.gypi' ],
    'make_global_settings': [
        ['CXX', '<(module_root_dir)/mason_packages/.link/bin/clang++-3.9'],
        ['LINK', '<(module_root_dir)/mason_packages/.link/bin/clang++-3.9']
    ],
    "variables": {
        # Flags we pass to the compiler to ensure the compiler
        # warns us about potentially buggy or dangerous code
        "compiler_checks": [
            '-Wall',
            '-Wextra',
            '-Weffc++',
            '-Wconversion',
            '-pedantic',
            '-Wconversion',
            '-Wshadow',
            '-Wfloat-equal',
            '-Wuninitialized',
            '-Wunreachable-code',
            '-Wold-style-cast',
            '-Wno-error=unused-variable',
            '-Wno-error=unused-value'
        ],
    },
    'targets': [
        {
            'target_name': 'action_before_build',
            'type': 'none',
            'hard_dependency': 1,
            'actions': [
                {
                    'action_name': 'install_deps',
                    'inputs': ['./scripts/install_deps.sh'],
                    'outputs': ['./mason_packages'],
                    'action': ['./scripts/install_deps.sh']
                }
            ]
        },
        {
            'target_name': '<(module_name)',
            'dependencies': [ 'action_before_build' ],
            'product_dir': '<(module_path)',
            'sources': [
                "./src/cpp_util.cpp",
                "./src/node_util.cpp",
                "./src/normalizationcache.cpp",
                "./src/memorycache.cpp",
                "./src/rocksdbcache.cpp",
                "./src/coalesce.cpp",
                "./src/binding.cpp"
            ],
            "include_dirs" : [
                'src/',
                '<!(node -e \'require("nan")\')',
                './mason_packages/.link/include/'
            ],
            "libraries": [
                '<(module_root_dir)/mason_packages/.link/lib/librocksdb.a',
                '<(module_root_dir)/mason_packages/.link/lib/libbz2.a'
            ],
            'cflags_cc!': ['-fno-rtti', '-fno-exceptions'],
            'cflags_cc' : [
                '<@(compiler_checks)'
            ],
            'ldflags': [
                '-Wl,-z,now',
            ],
            'xcode_settings': {
                'OTHER_LDFLAGS':[
                    '-Wl,-bind_at_load'
                ],
                'OTHER_CPLUSPLUSFLAGS':[
                    '<@(compiler_checks)'
                ],
                'GCC_ENABLE_CPP_RTTI': 'YES',
                'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
                'MACOSX_DEPLOYMENT_TARGET':'10.8',
                'CLANG_CXX_LIBRARY': 'libc++',
                'CLANG_CXX_LANGUAGE_STANDARD':'c++14',
                'GCC_VERSION': 'com.apple.compilers.llvm.clang.1_0'
            }
        }
    ]
}
