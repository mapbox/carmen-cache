{
  'includes': [ 'common.gypi' ],
  'targets': [
    {
      'target_name': 'make_index_pb',
      'type': 'none',
      'hard_dependency': 1,
      'actions': [
        {
          'action_name': 'run_protoc',
          'inputs': [
            './proto/index.proto'
          ],
          'outputs': [
            "<(SHARED_INTERMEDIATE_DIR)/index.pb.cc"
          ],
          'action': ['protoc','-Iproto/','--cpp_out=<(SHARED_INTERMEDIATE_DIR)/','./proto/index.proto']
        }
      ]
    },
    {
      "target_name": "index_pb",
      'dependencies': [ 'make_index_pb' ],
      'hard_dependency': 1,
      "type": "static_library",
      "sources": [
        "<(SHARED_INTERMEDIATE_DIR)/index.pb.cc"
      ],
      'include_dirs': [
        '<(SHARED_INTERMEDIATE_DIR)/'
      ],
      'cflags_cc!': ['-fno-rtti', '-fno-exceptions'],
      'cflags_cc' : [
            '<!@(pkg-config protobuf --cflags)',
            '-std=c++11',
            '-D_THREAD_SAFE',
            '-Wno-sign-compare'
      ],
      'xcode_settings': {
        'OTHER_CPLUSPLUSFLAGS':[
            '<!@(pkg-config protobuf --cflags)',
            '-D_THREAD_SAFE',
            '-Wno-sign-compare'
        ],
        'GCC_ENABLE_CPP_RTTI': 'YES',
        'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
        'MACOSX_DEPLOYMENT_TARGET':'10.8',
        'CLANG_CXX_LIBRARY': 'libc++',
        'CLANG_CXX_LANGUAGE_STANDARD':'c++11',
        'GCC_VERSION': 'com.apple.compilers.llvm.clang.1_0'
      },
      'direct_dependent_settings': {
        'include_dirs': [
          '<(SHARED_INTERMEDIATE_DIR)/'
        ],
        'cflags_cc' : [
            '-D_THREAD_SAFE',
            '<!@(pkg-config protobuf --cflags)',
        ],
        'libraries':[
            '<!@(pkg-config protobuf --libs-only-L)',
            '-lprotobuf-lite'
        ],
        'xcode_settings': {
          'OTHER_CPLUSPLUSFLAGS':[
             '-D_THREAD_SAFE',
             '<!@(pkg-config protobuf --cflags)',
          ],
        },
      },
      'conditions': [
        ['OS=="win"',
          {
            'include_dirs':[
              '<!@(mapnik-config --includes)'
            ],
            'libraries': [
              'libprotobuf-lite.lib'
            ]
          },
          {
            'libraries':[
              '-lprotobuf-lite'
            ],
          }
        ]
      ]
    },
    {
      'target_name': '<(module_name)',
      'dependencies': [ 'index_pb' ],
      'sources': [
        "./src/binding.cpp"
      ],
      "include_dirs" : [
          'src/',
          'deps/',
          '<(SHARED_INTERMEDIATE_DIR)/',
          "<!(node -p -e \"require('path').dirname(require.resolve('nan'))\")"
      ],
      'cflags_cc!': ['-fno-rtti', '-fno-exceptions'],
      'cflags_cc' : [
          '-std=c++11',
          '-Wconversion'
      ],
      'xcode_settings': {
        'OTHER_CPLUSPLUSFLAGS':[
           '-Wshadow',
           '-Wconversion'
        ],
        'GCC_ENABLE_CPP_RTTI': 'YES',
        'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
        'MACOSX_DEPLOYMENT_TARGET':'10.8',
        'CLANG_CXX_LIBRARY': 'libc++',
        'CLANG_CXX_LANGUAGE_STANDARD':'c++11',
        'GCC_VERSION': 'com.apple.compilers.llvm.clang.1_0'
      }
    },
    {
      'target_name': 'action_after_build',
      'type': 'none',
      'dependencies': [ '<(module_name)' ],
      'copies': [
          {
            'files': [ '<(PRODUCT_DIR)/<(module_name).node' ],
            'destination': '<(module_path)'
          }
      ]
    }
  ]
}
