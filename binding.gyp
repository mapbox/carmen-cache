{
  'includes': [ 'common.gypi' ],
  'targets': [
    {
      'target_name': 'action_before_build',
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
      'target_name': '<(module_name)',
      'dependencies': [ 'action_before_build' ],
      'sources': [
        "./src/binding.cpp",
        "<(SHARED_INTERMEDIATE_DIR)/index.pb.cc"
      ],
      "include_dirs" : [
          'src/',
          'deps/',
          '<(SHARED_INTERMEDIATE_DIR)/',
          "<!(node -p -e \"require('path').dirname(require.resolve('nan'))\")"
      ],
      'libraries':[
          '<!@(pkg-config protobuf --libs-only-L)',
          '-lprotobuf-lite'
      ],
      'cflags_cc!': ['-fno-rtti', '-fno-exceptions'],
      'cflags_cc' : [
          '-std=c++11',
          '<!@(pkg-config protobuf --cflags)'
      ],
      'xcode_settings': {
        'GCC_ENABLE_CPP_RTTI': 'YES',
        'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
        'OTHER_CPLUSPLUSFLAGS':[
           '<!@(pkg-config protobuf --cflags)',
           '-Wshadow',
           '-std=c++11',
           '-stdlib=libc++'
        ],
        'OTHER_LDFLAGS':['-stdlib=libc++'],
        'CLANG_CXX_LANGUAGE_STANDARD':'c++11',
        'MACOSX_DEPLOYMENT_TARGET':'10.7'
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
