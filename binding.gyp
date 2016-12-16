{
  'includes': [ 'common.gypi' ],
  'targets': [
    {
      'target_name': '<(module_name)',
      'sources': [
        "./src/binding.cpp"
      ],
      "include_dirs" : [
          'src/',
          'deps/',
          '<(SHARED_INTERMEDIATE_DIR)/',
          "<!(node -p -e \"require('path').dirname(require.resolve('nan'))\")",
          './node_modules/protozero/include/',
          './mason_packages/.link/include/'
      ],
      "libraries": [
        '<(module_root_dir)/mason_packages/.link/lib/librocksdb.a',
        '<(module_root_dir)/mason_packages/.link/lib/libbz2.a'
      ],
      'ldflags': [
        '-Wl,-z,now',
      ],
      'cflags_cc!': ['-fno-rtti', '-fno-exceptions'],
      'cflags_cc' : [
          '-std=c++11',
          '-Wconversion'
      ],
      'xcode_settings': {
        'OTHER_LDFLAGS':[
          '-Wl,-bind_at_load'
        ],
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
