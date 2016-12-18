{
  'includes': [ 'common.gypi' ],
  'targets': [
    {
      'target_name': 'action_before_build',
      'type': 'none',
      'hard_dependency': 1,
      'actions': [
        {
          'action_name': 'install_mason',
          'inputs': ['./install_mason.sh'],
          'outputs': ['./mason_packages'],
          'action': ['./install_mason.sh']
        }
      ]
    },
    {
      'target_name': '<(module_name)',
      'dependencies': [ 'action_before_build' ],
      'product_dir': '<(module_path)',
      'sources': [
        "./src/binding.cpp"
      ],
      "include_dirs" : [
          'src/',
          '<!(node -e \'require("nan")\')',
          '<!(node -e \'require("protozero")\')',
          './mason_packages/.link/include/'
      ],
      "libraries": [
        '<(module_root_dir)/mason_packages/.link/lib/librocksdb.a',
        '<(module_root_dir)/mason_packages/.link/lib/libbz2.a'
      ],
      'cflags_cc!': ['-fno-rtti', '-fno-exceptions'],
      'cflags_cc' : [
          '-std=c++11',
          '-Wconversion'
      ],
      'ldflags': [
        '-Wl,-z,now',
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
    }
  ]
}
