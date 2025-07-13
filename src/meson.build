# -*- meson-indent-basic: 8 -*-

sources = files(
        'be/mock.rs',
        'be/mod.rs',
        'be/validation.rs',
        'be/zfs.rs',
        'dbus.rs',
        'main.rs',
)

# Note: this is just for convenience and compatibility with cargo. This would
# normally be in the build root instead.
target_dir = meson.project_build_root() / 'target'

cargo_flags = [
        '--manifest-path', meson.project_source_root() / 'Cargo.toml',
        '--target-dir', target_dir,
]

# Handle feature flags.
if not get_option('dbus').allowed() or get_option('dbus').disabled()
        cargo_flags += ['--no-default-features']
endif

# Partially translate meson's buildtype to cargo flags.
profile = 'debug'
if get_option('buildtype') == 'release'
        cargo_flags += ['--release']
        profile = 'release'
elif get_option('buildtype') == 'plain'
        # This is debug=false and opt-level=0.
        cargo_flags += ['--config', 'profile.dev.debug=false']
elif get_option('buildtype') == 'debugoptimized'
        # This is debug=false and opt-level=2.
        cargo_flags += ['--config', 'profile.dev.opt-level=2']
elif get_option('buildtype') != 'debug'
        error('Unsupported build type: ' + get_option('buildtype'))
endif

cargo_env = []

custom_target(
        'beadm',
        output : 'beadm',
        input : sources,
        build_by_default : true,
        console : true,
        # Note this is required until cargo stabilises --artifact-dir.
        command : [
                'env',
                cargo_env,
                cargo, 'build',
                cargo_flags,
                '&&',
                'cp', target_dir / profile / 'beadm', '@OUTPUT@',
        ],
        install : true,
        install_dir : bindir,
        install_tag : 'bin')

test('cargo', cargo, args: ['test', cargo_flags])
