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

if profile == 'release'
        cargo_flags += ['--release']
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
