# -*- meson-indent-basic: 8 -*-

# Note: this is just for convenience and compatibility with cargo. This would
# normally be in the build root instead.
target_dir = meson.project_source_root() / 'target'

cargo_flags = [
        '--manifest-path', meson.project_source_root() / 'Cargo.toml',
        '--target-dir', target_dir,
]

if profile == 'release'
        cargo_flags += ['--release']
endif

beadm_bin = custom_target(
        'beadm-bin',
        output : 'beadm-bin',
        build_always_stale : true,
        command : [cargo, 'build', cargo_flags])

beadm = custom_target(
        'beadm',
        output : 'beadm',
        depends : beadm_bin,
        build_always_stale : true,
        # Note this is required until cargo stabilises --artifact-dir.
        command : ['cp', target_dir / profile / 'beadm', '@OUTPUT@'],
        install : true,
        install_dir : bindir,
        install_tag : 'bin')

test('cargo', cargo, args: ['test', cargo_flags])
