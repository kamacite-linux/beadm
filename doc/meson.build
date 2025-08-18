# -*- meson-indent-basic: 8 -*-

# scdoc expects standard input, so we have to use a shell here.
sh = find_program('sh', native : true)
custom_target(
        'beadm.8',
        input : files('beadm.8.scd'),
        output : 'beadm.8',
        command : [
                sh, '-c', '@0@ < @INPUT@'.format(scdoc.full_path())
        ],
        capture : true,
        install : true,
        install_dir : mandir / 'man8')
