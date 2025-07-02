# -*- meson-indent-basic: 8 -*-

project('beadm',
        'rust',
        version : '0.1.0',
        meson_version : '>=0.60.0',
        default_options : ['warning_level=2'])

# Dependencies -----------------------------------------------------------------

cargo = find_program('cargo', required : true)
libzfs_dep = dependency('libzfs', required : true)
systemd_dep = dependency('systemd', version : '>= 242', required : get_option('systemd'))

# Install directories ----------------------------------------------------------

prefix = get_option('prefix')
bindir = prefix / get_option('bindir')
datadir = prefix / get_option('datadir')
dbus_service_dir = datadir / 'dbus-1' / 'services'

summary({
        'prefix' : prefix,
        'bindir' : bindir,
        'datadir' : datadir,
        'dbus' : dbus_service_dir,
})

if get_option('systemd').allowed() and systemd_dep.found()
        unit_dir = systemd_dep.get_variable(
                pkgconfig: 'systemdsystemunitdir',
                pkgconfig_define: ['prefix', prefix]
        )
        summary('unit', unit_dir)
endif

# Configuration data -----------------------------------------------------------

profile = get_option('profile')
bus_name = 'ca.kamacite.BootEnvironments1'
conf = configuration_data({'bindir' : bindir, 'bus_name' : bus_name})

# Subdirectories ---------------------------------------------------------------

subdir('data')
subdir('src')
