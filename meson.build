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

summary({
        'prefix' : prefix,
        'bindir' : bindir,
        'datadir' : datadir,
        'opt': get_option('optimization'),
        'debug': get_option('debug'),
        'buildtype': get_option('buildtype'),
})

if get_option('systemd').allowed() and systemd_dep.found()
        unit_dir = systemd_dep.get_variable(
                pkgconfig: 'systemdsystemunitdir',
                pkgconfig_define: ['prefix', prefix]
        )
        summary('unit', unit_dir)
endif

if get_option('dbus').allowed()
        dbus_service_dir = datadir / 'dbus-1' / 'system-services'
        # polkit_actions_dir = datadir / 'polkit-1' / 'actions'

        # The dbus-daemon doesn't seem to be configured to look for policies in
        # /usr/local/share/dbus-1/system.d in distros I've tested, so only use
        # the datadir if we think it's actually going to work; otherwise use
        # /etc.
        if prefix == '/usr'
                dbus_policy_dir = datadir / 'dbus-1' / 'system.d'
        else
                dbus_policy_dir = '/' / get_option('sysconfdir') / 'dbus-1' / 'system.d'
        endif

        summary({
                'dbus' : dbus_service_dir,
                'dbus policy' : dbus_policy_dir,
                # 'polkit': polkit_actions_dir,
        })
endif

apt_conf_dir = '/' / get_option('sysconfdir') / 'apt'
has_apt = import('fs').is_dir(apt_conf_dir)
# As of meson 1.1.0 this could be:
# > get_option('apt-hook').enable_auto_if(has_apt).allowed()
enable_apt_hook = get_option('apt-hook').auto() and has_apt
if get_option('apt-hook').enabled()
        enable_apt_hook = true
endif

if enable_apt_hook
        apt_hooks_dir = apt_conf_dir / 'apt.conf.d'
        summary('apt hooks', apt_hooks_dir)
endif

# Configuration data -----------------------------------------------------------

bus_name = 'ca.kamacite.BootEnvironments1'
conf = configuration_data({'bindir' : bindir, 'bus_name' : bus_name})

# Subdirectories ---------------------------------------------------------------

subdir('data')
subdir('src')
