# -*- meson-indent-basic: 8 -*-

if enable_systemd
        # "Clear boot once" oneshot service
        clear_boot_once_service = configure_file(
                configuration : conf,
                input : 'beadm-clear-boot-once.service.in',
                output : '@BASENAME@')

        install_data(clear_boot_once_service, install_dir : unit_dir)
elif enable_dinit
        # "Clear boot once" dinit service
        clear_boot_once_service = configure_file(
                configuration : conf,
                input : 'beadm-clear-boot-once-dinit.in',
                output : 'beadm-clear-boot-once')

        install_data(clear_boot_once_service, install_dir : service_dir)
endif

if enable_systemd and get_option('dbus').allowed()
        # Unit file
        systemd_service = configure_file(
                configuration : conf,
                input : 'beadm.service.in',
                output : '@BASENAME@')

        install_data(systemd_service, install_dir : unit_dir)

        # systemd-specific D-Bus service
        dbus_service = configure_file(
                configuration : conf,
                input : 'dbus-service-systemd.in',
                output : bus_name + '.service')

        install_data(dbus_service, install_dir : dbus_service_dir)
elif get_option('dbus').allowed()
        # Generic D-Bus service
        dbus_service = configure_file(
                configuration : conf,
                input : 'dbus-service.in',
                output : bus_name + '.service')

        install_data(dbus_service, install_dir : dbus_service_dir)
endif

# D-Bus policy file.
if get_option('dbus').allowed()
        dbus_policy = configure_file(
                configuration : conf,
                input : 'dbus-policy.conf.in',
                output : 'beadm.conf')

        install_data(dbus_policy, install_dir : dbus_policy_dir)
endif

# APT hook configuration.
if enable_apt_hook
        apt_hook_conf = configure_file(
                configuration : conf,
                input : '90-boot-environments.conf.in',
                output : '@BASENAME@')

        install_data(apt_hook_conf, install_dir : apt_hooks_dir)
endif

# install_data('ca.kamacite.BootEnvironments1.policy',
#              install_dir : polkit_actions_dir)
