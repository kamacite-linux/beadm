# -*- meson-indent-basic: 8 -*-

if get_option('systemd').allowed()
        # "Clear boot once" oneshot service
        clear_boot_once_service = configure_file(
                configuration : conf,
                input : 'beadm-clear-boot-once.service.in',
                output : '@BASENAME@')

        install_data(clear_boot_once_service, install_dir : unit_dir)
endif

if get_option('systemd').allowed() and get_option('dbus').allowed()
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

# install_data('ca.kamacite.BootEnvironments1.policy',
#              install_dir : polkit_actions_dir)
