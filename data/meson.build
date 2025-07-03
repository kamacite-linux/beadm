# -*- meson-indent-basic: 8 -*-

if get_option('systemd').allowed()
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
else
        # Generic D-Bus service
        dbus_service = configure_file(
                configuration : conf,
                input : 'dbus-service.in',
                output : bus_name + '.service')
endif

install_data(dbus_service, install_dir : dbus_service_dir)

dbus_policy = configure_file(
        configuration : conf,
        input : 'dbus-policy.conf.in',
        output : 'beadm.conf')

install_data(dbus_policy, install_dir : dbus_policy_dir)

# install_data('ca.kamacite.BootEnvironments1.policy',
#              install_dir : polkit_actions_dir)
