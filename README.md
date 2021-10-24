# RuuviTag-MQTT

This small doodad reads data from RuuviTag bluetooth sensors and pushes the parsed data to an MQTT instance. It is not very featureful nor suited for general use, but works for my needs.

I think it requires Bluez 5.50 or newer. At least it doesn't work on 5.43 due to missing `AddressType` adapter property in Bluez D-Bus interface.

I run it on a Raspberry pi, so cross compilation helpers are included.
