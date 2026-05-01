#!/usr/bin/env bash
set -euo pipefail

# Allow only the detected LAN subnets to reach the R-PhysGen-DB static service.
# Run with sudo/root on the host that serves deploy/lan.
ufw allow from 10.1.0.0/22 to any port 8088 proto tcp comment 'R-PhysGen-DB LAN eno2'
ufw allow from 192.168.8.0/24 to any port 8088 proto tcp comment 'R-PhysGen-DB LAN WiFi'
ufw reload
ufw status numbered
