#!/usr/bin/env bash
# Configure LinuxPTP (ptp_kvm + phc2sys) on a FABRIC KVM VM (per fabric-testbed/ptp).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

sudo sed -i '/192\.168\.122\.1/d; /0\.0\.0\.0/d' /etc/resolv.conf 2>/dev/null || true
if ! ip -4 route show default 2>/dev/null | grep -q default; then
    sudo sed -i 's/^mirrorlist=/#mirrorlist=/g; s|^#baseurl=http://dl.rockylinux.org|baseurl=https://dl.rockylinux.org|g' /etc/yum.repos.d/Rocky-*.repo 2>/dev/null || true
fi

if ! command -v phc2sys &>/dev/null; then
    sudo dnf install -y --disablerepo=docker*,epel* --setopt=timeout=5 --setopt=retries=2 linuxptp >/dev/null
fi

sudo systemctl stop chronyd 2>/dev/null || true
sudo systemctl disable chronyd 2>/dev/null || true
echo ptp_kvm | sudo tee /etc/modules-load.d/ptp_kvm.conf >/dev/null
sudo modprobe ptp_kvm
sudo udevadm settle
sudo cp "${SCRIPT_DIR}/phc2sys@.service" /lib/systemd/system/phc2sys@.service
sudo phc_ctl CLOCK_REALTIME set "$(sudo phc_ctl /dev/ptp_kvm get | awk '{print $5}')" >/dev/null 2>&1 || true
sudo systemctl daemon-reload
sudo systemctl enable --now phc2sys@CLOCK_REALTIME.service >/dev/null 2>&1
sudo systemctl restart phc2sys@CLOCK_REALTIME.service
sleep 1
sudo phc_ctl /dev/ptp_kvm cmp | grep offset
