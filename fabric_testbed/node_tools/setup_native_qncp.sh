#!/usr/bin/env bash
# Set up a native Python 3.11 virtualenv and install QNCP repositories from Git
# (or from pre-uploaded source directories in ~/qncp/src/).
#
# Usage:
#   setup_native_qncp.sh <role: controller|agent> [git_org] [mq_branch] [plugins_branch] [role_branch]
set -euo pipefail

ROLE="${1:-agent}"
GIT_ORG="${2:-youf3}"
MQ_BRANCH="${3:-ethernet_probe_emulation}"
PLUGINS_BRANCH="${4:-ethernet_probe_emulation}"
ROLE_BRANCH="${5:-ethernet_probe_emulation}"

QNCP_ROOT="/home/rocky/qncp"
SRC_DIR="${QNCP_ROOT}/src"
VENV_DIR="${QNCP_ROOT}/.venv"
LOG_DIR="${QNCP_ROOT}/logs"

# 0. Remove unreachable IPv4 nameservers and use dual-stack Rocky CDN on IPv6-only management VMs
if ! ip -4 route show default 2>/dev/null | grep -q default; then
    sudo sed -i '/192\.168\.122\.1/d; /0\.0\.0\.0/d' /etc/resolv.conf 2>/dev/null || true
    sudo sed -i 's/^mirrorlist=/#mirrorlist=/g; s|^#baseurl=http://dl.rockylinux.org|baseurl=https://dl.rockylinux.org|g' /etc/yum.repos.d/Rocky-*.repo 2>/dev/null || true
fi

# 1. Disable firewalld so FABNetv6 UDP probe ports (15000-25000) and MQTT (1883) are open
sudo systemctl stop firewalld 2>/dev/null || true
sudo systemctl disable firewalld 2>/dev/null || true
sudo loginctl enable-linger rocky 2>/dev/null || true

# 2. Install system packages (Python 3.11, git, tmux, gcc, linuxptp)
DNF_OPTS="--disablerepo=docker*,epel* --setopt=timeout=5 --setopt=retries=2 --setopt=minrate=1000"
if ! command -v python3.11 &>/dev/null || ! command -v tmux &>/dev/null || ! command -v gcc &>/dev/null || ! command -v phc2sys &>/dev/null; then
    sudo dnf install -y ${DNF_OPTS} git tmux gcc linuxptp python3.11 python3.11-pip python3.11-devel 2>/dev/null || \
        sudo dnf install -y ${DNF_OPTS} git tmux gcc linuxptp python39 python39-pip python39-devel
fi

# 2b. Configure LinuxPTP (ptp_kvm + phc2sys) to synchronize CLOCK_REALTIME with KVM host PTP clock
if command -v phc2sys &>/dev/null; then
    sudo systemctl stop chronyd 2>/dev/null || true
    sudo systemctl disable chronyd 2>/dev/null || true
    echo ptp_kvm | sudo tee /etc/modules-load.d/ptp_kvm.conf >/dev/null
    sudo modprobe ptp_kvm 2>/dev/null || true
    sudo udevadm settle 2>/dev/null || true
    sudo tee /lib/systemd/system/phc2sys@.service >/dev/null << 'EOF'
[Unit]
Description=Synchronize system clock or PTP hardware clock (PHC)
Documentation=man:phc2sys

[Service]
Type=simple
ExecStartPre=/bin/bash -c "/bin/systemctl set-environment PTP_KVM_DEV=$(readlink /dev/ptp_kvm)"
ExecStart=/usr/sbin/phc2sys -u 8 -E linreg -O +0 -s /dev/${PTP_KVM_DEV} -c %i

[Install]
WantedBy=multi-user.target
EOF
    sudo phc_ctl CLOCK_REALTIME set $(sudo phc_ctl /dev/ptp_kvm get | awk '{print $5}') >/dev/null 2>&1 || true
    sudo systemctl daemon-reload
    sudo systemctl enable --now phc2sys@CLOCK_REALTIME.service >/dev/null 2>&1 || true
fi

PY_BIN="python3.11"
if ! command -v "${PY_BIN}" &>/dev/null; then
    PY_BIN="python3.9"
fi

mkdir -p "${SRC_DIR}" "${LOG_DIR}"

# 3. Create virtualenv and install base build dependencies
if [[ ! -d "${VENV_DIR}" ]]; then
    "${PY_BIN}" -m venv "${VENV_DIR}"
fi

PIP="${VENV_DIR}/bin/pip"
"${PIP}" install --upgrade pip setuptools wheel
"${PIP}" install pluggy==1.0.0 readme-renderer==37.3 docutils==0.19

clone_or_update() {
    local repo="$1"
    local branch="$2"
    local target="${SRC_DIR}/${repo}"
    if [[ -d "${target}/.git" ]]; then
        git -C "${target}" fetch origin "${branch}" && git -C "${target}" checkout "${branch}" && git -C "${target}" pull --ff-only origin "${branch}" || true
    elif [[ -d "${target}" && -n "$(ls -A "${target}" 2>/dev/null)" ]]; then
        echo "Using pre-uploaded source directory ${target}"
    else
        git clone --branch "${branch}" "https://github.com/${GIT_ORG}/${repo}.git" "${target}"
    fi
}

# 4. Clone / verify shared repositories
clone_or_update "qn-mq" "${MQ_BRANCH}"
clone_or_update "qn-plugins" "${PLUGINS_BRANCH}"
"${PIP}" install -e "${SRC_DIR}/qn-mq"

# 5. Clone / verify role-specific repository
if [[ "${ROLE}" == "controller" ]]; then
    clone_or_update "qn-server" "${ROLE_BRANCH}"
    "${PIP}" install -e "${SRC_DIR}/qn-server"
    clone_or_update "qn-api" "${ROLE_BRANCH}"
    if [[ -d "${SRC_DIR}/qn-api" ]]; then
        "${PIP}" install -e "${SRC_DIR}/qn-api" 'bcrypt<4.1.0'
        if [[ ! -f "${SRC_DIR}/qn-api/static/js/dist/bundle.min.js" ]]; then
            if ! command -v npm &>/dev/null; then
                sudo dnf module install -y ${DNF_OPTS} nodejs:18/common 2>/dev/null || \
                    sudo dnf install -y ${DNF_OPTS} nodejs npm 2>/dev/null || true
            fi
            if command -v npm &>/dev/null; then
                (cd "${SRC_DIR}/qn-api" && npm install && npm run build)
            fi
        fi
    fi
else
    clone_or_update "qn-agent" "${ROLE_BRANCH}"
    "${PIP}" install -e "${SRC_DIR}/qn-agent"
fi

echo "Native QNCP environment ready for role=${ROLE} in ${VENV_DIR}"
