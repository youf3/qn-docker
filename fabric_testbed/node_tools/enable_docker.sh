#!/bin/bash
# Enable Docker daemon on FABRIC docker_rocky_8 VMs.
# Usage: enable_docker.sh <image_type>
# For docker_rocky_8, this just starts the daemon and adds the user to the docker group.
image=$1

if [[ $image == "docker_rocky_8" ]]; then
    # Remove unreachable IPv4 nameservers on IPv6-only management VMs to prevent 5s DNS timeouts
    if ! ip -4 route show default 2>/dev/null | grep -q default; then
        sudo sed -i '/192\.168\.122\.1/d; /0\.0\.0\.0/d' /etc/resolv.conf 2>/dev/null || true
    fi
    sudo systemctl stop firewalld 2>/dev/null || true
    sudo systemctl disable firewalld 2>/dev/null || true
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -aG docker rocky
    # Install docker-compose plugin if not present
    if ! sudo docker compose version &>/dev/null; then
        sudo dnf install -y docker-compose-plugin 2>/dev/null || true
    fi
else
    echo "Unsupported image type: $image"
    exit 1
fi
