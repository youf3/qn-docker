#!/usr/bin/env python3
# fablib.config
"""Deploy a distributed QNCP full-mesh topology on FABRIC testbed.

Creates a FABRIC slice with:
  - 1 Controller VM (Docker: mosquitto + mongo; Native tmux: quantnet_controller + qn-api)
  - N Agent VMs (one per quantum node: QPUs + BSMs; Native tmux: quantnet_agent)
  - 1 FABNetv6 L3 network slice connecting a dedicated NIC_Basic on every VM
    for MQTT control traffic and emulated Ethernet quantum probes
    (DummyLightSrc <-> DummySignalMeasurement).

Prerequisites:
  - FABRIC account with bastion + sliver SSH keys configured
  - FABlib configured (~/.fabric/fabric_rc or environment variables)
  - pip install fabrictestbed-extensions
  - Install qn-config-builder from GitHub or local checkout (not on PyPI):
      pip install git+https://github.com/youf3/qn-config-builder.git
      # or: pip install -e ../../qn-config-builder
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import socket
import sys
import tarfile
import tempfile
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import networkx as _nx
import yaml
from networkx.readwrite import graphml as _graphml_module

try:
    from qn_config_builder.builder import build_deployment
    from qn_config_builder.models import Descriptor, NodeSpec, NodeType
    from qn_config_builder.topology import get_builder
except ImportError:
    print(
        "ERROR: qn-config-builder is not installed (not published on PyPI).\n"
        "  pip install git+https://github.com/youf3/qn-config-builder.git\n"
        "  # or: pip install -e ../../qn-config-builder",
        file=sys.stderr,
    )
    sys.exit(1)

# Patch networkx GraphML serializer so dict attributes are serialized as JSON strings
_original_generate_graphml = _graphml_module.generate_graphml


def _patched_generate_graphml(G, prettyprint=True, version="1.1"):
    G_copy = G.copy()
    for _, data in G_copy.nodes(data=True):
        for k, v in data.items():
            if isinstance(v, dict):
                data[k] = json.dumps(v)
    for _, _, data in G_copy.edges(data=True):
        for k, v in data.items():
            if isinstance(v, dict):
                data[k] = json.dumps(v)
    for k, v in G_copy.graph.items():
        if isinstance(v, dict):
            G_copy.graph[k] = json.dumps(v)
    return _original_generate_graphml(G_copy, prettyprint=prettyprint)


_graphml_module.generate_graphml = _patched_generate_graphml
_nx.generate_graphml = _patched_generate_graphml

try:
    from fabrictestbed_extensions.fablib.fablib import FablibManager as fablib_manager
except ImportError:
    print(
        "ERROR: fabrictestbed-extensions is not installed.\n"
        "  pip install fabrictestbed-extensions\n"
        "See https://learn.fabric-testbed.net/ for FABRIC account setup.",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
def _resolve_workspace_dir(script_dir: Path) -> Path:
    """Locate the parent Quant-Net workspace containing sibling qn-* repos."""
    env_ws = os.environ.get("QUANTNET_WORKSPACE")
    if env_ws:
        return Path(env_ws).expanduser().resolve()
    for candidate in script_dir.parents:
        if any((candidate / repo).is_dir() for repo in ("qn-mq", "qn-server", "qn-agent", "qn-plugins")):
            return candidate
    return script_dir.parent


SCRIPT_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = _resolve_workspace_dir(SCRIPT_DIR)
NODE_TOOLS_DIR = SCRIPT_DIR / "node_tools"
FABRIC_IMAGE = "docker_rocky_8"
CONTROLLER_DISK_GB = 10
AGENT_DISK_GB = 10
DOCKER_STARTUP_WAIT = 15
AGENT_STARTUP_DELAY = 5

# International partner sites whose FABNetv6 routers are isolated from the US 2602:fcfb::/40 core
NON_US_ISOLATED_SITES = {"CIEN", "CERN", "AMST", "TOKY", "BRIS"}

DEFAULT_GIT_ORG = "youf3"
DEFAULT_AGENT_BRANCH = "ethernet_probe_emulation"
DEFAULT_SERVER_BRANCH = "ethernet_probe_emulation"
DEFAULT_MQ_BRANCH = "ethernet_probe_emulation"
DEFAULT_PLUGINS_BRANCH = "ethernet_probe_emulation"
DEFAULT_PROBE_BASE_PORT = 15000


def _vm_name(node_id: str) -> str:
    """Convert a QNCP node ID (e.g. 'BSM-1_2') to its FABRIC VM name ('bsm-1-2')."""
    return node_id.lower().replace("_", "-")


def _load_fabric_rc_env() -> None:
    """Populate FABRIC_* environment variables from ~/.fabric/fabric_rc if not already set."""
    key_map = {
        "fabric_project_id": "FABRIC_PROJECT_ID",
        "fabric_bastion_username": "FABRIC_BASTION_USERNAME",
        "fabric_bastion_key_location": "FABRIC_BASTION_KEY_LOCATION",
        "fabric_slice_private_key_location": "FABRIC_SLICE_PRIVATE_KEY_FILE",
        "fabric_slice_public_key_location": "FABRIC_SLICE_PUBLIC_KEY_FILE",
        "token_location": "FABRIC_TOKEN_LOCATION",
    }
    rc_path = Path.home() / ".fabric" / "fabric_rc"
    if not rc_path.exists():
        return
    for line in rc_path.read_text().splitlines():
        if "=" not in line or line.strip().startswith(("#", "[")):
            continue
        k, v = line.split("=", 1)
        k_clean = k.strip().lower().removeprefix("export ").strip()
        v_clean = v.strip().strip("\"'")
        env_name = key_map.get(k_clean)
        if env_name and v_clean:
            os.environ.setdefault(env_name, v_clean)


def _resolve_reachable_bastion_host(host: str = "bastion.fabric-testbed.net", port: int = 22) -> str:
    """Resolve bastion hostname to a reachable IP (avoids Paramiko IPv6 timeout on IPv4-only networks)."""
    try:
        infos = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    except Exception:
        return host
    for af, socktype, proto, _, addr in infos:
        try:
            with socket.socket(af, socktype, proto) as s:
                s.settimeout(2.0)
                s.connect(addr)
                return str(addr[0])
        except Exception:
            continue
    return host


def _init_fablib():
    """Initialize FablibManager using stored credentials, refreshing or re-authenticating if needed."""
    _load_fabric_rc_env()
    bastion_host = _resolve_reachable_bastion_host(
        os.environ.get("FABRIC_BASTION_HOST", "bastion.fabric-testbed.net")
    )
    os.environ["FABRIC_BASTION_HOST"] = bastion_host
    fablib = fablib_manager(bastion_host=bastion_host)
    try:
        fablib.get_user_info()
    except Exception as exc:
        if "Token refresh failed" in str(exc) or "expired" in str(exc).lower():
            print(
                "Stored FABRIC token is expired and could not be refreshed automatically.\n"
                "Starting browser authentication via FABRIC Credential Manager..."
            )
            from fabrictestbed.external_api.credmgr_client import CredmgrClient

            token_file = os.environ.get("FABRIC_TOKEN_LOCATION", str(Path.home() / ".tokens.json"))
            cm = CredmgrClient("cm.fabric-testbed.net")
            cm.create_cli(
                project_id=os.environ.get("FABRIC_PROJECT_ID"),
                file_path=token_file,
                open_browser=True,
                timeout=1800,
            )
            fablib = fablib_manager(bastion_host=bastion_host)
        else:
            raise
    return fablib


# ═══════════════════════════════════════════════════════════════════════════
# Phase 1: Generate configs using qn-config-builder + FABNetv6 post-processing
# ═══════════════════════════════════════════════════════════════════════════


def generate_configs(
    num_qpus: int,
    image_tag: str,
    staging_dir: Path,
    controller_ip: str,
    fabnetv6_ips: dict[str, str] | None = None,
    probe_base_port: int = DEFAULT_PROBE_BASE_PORT,
) -> list[NodeSpec]:
    """Generate all QNCP config artifacts for distributed deployment."""
    descriptor = Descriptor(
        topology={"template": "full-mesh"},
        nodes={"qpu": {"count": num_qpus}},
        docker={"image_tag": image_tag},
        deployment={"mode": "distributed", "controller_address": controller_ip},
    )

    build_deployment(descriptor, staging_dir)
    nodes = get_builder(descriptor.topology.template).build(descriptor)

    if fabnetv6_ips is None:
        fabnetv6_ips = build_mock_fabnetv6_ips(nodes)

    configure_native_and_fabnetv6_bundles(
        staging_dir=staging_dir,
        nodes=nodes,
        controller_ip=controller_ip,
        fabnetv6_ips=fabnetv6_ips,
        probe_base_port=probe_base_port,
    )

    print(f"Generated configs for {len(nodes)} nodes in {staging_dir}")
    return nodes


def build_mock_fabnetv6_ips(nodes: list[NodeSpec]) -> dict[str, str]:
    """Build deterministic mock FABNetv6 IPv6 addresses for local config testing."""
    ips = {"controller": "2602:fcfb:000d:1::10"}
    for idx, node in enumerate(nodes, start=11):
        ips[node.id] = f"2602:fcfb:000d:1::{idx:x}"
    return ips


def configure_native_and_fabnetv6_bundles(
    staging_dir: Path,
    nodes: list[NodeSpec],
    controller_ip: str,
    fabnetv6_ips: dict[str, str],
    probe_base_port: int = DEFAULT_PROBE_BASE_PORT,
) -> None:
    """Rewrite generated controller and agent bundles for native execution + FABNetv6 probes."""
    (staging_dir / "fabnetv6_ips.json").write_text(json.dumps(fabnetv6_ips, indent=2) + "\n")

    # 1. Controller docker-compose.yml (mosquitto + mongo only) and dual-stack mosquitto.conf
    ctrl_compose = {
        "services": {
            "mosquitto": {
                "image": "eclipse-mosquitto:2.0",
                "container_name": "mosquitto",
                "restart": "always",
                "network_mode": "host",
                "ports": ["1883:1883", "8080:8080"],
                "volumes": ["./conf/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro"],
            },
            "mongo": {
                "image": "mongo:8.0",
                "container_name": "mongo",
                "restart": "always",
                "network_mode": "host",
                "ports": ["27017:27017"],
            },
        }
    }
    (staging_dir / "controller" / "docker-compose.yml").write_text(
        yaml.safe_dump(ctrl_compose, sort_keys=False)
    )
    (staging_dir / "controller" / "conf" / "mosquitto.conf").write_text(
        textwrap.dedent("""\
            listener 1883
            allow_anonymous true

            listener 8080
            protocol websockets
            allow_anonymous true
        """)
    )

    # 2. Controller quantnet.cfg for native execution
    (staging_dir / "controller" / "conf" / "quantnet.cfg").write_text(
        textwrap.dedent("""\
            [common]
            logdir = /home/rocky/qncp/logs
            loglevel = INFO

            [mq]
            rpc_server_topic=rpc/qn-server
            host=127.0.0.1
            port=1883
            ws_host=127.0.0.1
            ws_port=8080
            mongo_host=127.0.0.1
            mongo_port=27017

            [database]
            default = mongodb://127.0.0.1:27017
            schema = dev
            echo=0
            pool_recycle=3600
            pool_size=20
            max_overflow=20
            pool_reset_on_return=rollback

            [quantnet_api]
            base_url = http://127.0.0.1:8000

            [simulation]
            database_file_path=/home/rocky/qncp/conf/quant-sim-db.json
            mapping_file_path=/home/rocky/qncp/conf/mapping.yaml

            [schedule_manager]
            grace_period = 1000

            [plugins]
            path=/home/rocky/qncp/src/qn-plugins/plugins

            [schemas]
            path=/home/rocky/qncp/src/qn-plugins/plugins/schema

            [scheduling]
            name=BatchScheduler

            [routing]
            name=PathFinder

            [monitoring]
            name=Monitor
        """)
    )

    # 3. Per-agent conf/agent.cfg with FABNetv6 IPv6 probe configuration
    peer_ips_str = ",".join(
        f"{node.id}:{fabnetv6_ips[node.id]}" for node in nodes if node.id in fabnetv6_ips
    )

    for node in nodes:
        agent_cfg_path = staging_dir / "agents" / node.id / "conf" / "agent.cfg"
        node_ipv6 = fabnetv6_ips.get(node.id, "::1")

        tasks_block = ""
        if node.type == NodeType.QNODE:
            tasks_block = "\n[tasks]\n" + "\n".join(
                f"[[downhill_calibration{i}]]\n"
                f"path=/home/rocky/qncp/src/qn-agent/config/downhill_calibration{i}.json\n"
                for i in range(1, 5)
            )

        agent_cfg_content = textwrap.dedent(f"""\
            [common]
            logdir = /home/rocky/qncp/logs
            loglevel = INFO

            [agent]
            threads = 8

            [mq]
            host={controller_ip}
            port=1883

            [link]
            hello_interval = 10
            hold_time = 30

            [interpreters]
            path=/home/rocky/qncp/src/qn-plugins/plugins/pingpong/interpreter

            [schemas]
            path=/home/rocky/qncp/src/qn-plugins/plugins/schema

            [protocols]
            pingpong=pingpong.py

            [devices]
            [[exp_framework]]
            enabled=true
            type=Exp_Framework
            driver=DummyExpFramework

            [[lightsource]]
            enabled=true
            type=Light_Source
            driver=DummyLightSrc
            ip={node_ipv6}
            base_port={probe_base_port}
            peer_ips="{peer_ips_str}"

            [[signalmeasurement]]
            enabled=true
            type=Signal_Measurement
            driver=DummySignalMeasurement
            ip={node_ipv6}
            base_port={probe_base_port}
            window=2.0
            resolution=1e-7
            peer_ips="{peer_ips_str}"

            [[epc]]
            enabled=true
            type=Filter
            driver=DummyEPC

            [[polarimeter]]
            enabled=true
            type=Light_Measurement
            driver=DummyPolarimeter

            [[egp]]
            enabled=true
            type=EGP
            driver=SimEGPDriver
            protocol=EGProtocol

            [[messaging]]
            enabled=true
            type=Messaging
            driver=PassthroughDriver
        """) + tasks_block
        agent_cfg_path.write_text(agent_cfg_content)


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2: Create FABRIC slice with FABNetv6 L3 data-plane network
# ═══════════════════════════════════════════════════════════════════════════


def _add_qncp_vm(slice_, name: str, site: str, disk_gb: int):
    """Add a VM with a FABNetv6 interface and post-boot Docker setup to a FABRIC slice."""
    vm = slice_.add_node(
        name=name,
        site=site,
        cores=2,
        ram=8,
        disk=disk_gb,
        image=FABRIC_IMAGE,
    )
    vm.add_fabnet(name="FABNET", net_type="IPv6", nic_type="NIC_Basic")
    vm.add_post_boot_upload_directory(str(NODE_TOOLS_DIR), ".")
    vm.add_post_boot_execute(
        f"chmod +x node_tools/enable_docker.sh node_tools/setup_native_qncp.sh && "
        f"./node_tools/enable_docker.sh {FABRIC_IMAGE}"
    )
    return vm


def create_slice(
    slice_name: str,
    site: str | None,
    nodes: list[NodeSpec],
) -> tuple:
    """Create or reuse a FABRIC slice with 1 controller VM + N agent VMs + FABNetv6 network."""
    fablib = _init_fablib()

    try:
        existing_slice = fablib.get_slice(name=slice_name)
    except Exception:
        existing_slice = None

    if existing_slice is not None and existing_slice.get_state() in (
        "Nascent", "Configuring", "StableOK", "ModifyOK"
    ):
        existing_sites = sorted({n.get_site() for n in existing_slice.get_nodes() if n.get_site()})
        print(
            f"Reusing existing slice '{slice_name}' "
            f"(state: {existing_slice.get_state()}, sites: {existing_sites})..."
        )
        existing_slice.wait(timeout=1800, interval=15, progress=True)
        existing_slice.wait_ssh(timeout=1800, interval=15, progress=True)
        return fablib, existing_slice

    total_vms = 1 + len(nodes)
    if site is None or site == "random":
        available_sites = (
            fablib.list_sites(
                output="list",
                quiet=True,
                filter_function=lambda x: x.get("ptp_capable") is True and x.get("state") == "Active",
            )
            or []
        )
        viable_sites = [
            s["name"] for s in available_sites
            if s.get("state") == "Active"
            and s.get("ptp_capable") is True
            and s.get("name") not in NON_US_ISOLATED_SITES
            and (s.get("cores_available") or 0) >= 2
            and (s.get("ram_available") or 0) >= 8
            and (s.get("disk_available") or 0) >= 10
            and (s.get("nic_basic_available") or 0) >= 1
        ]
        if len(viable_sites) >= total_vms:
            chosen_sites = random.sample(viable_sites, k=total_vms)
        elif viable_sites:
            chosen_sites = [random.choice(viable_sites) for _ in range(total_vms)]
        else:
            chosen_sites = ["CLEM"] * total_vms
    else:
        chosen_sites = [str(site)] * total_vms

    ctrl_site, agent_sites = chosen_sites[0], chosen_sites[1:]
    site_map_str = f"controller@{ctrl_site}, " + ", ".join(
        f"{_vm_name(n.id)}@{s}" for n, s in zip(nodes, agent_sites)
    )
    print(f"Selected FABRIC sites: {site_map_str}")

    slice_ = fablib.new_slice(name=slice_name)
    _add_qncp_vm(slice_, "controller", ctrl_site, CONTROLLER_DISK_GB)
    for node, node_site in zip(nodes, agent_sites):
        _add_qncp_vm(slice_, _vm_name(node.id), node_site, AGENT_DISK_GB)

    print(
        f"Submitting slice '{slice_name}' with {total_vms} VMs + "
        f"FABNetv6 network across sites ({site_map_str})..."
    )
    slice_.submit()
    print(f"Slice '{slice_name}' is ready (state: {slice_.get_state()})")
    return fablib, slice_


def _extract_vm_fabnetv6_ip(vm) -> str:
    """Retrieve the FABNetv6 global IPv6 address assigned to a FABRIC VM."""
    try:
        iface = vm.get_interface(network_name=f"FABNET_IPv6_{vm.get_site()}")
        if iface and iface.get_ip_addr():
            return str(iface.get_ip_addr()).split("/", 1)[0].strip()
    except Exception:
        pass

    stdout, _ = vm.execute(
        "ip -6 -o addr show scope global | awk '{print $4}' | cut -d/ -f1",
        quiet=True,
    )
    candidates = [line.strip() for line in stdout.splitlines() if line.strip()]
    for cand in candidates:
        if cand.lower().startswith("2602:fcfb"):
            return cand
    return candidates[-1] if candidates else "::1"


def get_fabnetv6_ips(slice_, nodes: list[NodeSpec]) -> dict[str, str]:
    """Collect FABNetv6 IPv6 addresses for controller and all agent VMs."""
    ctrl_vm = slice_.get_node("controller")
    fabnetv6_ips = {"controller": _extract_vm_fabnetv6_ip(ctrl_vm)}
    print(f"  controller (@{ctrl_vm.get_site()}) FABNetv6 IP: {fabnetv6_ips['controller']}")

    for node in nodes:
        vm_name = _vm_name(node.id)
        agent_vm = slice_.get_node(vm_name)
        ip_v6 = _extract_vm_fabnetv6_ip(agent_vm)
        fabnetv6_ips[node.id] = ip_v6
        print(f"  {node.id:10s} ({vm_name}@{agent_vm.get_site()}) FABNetv6 IP: {ip_v6}")

    return fabnetv6_ips


def setup_ptp_on_slice(slice_, nodes: list[NodeSpec]) -> None:
    """Configure LinuxPTP (ptp_kvm + phc2sys) across all slice VMs (per fabric-testbed/ptp).

    On FABRIC KVM hosts, `/dev/ptp_kvm` exposes the host's GPS/PTP-disciplined clock
    directly inside the VM. Following https://github.com/fabric-testbed/ptp, this
    disables chronyd, loads the `ptp_kvm` kernel module, installs `linuxptp`, steps
    `CLOCK_REALTIME` to `/dev/ptp_kvm`, and runs `phc2sys@CLOCK_REALTIME.service` so
    unidirectional quantum probe timestamps (`rx_ts - tx_ts`) have microsecond accuracy.
    """
    ptp_setup_script = textwrap.dedent("""\
        set -e
        if ! ip -4 route show default 2>/dev/null | grep -q default; then
            sudo sed -i '/192\\.168\\.122\\.1/d; /0\\.0\\.0\\.0/d' /etc/resolv.conf 2>/dev/null || true
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
        sudo systemctl enable --now phc2sys@CLOCK_REALTIME.service >/dev/null 2>&1
        sudo systemctl restart phc2sys@CLOCK_REALTIME.service
        sleep 1
        sudo phc_ctl /dev/ptp_kvm cmp | grep offset
    """)

    all_vms = [("controller", slice_.get_node("controller"))] + [
        (n.id, slice_.get_node(_vm_name(n.id))) for n in nodes
    ]

    def _configure_vm_ptp(item: tuple[str, object]) -> str:
        label, vm = item
        out, err = vm.execute(ptp_setup_script, quiet=True)
        summary = " | ".join(line.strip() for line in out.splitlines() if line.strip())
        if not summary and err:
            summary = err.strip().splitlines()[-1]
        return f"  [PTP] {label:10s} ({vm.get_name()}@{vm.get_site()}): {summary}"

    with ThreadPoolExecutor(max_workers=max(1, len(all_vms))) as pool:
        futures = [pool.submit(_configure_vm_ptp, item) for item in all_vms]
        for fut in as_completed(futures):
            print(fut.result())


# ═══════════════════════════════════════════════════════════════════════════
# Phase 3: Deploy controller (Docker backends + native controller in tmux)
#          and agents (native quantnet_agent in tmux)
# ═══════════════════════════════════════════════════════════════════════════


def _upload_bundle_contents(vm, local_dir: Path, remote_dir: str = "/home/rocky/qncp") -> None:
    """Upload the contents inside local_dir directly into remote_dir."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tar_path = Path(tmpdir) / "bundle.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tf:
            for item in local_dir.iterdir():
                tf.add(item, arcname=item.name)
        remote_tar = f"/tmp/bundle_{local_dir.name}.tar.gz"
        vm.upload_file(str(tar_path), remote_tar)
        vm.execute(
            f"mkdir -p {remote_dir} {remote_dir}/logs && "
            f"tar -xzf {remote_tar} -C {remote_dir} && "
            f"rm -f {remote_tar}",
            quiet=True,
        )


def _upload_local_repo(vm, repo_name: str) -> bool:
    """Package a local workspace repo and upload it to ~/qncp/src/<repo_name>."""
    local_repo = WORKSPACE_DIR / repo_name
    if not local_repo.is_dir():
        return False

    exclude_parts = {".git", "__pycache__", ".pytest_cache", ".mypy_cache", ".tox", ".venv", "node_modules"}

    def _tar_filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
        parts = Path(tarinfo.name).parts
        if any(p in exclude_parts or p.endswith(".egg-info") for p in parts):
            return None
        return tarinfo

    with tempfile.TemporaryDirectory() as tmpdir:
        tar_path = Path(tmpdir) / f"{repo_name}.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tf:
            for item in local_repo.iterdir():
                if item.name in exclude_parts or item.name.endswith(".egg-info"):
                    continue
                tf.add(item, arcname=item.name, filter=_tar_filter)

        remote_tar = f"/tmp/{repo_name}.tar.gz"
        vm.upload_file(str(tar_path), remote_tar)
        vm.execute(
            f"mkdir -p /home/rocky/qncp/src/{repo_name} && "
            f"tar -xzf {remote_tar} -C /home/rocky/qncp/src/{repo_name} && "
            f"rm -f {remote_tar}",
            quiet=True,
        )
    return True


def _ensure_native_qncp_on_vm(
    vm,
    role: str,
    git_org: str,
    mq_branch: str,
    plugins_branch: str,
    role_branch: str,
) -> None:
    """Install native Python venv and QNCP packages on a VM (and sync local repo sources)."""
    role_repo = "qn-server" if role == "controller" else "qn-agent"
    repos = ["qn-mq", "qn-plugins", role_repo]
    if role == "controller":
        repos.append("qn-api")
    for repo_name in repos:
        _upload_local_repo(vm, repo_name)

    role_bin = "quantnet_controller" if role == "controller" else "quantnet_agent"
    chk_existing, _ = vm.execute(
        f"test -x /home/rocky/qncp/.venv/bin/{role_bin} && echo READY || echo MISSING",
        quiet=True,
    )
    if "READY" in chk_existing:
        return

    cmd = (
        f"chmod +x ~/node_tools/setup_native_qncp.sh && "
        f"~/node_tools/setup_native_qncp.sh {role} {git_org} "
        f"{mq_branch} {plugins_branch} {role_branch}"
    )
    stdout, stderr = vm.execute(cmd, quiet=True)
    chk_out, _ = vm.execute(
        f"test -x /home/rocky/qncp/.venv/bin/{role_bin} && echo OK || echo MISSING",
        quiet=True,
    )
    if "OK" not in chk_out:
        raise RuntimeError(
            f"Failed to set up native {role} environment on {vm.get_name()}:\n"
            f"stdout: {stdout}\nstderr: {stderr}"
        )


def deploy_controller(
    slice_,
    staging_dir: Path,
    git_org: str = DEFAULT_GIT_ORG,
    mq_branch: str = DEFAULT_MQ_BRANCH,
    plugins_branch: str = DEFAULT_PLUGINS_BRANCH,
    server_branch: str = DEFAULT_SERVER_BRANCH,
) -> None:
    """Upload configs, start mosquitto+mongo in Docker, and launch controller + API in tmux."""
    controller_vm = slice_.get_node("controller")
    print(f"Controller management IP ({controller_vm.get_site()}): {controller_vm.get_management_ip()}")

    _upload_bundle_contents(controller_vm, staging_dir / "controller", "/home/rocky/qncp")
    controller_vm.execute(
        "mkdir -p /home/rocky/qncp/etc && "
        "ln -sf /home/rocky/qncp/conf/quantnet.cfg /home/rocky/qncp/etc/quantnet.cfg",
        quiet=True,
    )
    print("  Uploaded controller configs to /home/rocky/qncp")

    _, stderr = controller_vm.execute(
        "cd /home/rocky/qncp && sudo docker compose up -d mosquitto mongo",
        quiet=True,
    )
    print("  Backend containers (mosquitto + mongo) started")
    if stderr and "Error" in stderr:
        print(f"  stderr: {stderr}", file=sys.stderr)

    print(
        f"  Setting up native controller venv "
        f"(org={git_org}, qn-server={server_branch}, qn-mq={mq_branch})..."
    )
    _ensure_native_qncp_on_vm(
        controller_vm,
        role="controller",
        git_org=git_org,
        mq_branch=mq_branch,
        plugins_branch=plugins_branch,
        role_branch=server_branch,
    )

    ctrl_cmd = (
        "tmux kill-session -t qncp-controller 2>/dev/null || true; "
        "tmux new-session -d -s qncp-controller "
        "\"QUANTNET_LINK_ADJACENCY=true /home/rocky/qncp/.venv/bin/quantnet_controller "
        "-c /home/rocky/qncp/conf/quantnet.cfg "
        "--mq-broker-host 127.0.0.1 "
        "--mq-mongo-host 127.0.0.1 "
        "--plugin-path /home/rocky/qncp/src/qn-plugins/plugins "
        "--schema-path /home/rocky/qncp/src/qn-plugins/plugins/schema "
        "2>&1 | tee /home/rocky/qncp/logs/controller.console.log\""
    )
    controller_vm.execute(ctrl_cmd, quiet=True)
    print("  Started native quantnet_controller in tmux session 'qncp-controller'")

    api_cmd = (
        "tmux kill-session -t qncp-api 2>/dev/null || true; "
        "tmux new-session -d -s qncp-api "
        "\"cd /home/rocky/qncp/src/qn-api && "
        "QUANTNET_HOME=/home/rocky/qncp /home/rocky/qncp/.venv/bin/uvicorn main:app "
        "--host :: --port 8081 "
        "2>&1 | tee /home/rocky/qncp/logs/api.console.log\""
    )
    controller_vm.execute(api_cmd, quiet=True)
    print("  Started native qn-api dashboard in tmux session 'qncp-api' (port 8081)")


def _build_agent_health_cmd(node_id: str | None = None, *, tmux_bin: str = "tmux") -> str:
    """Build shell command to verify both the qncp-agent tmux session and quantnet_agent process are running."""
    proc_pattern = f"^[^ ]*python.*-a {node_id}" if node_id else "^[^ ]*python.*quantnet_agent"
    return (
        f"{tmux_bin} has-session -t qncp-agent 2>/dev/null "
        f"&& pgrep -f '{proc_pattern}' >/dev/null "
        "&& echo UP || echo DOWN"
    )


def _build_agent_tmux_cmd(
    node_id: str,
    ndef_path: str,
    *,
    tmux_bin: str = "tmux",
    log_path: str = "/home/rocky/qncp/logs/agent.console.log",
    agent_exec: str | None = None,
    enable_linger: bool = True,
) -> str:
    """Build shell command to start an intact qncp-agent tmux shell session first and then launch quantnet_agent."""
    if agent_exec is None:
        agent_exec = (
            f"QUANTNET_LINK_ADJACENCY=true /home/rocky/qncp/.venv/bin/quantnet_agent "
            f"-a {node_id} "
            f"-c /home/rocky/qncp/conf/agent.cfg "
            f"-n {ndef_path} "
            f"--interpreter-path /home/rocky/qncp/src/qn-plugins/plugins/pingpong/interpreter "
            f"--schema-path /home/rocky/qncp/src/qn-plugins/plugins/schema"
        )
    linger_prefix = "sudo loginctl enable-linger rocky 2>/dev/null || true; " if enable_linger else ""
    log_dir = str(Path(log_path).parent)
    proc_pattern = f"^[^ ]*python.*-a {node_id}"
    return (
        f"{linger_prefix}"
        f"mkdir -p {log_dir}; "
        f"pkill -INT -f '{proc_pattern}' 2>/dev/null || true; "
        "for _ in 1 2 3 4 5 6 7 8 9 10; do "
        f"! pgrep -f '{proc_pattern}' >/dev/null && break; "
        "sleep 0.2; "
        "done; "
        f"pkill -9 -f '{proc_pattern}' 2>/dev/null || true; "
        f"{tmux_bin} start-server \\; set-option -s exit-empty off; "
        f"if {tmux_bin} has-session -t qncp-agent 2>/dev/null; then "
        f"{tmux_bin} send-keys -t qncp-agent C-c C-u; "
        "else "
        f"{tmux_bin} new-session -d -s qncp-agent; "
        "fi; "
        f"{tmux_bin} pipe-pane -t qncp-agent -o 'cat >> {log_path}'; "
        f"{tmux_bin} send-keys -t qncp-agent \"{agent_exec}\" C-m"
    )


def _start_agent_tmux_session(
    vm,
    node_id: str,
    ndef_path: str,
    *,
    max_attempts: int = 3,
    verify_delay: float = 1.0,
) -> None:
    """Start (or restart) qncp-agent in tmux on a VM and verify both session and agent stay UP."""
    agent_cmd = _build_agent_tmux_cmd(node_id, ndef_path)
    health_cmd = _build_agent_health_cmd(node_id)
    last_out = ""
    last_err = ""
    for _ in range(1, max_attempts + 1):
        last_out, last_err = vm.execute(agent_cmd, quiet=True)
        if verify_delay > 0:
            time.sleep(verify_delay)
        chk, _ = vm.execute(health_cmd, quiet=True)
        if "UP" in chk:
            return
    log_tail, _ = vm.execute(
        "tail -n 30 /home/rocky/qncp/logs/agent.console.log 2>/dev/null || true",
        quiet=True,
    )
    raise RuntimeError(
        f"Failed to start tmux session 'qncp-agent' for {node_id} on {vm.get_name()} "
        f"after {max_attempts} attempts.\n"
        f"stdout: {last_out}\nstderr: {last_err}\nlog tail:\n{log_tail}"
    )


def deploy_agents(
    slice_,
    nodes: list[NodeSpec],
    staging_dir: Path,
    fabnetv6_ips: dict[str, str],
    git_org: str = DEFAULT_GIT_ORG,
    mq_branch: str = DEFAULT_MQ_BRANCH,
    plugins_branch: str = DEFAULT_PLUGINS_BRANCH,
    agent_branch: str = DEFAULT_AGENT_BRANCH,
) -> None:
    """Upload per-agent configs in parallel and start native quantnet_agent in tmux on each agent VM."""

    def _provision_single_agent(node: NodeSpec) -> None:
        vm_name = _vm_name(node.id)
        agent_vm = slice_.get_node(vm_name)
        node_ipv6 = fabnetv6_ips.get(node.id, "::1")

        print(
            f"  [Provisioning] {node.id} on {vm_name}@{agent_vm.get_site()} "
            f"(Mgmt: {agent_vm.get_management_ip()}, FABNetv6: {node_ipv6})..."
        )
        _upload_bundle_contents(agent_vm, staging_dir / "agents" / node.id, "/home/rocky/qncp")
        _ensure_native_qncp_on_vm(
            agent_vm,
            role="agent",
            git_org=git_org,
            mq_branch=mq_branch,
            plugins_branch=plugins_branch,
            role_branch=agent_branch,
        )
        print(f"  [Ready] Native venv installed on {vm_name}@{agent_vm.get_site()}")

    with ThreadPoolExecutor(max_workers=max(1, len(nodes))) as pool:
        futures = [pool.submit(_provision_single_agent, n) for n in nodes]
        for fut in as_completed(futures):
            fut.result()

    for i, node in enumerate(nodes):
        vm_name = _vm_name(node.id)
        agent_vm = slice_.get_node(vm_name)
        if i > 0:
            time.sleep(AGENT_STARTUP_DELAY)
        _start_agent_tmux_session(
            agent_vm,
            node.id,
            f"/home/rocky/qncp/node_defs/conf_{node.id}.json",
        )
        print(
            f"  Started native quantnet_agent ({node.id}) in tmux session 'qncp-agent' "
            f"on {vm_name}@{agent_vm.get_site()}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# Phase 4: Verify deployment & print summary
# ═══════════════════════════════════════════════════════════════════════════


def verify_deployment(
    slice_,
    nodes: list[NodeSpec],
    fabnetv6_ips: dict[str, str] | None = None,
) -> bool:
    """Check that backend containers, native tmux sessions, and FABNetv6 links are up."""
    print("\n" + "=" * 60)
    print("DEPLOYMENT VERIFICATION")
    print("=" * 60)

    all_ok = True
    controller_vm = slice_.get_node("controller")
    stdout, _ = controller_vm.execute("sudo docker ps --format '{{.Names}}: {{.Status}}'", quiet=True)
    tmux_out, _ = controller_vm.execute(
        "tmux has-session -t qncp-controller 2>/dev/null && echo 'qncp-controller: Up (tmux)' "
        "|| echo 'qncp-controller: DOWN'",
        quiet=True,
    )
    ctrl_v6 = (fabnetv6_ips or {}).get("controller", "")
    print(
        f"\nController VM (@{controller_vm.get_site()}, "
        f"Mgmt: {controller_vm.get_management_ip()}, FABNetv6: {ctrl_v6}):"
    )
    for line in stdout.strip().splitlines():
        print(f"  [docker] {line}")
        if "Up" not in line:
            all_ok = False
    for line in tmux_out.strip().splitlines():
        print(f"  [native] {line}")
        if "Up" not in line:
            all_ok = False

    for node in nodes:
        vm_name = _vm_name(node.id)
        agent_vm = slice_.get_node(vm_name)
        tmux_agent, _ = agent_vm.execute(
            f"tmux has-session -t qncp-agent 2>/dev/null && pgrep -f '^[^ ]*python.*-a {node.id}' >/dev/null "
            "&& echo 'qncp-agent: Up (tmux)' || echo 'qncp-agent: DOWN'",
            quiet=True,
        )
        node_v6 = (fabnetv6_ips or {}).get(node.id, "")
        print(
            f"\nAgent VM {node.id} (@{agent_vm.get_site()}, "
            f"Mgmt: {agent_vm.get_management_ip()}, FABNetv6: {node_v6}):"
        )
        for line in tmux_agent.strip().splitlines():
            print(f"  [native] {line}")
            if "Up" not in line:
                all_ok = False

    if fabnetv6_ips and len(nodes) >= 2:
        src_node, dst_node = nodes[0], nodes[1]
        dst_v6 = fabnetv6_ips.get(dst_node.id)
        if dst_v6 and dst_v6 != "::1":
            src_vm = slice_.get_node(_vm_name(src_node.id))
            ping_out, _ = src_vm.execute(
                f"ping -6 -c 1 -W 3 {dst_v6} >/dev/null 2>&1 && echo REACHABLE || echo UNREACHABLE",
                quiet=True,
            )
            print(
                f"\nFABNetv6 data-plane check ({src_node.id} -> {dst_node.id} [{dst_v6}]): "
                f"{ping_out.strip()}"
            )

    return all_ok


def _zsh_safe_ssh_command(vm_node) -> str:
    """Return node SSH command with [%h]:%p escaped so macOS zsh does not glob IPv6 brackets."""
    return vm_node.get_ssh_command().replace("-W [%h]:%p", r"-W \[%h\]:%p")


def print_summary(
    slice_,
    nodes: list[NodeSpec],
    broker_ip: str,
    fabnetv6_ips: dict[str, str] | None = None,
) -> None:
    """Print deployment summary with SSH commands, FABNetv6 IPs, and REPL instructions."""
    print("\n" + "=" * 60)
    print("DEPLOYMENT SUMMARY")
    print("=" * 60)

    qpu_count = sum(1 for n in nodes if n.type == NodeType.QNODE)
    bsm_count = sum(1 for n in nodes if n.type == NodeType.BSM_NODE)
    print(f"\nSlice name:  {slice_.get_name()}")
    print(f"Slice state: {slice_.get_state()}")
    print(f"Topology:    full-mesh ({qpu_count} QPUs, {bsm_count} BSMs)")
    print(f"MQTT Broker: {broker_ip}:1883")
    print("Probe Slice: FABNetv6 (NIC_Basic / IPv6 UDP quantum probes)")

    if fabnetv6_ips:
        print("\nFABNetv6 Data-Plane IPv6 Addresses:")
        for k, v in fabnetv6_ips.items():
            print(f"  {k:15s} {v}")

    print("\nSSH Commands:")
    for vm_node in slice_.get_nodes():
        label = f"{vm_node.get_name()} (@{vm_node.get_site()})"
        print(f"  {label:28s} {_zsh_safe_ssh_command(vm_node)}")

    print("\nInteractive Probe Monitoring (inside any agent VM):")
    print("  tmux attach -t qncp-agent")
    print("  qncp> show probe")
    print("  qncp> monitor probe")
    print("  qncp> probe <neighbor>:<ch>")

    print("\nController Console (inside controller VM):")
    print("  tmux attach -t qncp-controller")

    print("\nTo delete or renew the slice:")
    print(f"  python deploy_fabric_qncp.py delete --slice-name {slice_.get_name()}")
    print(f"  python deploy_fabric_qncp.py renew --slice-name {slice_.get_name()}")


# ═══════════════════════════════════════════════════════════════════════════
# CLI: deploy / delete / renew / status / test-config
# ═══════════════════════════════════════════════════════════════════════════


def _build_placeholder_nodes(num_qpus: int) -> list[NodeSpec]:
    """Build NodeSpec list for VM creation before slice IPs are assigned."""
    descriptor = Descriptor(
        topology={"template": "full-mesh"},
        nodes={"qpu": {"count": num_qpus}},
    )
    return get_builder(descriptor.topology.template).build(descriptor)


def cmd_deploy(args: argparse.Namespace) -> None:
    """Deploy a new QNCP slice on FABRIC with FABNetv6 and native execution."""
    staging_dir = Path(args.staging_dir)
    if staging_dir.exists():
        print(f"Removing existing staging directory: {staging_dir}")
        shutil.rmtree(staging_dir)

    placeholder_nodes = _build_placeholder_nodes(args.num_qpus)

    print("\n>>> Phase 1: Creating FABRIC slice with FABNetv6 L3 network...")
    _, slice_ = create_slice(
        slice_name=args.slice_name,
        site=args.site,
        nodes=placeholder_nodes,
    )

    controller_vm = slice_.get_node("controller")
    controller_mgmt_ip = str(controller_vm.get_management_ip())
    print(f"Controller Management IP: {controller_mgmt_ip}")

    print("\n>>> Collecting FABNetv6 IPv6 addresses from slice interfaces...")
    fabnetv6_ips = get_fabnetv6_ips(slice_, placeholder_nodes)
    broker_ip = fabnetv6_ips.get("controller") or controller_mgmt_ip

    print("\n>>> Configuring LinuxPTP (ptp_kvm + phc2sys) across all slice VMs...")
    setup_ptp_on_slice(slice_, placeholder_nodes)

    print("\n>>> Phase 2: Generating QNCP configs for native execution + FABNetv6 probes...")
    nodes = generate_configs(
        num_qpus=args.num_qpus,
        image_tag=args.image_tag,
        staging_dir=staging_dir,
        controller_ip=broker_ip,
        fabnetv6_ips=fabnetv6_ips,
        probe_base_port=args.probe_base_port,
    )

    print("\n>>> Phase 3: Deploying controller stack...")
    deploy_controller(
        slice_,
        staging_dir,
        git_org=args.git_org,
        mq_branch=args.mq_branch,
        plugins_branch=args.plugins_branch,
        server_branch=args.server_branch,
    )

    print(f"Waiting {DOCKER_STARTUP_WAIT}s for controller services to start...")
    time.sleep(DOCKER_STARTUP_WAIT)

    print("\n>>> Phase 4: Deploying agents...")
    deploy_agents(
        slice_,
        nodes,
        staging_dir,
        fabnetv6_ips=fabnetv6_ips,
        git_org=args.git_org,
        mq_branch=args.mq_branch,
        plugins_branch=args.plugins_branch,
        agent_branch=args.agent_branch,
    )

    print(f"\nWaiting {DOCKER_STARTUP_WAIT}s for agents to register and probe links...")
    time.sleep(DOCKER_STARTUP_WAIT)

    print("\n>>> Phase 5: Verifying deployment...")
    ok = verify_deployment(slice_, nodes, fabnetv6_ips=fabnetv6_ips)
    print_summary(slice_, nodes, broker_ip, fabnetv6_ips=fabnetv6_ips)

    if not ok:
        print("\nWARNING: Some services may not be running. Check logs with:")
        print("  ssh <vm> 'cat ~/qncp/logs/*.log'")
        sys.exit(1)


def cmd_delete(args: argparse.Namespace) -> None:
    """Delete an existing QNCP slice."""
    fablib = _init_fablib()
    try:
        slice_ = fablib.get_slice(name=args.slice_name)
        slice_.delete()
        print(f"Slice '{args.slice_name}' deleted.")
    except Exception as e:
        print(f"Error deleting slice: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_renew(args: argparse.Namespace) -> None:
    """Renew (extend) an existing QNCP slice."""
    fablib = _init_fablib()
    try:
        slice_ = fablib.get_slice(name=args.slice_name)
        end_date = datetime.now(timezone.utc) + timedelta(days=args.days)
        slice_.renew(end_date=end_date.strftime("%Y-%m-%d %H:%M:%S %z"))
        print(f"Slice '{args.slice_name}' renewed until {end_date.isoformat()}")
    except Exception as e:
        print(f"Error renewing slice: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_status(args: argparse.Namespace) -> None:
    """Show status of an existing QNCP slice."""
    fablib = _init_fablib()
    try:
        slice_ = fablib.get_slice(name=args.slice_name)
        print(f"Slice: {slice_.get_name()}")
        print(f"State: {slice_.get_state()}")
        print("\nNodes:")
        for node in slice_.get_nodes():
            v6_ip = _extract_vm_fabnetv6_ip(node)
            print(f"  {node.get_name():20s} Mgmt={node.get_management_ip()}  FABNetv6={v6_ip}")
            print(f"    ssh: {_zsh_safe_ssh_command(node)}")
            stdout, _ = node.execute(
                "sudo docker ps --format 'docker:{{.Names}}: {{.Status}}' 2>/dev/null; "
                "tmux ls 2>/dev/null | sed 's/^/tmux:/' || true",
                quiet=True,
            )
            for line in stdout.strip().splitlines():
                print(f"    {line}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_sync(args: argparse.Namespace) -> None:
    """Sync local QNCP repositories to an existing FABRIC slice and restart active services."""
    fablib = _init_fablib()
    slice_ = fablib.get_slice(name=args.slice_name)
    all_vms = slice_.get_nodes()
    controller_vm = slice_.get_node("controller")
    agent_vms = [vm for vm in all_vms if vm.get_name() != "controller"]

    print(
        f">>> Syncing local QNCP repositories from {WORKSPACE_DIR} "
        f"to slice '{slice_.get_name()}'..."
    )

    def _sync_controller() -> str:
        synced = [
            repo_name
            for repo_name in ("qn-mq", "qn-plugins", "qn-server", "qn-api")
            if _upload_local_repo(controller_vm, repo_name)
        ]
        label = ", ".join(synced) if synced else "none found locally"
        return f"  [Synced] controller: {label}"

    def _sync_agent(vm) -> tuple[str, bool, list[str]]:
        synced = [
            repo_name
            for repo_name in ("qn-mq", "qn-plugins", "qn-agent")
            if _upload_local_repo(vm, repo_name)
        ]
        chk, _ = vm.execute(_build_agent_health_cmd(), quiet=True)
        is_up = "UP" in chk
        return vm.get_name(), is_up, synced

    with ThreadPoolExecutor(max_workers=max(1, len(all_vms))) as pool:
        ctrl_fut = pool.submit(_sync_controller)
        agent_futs = [pool.submit(_sync_agent, vm) for vm in agent_vms]
        print(ctrl_fut.result())
        agent_states: dict[str, bool] = {}
        for fut in as_completed(agent_futs):
            vm_name, is_up, synced = fut.result()
            agent_states[vm_name] = is_up
            label = ", ".join(synced) if synced else "none found locally"
            print(f"  [Synced] {vm_name:15s}: {label} (qncp-agent was {'UP' if is_up else 'DOWN'})")

    print("\n>>> Restarting qncp-controller and qncp-api on controller VM...")
    ctrl_cmd = (
        "tmux kill-session -t qncp-controller 2>/dev/null || true; "
        "tmux new-session -d -s qncp-controller "
        "\"QUANTNET_LINK_ADJACENCY=true /home/rocky/qncp/.venv/bin/quantnet_controller "
        "-c /home/rocky/qncp/conf/quantnet.cfg "
        "--mq-broker-host 127.0.0.1 "
        "--mq-mongo-host 127.0.0.1 "
        "--plugin-path /home/rocky/qncp/src/qn-plugins/plugins "
        "--schema-path /home/rocky/qncp/src/qn-plugins/plugins/schema "
        "2>&1 | tee /home/rocky/qncp/logs/controller.console.log\""
    )
    controller_vm.execute(ctrl_cmd, quiet=True)

    api_cmd = (
        "tmux kill-session -t qncp-api 2>/dev/null || true; "
        "tmux new-session -d -s qncp-api "
        "\"cd /home/rocky/qncp/src/qn-api && "
        "QUANTNET_HOME=/home/rocky/qncp /home/rocky/qncp/.venv/bin/uvicorn main:app "
        "--host :: --port 8081 "
        "2>&1 | tee /home/rocky/qncp/logs/api.console.log\""
    )
    controller_vm.execute(api_cmd, quiet=True)
    print("  Restarted qncp-controller and qncp-api on controller VM")

    time.sleep(5)

    print("\n>>> Restarting qncp-agent sessions...")
    restart_all = getattr(args, "restart_all_agents", True)
    for vm in agent_vms:
        vm_name = vm.get_name()
        was_up = agent_states.get(vm_name, False)
        if not was_up and not restart_all:
            print(f"  [Skipped] {vm_name:15s} (leaving stopped as before)")
            continue
        ndef_out, _ = vm.execute("ls /home/rocky/qncp/node_defs/conf_*.json 2>/dev/null | head -n 1", quiet=True)
        ndef_path = ndef_out.strip()
        if not ndef_path:
            continue
        node_id = Path(ndef_path).stem.removeprefix("conf_")
        _start_agent_tmux_session(vm, node_id, ndef_path)
        action_label = "Restarted" if was_up else "Started"
        print(f"  [{action_label:9s}] {vm_name:13s} ({node_id})")


def cmd_test_config(args: argparse.Namespace) -> None:
    """Test config generation without FABRIC (no credentials needed)."""
    staging_dir = Path(args.staging_dir)
    if staging_dir.exists():
        print(f"Removing existing staging directory: {staging_dir}")
        shutil.rmtree(staging_dir)

    controller_ip = args.controller_ip or "10.20.30.40"

    print(f"\n>>> Testing config generation for {args.num_qpus} QPUs...")
    print(f"    Controller IP (mock): {controller_ip}")
    print(f"    Staging directory: {staging_dir}")

    try:
        nodes = generate_configs(
            num_qpus=args.num_qpus,
            image_tag=args.image_tag,
            staging_dir=staging_dir,
            controller_ip=controller_ip,
            probe_base_port=args.probe_base_port,
        )
        fabnetv6_ips = json.loads((staging_dir / "fabnetv6_ips.json").read_text())

        print("\n✓ Config generation successful")
        print(f"  Nodes generated: {[n.id for n in nodes]}")
        print(f"  Mock FABNetv6 IPs: {fabnetv6_ips}")
        print(f"  Total VMs (including controller): {len(nodes) + 1}")

        print("\n>>> Verifying directory structure & configs...")
        assert (staging_dir / "controller" / "docker-compose.yml").exists()
        assert (staging_dir / "controller" / "conf" / "quantnet.cfg").exists()
        assert (staging_dir / "controller" / "conf" / "mosquitto.conf").exists()
        assert (staging_dir / "fabnetv6_ips.json").exists()

        ctrl_compose = yaml.safe_load((staging_dir / "controller" / "docker-compose.yml").read_text())
        assert set(ctrl_compose["services"].keys()) == {"mosquitto", "mongo"}

        ctrl_cfg = (staging_dir / "controller" / "conf" / "quantnet.cfg").read_text()
        assert "host=127.0.0.1" in ctrl_cfg
        assert "path=/home/rocky/qncp/src/qn-plugins/plugins" in ctrl_cfg
        print("  ✓ Controller bundle verified")

        for node in nodes:
            agent_dir = staging_dir / "agents" / node.id
            agent_cfg_file = agent_dir / "conf" / "agent.cfg"
            node_def_file = agent_dir / "node_defs" / f"conf_{node.id}.json"
            assert agent_cfg_file.exists()
            assert node_def_file.exists()

            agent_cfg = agent_cfg_file.read_text()
            node_v6 = fabnetv6_ips[node.id]
            assert f"host={controller_ip}" in agent_cfg
            assert "driver=DummyLightSrc" in agent_cfg
            assert "driver=DummySignalMeasurement" in agent_cfg
            assert f"ip={node_v6}" in agent_cfg
            assert f"{node.id}:{node_v6}" in agent_cfg
            if node.type == NodeType.QNODE:
                assert "[tasks]" in agent_cfg
            else:
                assert "[tasks]" not in agent_cfg

            try:
                from configobj import ConfigObj
                from quantnet_agent.hal.driver.dummy import parse_virtual_links

                cfg_obj = ConfigObj(str(agent_cfg_file))
                ls_props = cfg_obj["devices"]["lightsource"]
                cid, out_vlinks, in_vlinks = parse_virtual_links(
                    str(node_def_file),
                    ip=ls_props["ip"],
                    base_port=int(ls_props["base_port"]),
                    peer_ips=ls_props.get("peer_ips"),
                )
                assert cid == node.id
                for (dst_cid, _), vlink in out_vlinks.items():
                    assert vlink.local_ip == node_v6
                    assert vlink.peer_ip == fabnetv6_ips[dst_cid]
                for (src_cid, _), vlink in in_vlinks.items():
                    assert vlink.local_ip == node_v6
                    assert vlink.peer_ip == fabnetv6_ips[src_cid]
            except ImportError:
                pass

        print(f"  ✓ Agent bundles verified ({len(nodes)} nodes)")
        print("\n✅ All tests passed! Config ready for FABRIC deployment.")

    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def _add_git_and_probe_args(parser: argparse.ArgumentParser) -> None:
    """Add git repository/branch and probe port arguments to a subparser."""
    parser.add_argument(
        "--git-org", default=DEFAULT_GIT_ORG,
        help=f"GitHub org/user for QNCP repos (default: {DEFAULT_GIT_ORG})",
    )
    parser.add_argument(
        "--agent-branch", default=DEFAULT_AGENT_BRANCH,
        help=f"Git branch for qn-agent (default: {DEFAULT_AGENT_BRANCH})",
    )
    parser.add_argument(
        "--server-branch", default=DEFAULT_SERVER_BRANCH,
        help=f"Git branch for qn-server (default: {DEFAULT_SERVER_BRANCH})",
    )
    parser.add_argument(
        "--mq-branch", default=DEFAULT_MQ_BRANCH,
        help=f"Git branch for qn-mq (default: {DEFAULT_MQ_BRANCH})",
    )
    parser.add_argument(
        "--plugins-branch", default=DEFAULT_PLUGINS_BRANCH,
        help=f"Git branch for qn-plugins (default: {DEFAULT_PLUGINS_BRANCH})",
    )
    parser.add_argument(
        "--probe-base-port", type=int, default=DEFAULT_PROBE_BASE_PORT,
        help=f"Base UDP port for emulated quantum probes over FABNetv6 (default: {DEFAULT_PROBE_BASE_PORT})",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deploy distributed QNCP on FABRIC testbed with FABNetv6 quantum probe emulation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python deploy_fabric_qncp.py deploy --num-qpus 3
              python deploy_fabric_qncp.py deploy --num-qpus 2 --site TACC
              python deploy_fabric_qncp.py status --slice-name QNCP-Testbed
              python deploy_fabric_qncp.py renew --slice-name QNCP-Testbed --days 3
              python deploy_fabric_qncp.py delete --slice-name QNCP-Testbed
        """),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # deploy
    p_deploy = subparsers.add_parser("deploy", help="Deploy a new QNCP slice with FABNetv6")
    p_deploy.add_argument("--num-qpus", type=int, default=2, help="Number of QPU nodes (default: 2)")
    p_deploy.add_argument("--slice-name", default="QNCP-Testbed", help="FABRIC slice name (default: QNCP-Testbed)")
    p_deploy.add_argument("--site", default=None, help="FABRIC site name (default: random US backbone sites)")
    p_deploy.add_argument("--image-tag", default="develop", help="Docker image tag (default: develop)")
    p_deploy.add_argument("--staging-dir", default="staging_qncp", help="Local staging directory")
    _add_git_and_probe_args(p_deploy)
    p_deploy.set_defaults(func=cmd_deploy)

    # delete
    p_delete = subparsers.add_parser("delete", help="Delete a QNCP slice")
    p_delete.add_argument("--slice-name", default="QNCP-Testbed")
    p_delete.set_defaults(func=cmd_delete)

    # renew
    p_renew = subparsers.add_parser("renew", help="Renew (extend) a QNCP slice")
    p_renew.add_argument("--slice-name", default="QNCP-Testbed")
    p_renew.add_argument("--days", type=int, default=1, help="Days to extend (default: 1)")
    p_renew.set_defaults(func=cmd_renew)

    # status
    p_status = subparsers.add_parser("status", help="Show QNCP slice status")
    p_status.add_argument("--slice-name", default="QNCP-Testbed")
    p_status.set_defaults(func=cmd_status)

    # sync
    p_sync = subparsers.add_parser("sync", help="Sync local QNCP repos to an existing slice and restart services")
    p_sync.add_argument("--slice-name", default="QNCP-Testbed")
    p_sync.add_argument(
        "--restart-all-agents",
        action="store_true",
        default=True,
        help="Start/restart qncp-agent on all agent VMs, including stopped ones (default: True)",
    )
    p_sync.add_argument(
        "--keep-stopped-agents",
        action="store_false",
        dest="restart_all_agents",
        help="Leave previously stopped qncp-agent VMs stopped instead of starting them",
    )
    p_sync.set_defaults(func=cmd_sync)

    # test-config
    p_test = subparsers.add_parser("test-config", help="Test config generation (no FABRIC account needed)")
    p_test.add_argument("--num-qpus", type=int, default=2, help="Number of QPU nodes (default: 2)")
    p_test.add_argument("--image-tag", default="develop", help="Docker image tag (default: develop)")
    p_test.add_argument("--staging-dir", default="staging_qncp_test", help="Local staging directory")
    p_test.add_argument("--controller-ip", default="10.20.30.40", help="Mock controller IP (default: 10.20.30.40)")
    _add_git_and_probe_args(p_test)
    p_test.set_defaults(func=cmd_test_config)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
