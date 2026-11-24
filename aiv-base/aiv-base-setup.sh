#!/usr/bin/env bash
#
# aiv-base-setup.sh - Enterprise Bare-Metal Clustered & Single-Node Deployer for AIV
# Compatible with RHEL / Rocky / Alma Linux (Versions 8, 9, and 10)
#
set -Eeuo pipefail
umask 022

# ==============================================================================
# 1. GLOBAL CONFIGURATION & PORT SETTINGS
# ==============================================================================
LOG_FILE="/var/log/aiv-base-setup.log"

# Core Versions & Download Links
JAVA_MAJOR="${JAVA_MAJOR:-21}"
AIV_JAVA_DIR="${AIV_JAVA_DIR:-/var/lib/aiv/jdk}"
TEMURIN_21_URL="${TEMURIN_21_URL:-https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.6%2B7/OpenJDK21U-jdk_x64_linux_hotspot_21.0.6_7.tar.gz}"
KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_LINK="/opt/kafka"

# Optional AIV RPM Download URL & Package Selection
DEFAULT_AIV_DOWNLOAD_URL="https://github.com/aiv-code/packages/releases/download/rpm%2F7.0.27-0/aiv-7.0.27-0.el9.noarch.rpm"
AIV_DOWNLOAD_URL="${AIV_DOWNLOAD_URL:-$DEFAULT_AIV_DOWNLOAD_URL}"
EXPLICIT_URL="no"
EXPLICIT_RPM=""
TARGET_RPM_FILE=""

# Network Ports Configuration
PG_PORT="${PG_PORT:-5432}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
AUTH_PORT="${AUTH_PORT:-8087}"
AIV_LB_PORT="${AIV_LB_PORT:-8086}"
AIV1_PORT="${AIV1_PORT:-8082}"
AIV2_PORT="${AIV2_PORT:-8083}"
AI_PORT="${AI_PORT:-8097}"

# Load Balancer Configuration ('nginx' or 'none')
LB_TYPE="${LB_TYPE:-nginx}"
F5_VIP="${F5_VIP:-}"
F5_PORT="${F5_PORT:-8086}"
F5_PROTO="${F5_PROTO:-http}"
F5_SELF_IPS="${F5_SELF_IPS:-}"

# S3 Shared Storage Settings
S3_BUCKET="${S3_BUCKET:-aiv}"
S3_URL="${S3_URL:-https://eu2.contabostorage.com}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-fc76dfcd488633409032aa65c5216151}"
S3_SECRET_KEY="${S3_SECRET_KEY:-92cb275801f4bd321bfe31b45297c2ec}"
S3_MOUNT_POINT="${S3_MOUNT_POINT:-/mnt/aiv-shared-repo}"

# Credentials & Database Configuration
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_NAME="${DB_NAME:-aiv}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-AivCluster2026_SecureKeyX9}"

# Multi-Server Cluster & Firewall Settings
SERVER1_IP="${SERVER1_IP:-169.58.216.153}"
KAFKA_REMOTE_HOST="${KAFKA_REMOTE_HOST:-169.58.216.153}"
ALLOW_IPS="${ALLOW_IPS:-}"
ALLOW_PORTS="${ALLOW_PORTS:-5432,9092,8087,8082,8083,8097}"

# Server IP (Auto-detected if empty, or overridden via SERVER_IP or --ip)
SERVER_IP="${SERVER_IP:-}"

# Runtime Flags
ROLE="server1"
IS_PRIMARY="yes"
ASSUME_YES="no"
UPGRADE_ONLY="no"
DOWNLOAD_AIV="yes"
INSTALL_PG="yes"
INSTALL_KAFKA="yes"
INSTALL_S3="no"
INSTALL_NGINX="yes"
INSTALL_BACKUP="no"
BACKUP_INTERVAL_DAYS="${BACKUP_INTERVAL_DAYS:-1}"
BACKUP_KEEP_DAYS="${BACKUP_KEEP_DAYS:-7}"

JAVA_BIN=""
JAVA_HOME_DIR=""
OS_MAJOR=""
RESULTS=()

# ==============================================================================
# 2. HELPER FUNCTIONS
# ==============================================================================
info() { printf '[INFO] %s\n' "$*"; }
ok()   { printf '[ OK ] %s\n' "$*"; }
warn() { printf '[WARN] %s\n' "$*"; }
err()  { printf '[FAIL] %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }
step() { printf '\n==== %s ====\n' "$*"; }
record() { RESULTS+=("$1|$2|$3"); }

trap 'err "Unexpected error at line ${LINENO}: ${BASH_COMMAND} (log: ${LOG_FILE})"' ERR

prompt_yes_no() {
  local prompt_text="$1"
  local default_val="$2"
  if [[ "$ASSUME_YES" == "yes" ]]; then
    echo "$default_val"
    return 0
  fi
  local answer
  local prompt_hint="[y/n]"
  [[ "$default_val" == "yes" ]] && prompt_hint="[Y/n]" || prompt_hint="[y/N]"
  while true; do
    read -r -p "$prompt_text $prompt_hint: " answer
    answer=$(echo "${answer:-$default_val}" | tr '[:upper:]' '[:lower:]')
    case "$answer" in
      y|yes) echo "yes"; return 0 ;;
      n|no)  echo "no";  return 0 ;;
      *) echo "Please enter 'y' for yes or 'n' for no." >&2 ;;
    esac
  done
}

# Compare two version strings: returns 0 if v1 == v2, 1 if v1 > v2, 2 if v1 < v2
ver_compare() {
  local v1="$1" v2="$2"
  [[ -z "$v1" && -z "$v2" ]] && { echo 0; return; }
  [[ -z "$v1" ]] && { echo 2; return; }
  [[ -z "$v2" ]] && { echo 1; return; }
  if [[ "$v1" == "$v2" ]]; then
    echo 0
  elif [[ $(printf '%s\n%s\n' "$v1" "$v2" | sort -V | tail -n 1) == "$v1" ]]; then
    echo 1
  else
    echo 2
  fi
}

# Scan /opt for all aiv-*.rpm files and return the path of the newest version
find_newest_local_rpm() {
  local files=()
  while IFS= read -r f; do
    [[ -n "$f" && -f "$f" ]] && files+=("$f")
  done < <(find /opt -maxdepth 1 -name "aiv-*.rpm" 2>/dev/null || true)

  [[ ${#files[@]} -eq 0 ]] && return 0

  local best_file="" best_ver=""
  for f in "${files[@]}"; do
    local cur_ver
    cur_ver=$(rpm -qp --queryformat '%{VERSION}-%{RELEASE}' "$f" 2>/dev/null || true)
    if [[ -z "$cur_ver" ]]; then
      local base
      base=$(basename "$f")
      cur_ver=$(echo "$base" | sed -E 's/^aiv-([0-9]+\.[0-9]+\.[0-9]+-[0-9]+).*/\1/' || true)
    fi
    [[ -z "$cur_ver" ]] && continue

    if [[ -z "$best_ver" ]]; then
      best_file="$f"
      best_ver="$cur_ver"
    elif [[ $(ver_compare "$cur_ver" "$best_ver") -eq 1 ]]; then
      best_file="$f"
      best_ver="$cur_ver"
    fi
  done
  echo "$best_file"
}

parse_args() {
  local role_explicit="no"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --role)          ROLE="${2:-}"; role_explicit="yes"; shift 2 ;;
      --server1-ip)    SERVER1_IP="${2:-}"; shift 2 ;;
      --allow-ip)      ALLOW_IPS="${2:-}"; shift 2 ;;
      --allow-ports)   ALLOW_PORTS="${2:-}"; shift 2 ;;
      --ip)            SERVER_IP="${2:-}"; shift 2 ;;
      --java)          JAVA_MAJOR="${2:-}"; shift 2 ;;
      --java-dir)      AIV_JAVA_DIR="${2:-}"; shift 2 ;;
      --upgrade|--upgrade-aiv) UPGRADE_ONLY="yes"; shift ;;
      --url)           AIV_DOWNLOAD_URL="${2:-}"; EXPLICIT_URL="yes"; shift 2 ;;
      --rpm)           EXPLICIT_RPM="${2:-}"; shift 2 ;;
      --lb-type)       LB_TYPE=$(echo "${2:-nginx}" | tr '[:upper:]' '[:lower:]'); shift 2 ;;
      --backup)        INSTALL_BACKUP="yes"; shift ;;
      --backup-days)   BACKUP_INTERVAL_DAYS="${2:-}"; shift 2 ;;
      --backup-keep)   BACKUP_KEEP_DAYS="${2:-}"; shift 2 ;;
      # [FUTURE F5 INTEGRATION - UNCOMMENT WHEN READY]
      # --f5-vip)        F5_VIP="${2:-}"; shift 2 ;;
      # --f5-port)       F5_PORT="${2:-8086}"; shift 2 ;;
      # --f5-proto)      F5_PROTO=$(echo "${2:-http}" | tr '[:upper:]' '[:lower:]'); shift 2 ;;
      # --f5-self-ips)   F5_SELF_IPS="${2:-}"; shift 2 ;;
      -y|--yes)        ASSUME_YES="yes"; shift ;;
      -h|--help)       exit 0 ;;
      *)               shift ;;
    esac
  done

  # Auto-detect existing role & custom ports if upgrading or re-running without explicit flags
  if [[ -f /var/lib/aiv/enviroment ]]; then
    if [[ "$role_explicit" == "no" ]]; then
      local env_db_host
      # Missing keys must not abort under pipefail (older env files may omit fields)
      env_db_host=$(grep -E "^export DATABASE_URL=" /var/lib/aiv/enviroment 2>/dev/null | sed -E 's#.*jdbc:postgresql://([^:/]+).*#\1#' || true)
      local srv_ip
      srv_ip=$(hostname -I 2>/dev/null | awk '{print $1}' || true)
      if [[ -n "$env_db_host" && "$env_db_host" != "127.0.0.1" && "$env_db_host" != "localhost" && "$env_db_host" != "$srv_ip" ]]; then
        ROLE="server2"
        SERVER1_IP="$env_db_host"
        info "Auto-detected existing Server 2 (Worker Node) configuration pointing to Primary Server: ${SERVER1_IP}"
      fi
    fi

    local env_p1 env_p2
    # If ports are absent (pre-port env files), keep script defaults instead of failing upgrade
    env_p1=$(grep -E "^export AIV1_PORT=" /var/lib/aiv/enviroment 2>/dev/null | cut -d'=' -f2 | tr -d "\"' " || true)
    env_p2=$(grep -E "^export AIV2_PORT=" /var/lib/aiv/enviroment 2>/dev/null | cut -d'=' -f2 | tr -d "\"' " || true)
    if [[ -n "$env_p1" && "$env_p1" =~ ^[0-9]+$ ]]; then
      AIV1_PORT="$env_p1"
      info "Loaded Node 1 port from environment file: ${AIV1_PORT}"
    fi
    if [[ -n "$env_p2" && "$env_p2" =~ ^[0-9]+$ ]]; then
      AIV2_PORT="$env_p2"
      info "Loaded Node 2 port from environment file: ${AIV2_PORT}"
    fi

    local env_lb_type
    env_lb_type=$(grep -E "^export LB_TYPE=" /var/lib/aiv/enviroment 2>/dev/null | cut -d'=' -f2 | tr -d "\"' " || true)
    [[ -n "$env_lb_type" ]] && LB_TYPE="$env_lb_type"
    # [FUTURE F5 INTEGRATION - UNCOMMENT WHEN READY]
    # local env_f5_vip env_f5_port env_f5_proto
    # env_f5_vip=$(grep -E "^export F5_VIP=" /var/lib/aiv/enviroment 2>/dev/null | cut -d'=' -f2 | tr -d "\"' " || true)
    # env_f5_port=$(grep -E "^export F5_PORT=" /var/lib/aiv/enviroment 2>/dev/null | cut -d'=' -f2 | tr -d "\"' " || true)
    # env_f5_proto=$(grep -E "^export F5_PROTO=" /var/lib/aiv/enviroment 2>/dev/null | cut -d'=' -f2 | tr -d "\"' " || true)
    # [[ -n "$env_f5_vip" ]] && F5_VIP="$env_f5_vip"
    # [[ -n "$env_f5_port" ]] && F5_PORT="$env_f5_port"
    # [[ -n "$env_f5_proto" ]] && F5_PROTO="$env_f5_proto"
  fi

  if [[ "$LB_TYPE" == "none" ]]; then
    INSTALL_NGINX="no"
  fi

  # Adjust defaults automatically when deploying as Server 2 (Worker Node)
  if [[ "$ROLE" == "server2" ]]; then
    IS_PRIMARY="no"
    INSTALL_PG="no"
    INSTALL_KAFKA="no"
    INSTALL_NGINX="no"
    if [[ -n "$SERVER1_IP" ]]; then
      DB_HOST="$SERVER1_IP"
      KAFKA_REMOTE_HOST="$SERVER1_IP"
    fi
  fi
}

setup_logging() {
  mkdir -p "$(dirname "$LOG_FILE")"
  exec > >(tee -a "$LOG_FILE") 2>&1
  printf '\n######## %s Deployment Initiated ########\n' "$(date '+%F %T')"
}

detect_os() {
  [[ -r /etc/os-release ]] || die "Cannot read /etc/os-release"
  . /etc/os-release
  OS_MAJOR="${VERSION_ID%%.*}"
  command -v dnf >/dev/null 2>&1 || die "dnf not found."
  info "Detected OS: ${PRETTY_NAME:-$ID $VERSION_ID} (Major: ${OS_MAJOR}) on $(uname -m)"
  if [[ "$OS_MAJOR" -lt 10 && -z "${JAVA_MAJOR:-}" ]]; then
    JAVA_MAJOR="17"
  fi
}

get_server_ip() {
  if [[ -n "${SERVER_IP:-}" ]]; then
    echo "$SERVER_IP"
    return 0
  fi
  local detected_ip=""
  detected_ip=$(curl -s --connect-timeout 3 https://api.ipify.org 2>/dev/null || \
                curl -s --connect-timeout 3 https://ifconfig.me 2>/dev/null || \
                ip -4 route get 1.1.1.1 2>/dev/null | grep -oP 'src \K\S+' || \
                hostname -I 2>/dev/null | awk '{print $1}')
  SERVER_IP="${detected_ip:-127.0.0.1}"
  echo "$SERVER_IP"
}

detect_existing_whitelisted_ips() {
  local ips=()
  local hba_file="/var/lib/pgsql/data/pg_hba.conf"
  if [[ -f "$hba_file" ]]; then
    while read -r rip; do
      if [[ -n "$rip" && "$rip" != 127.* && "$rip" != ::* && "$rip" != "localhost"* ]]; then
        ips+=("$rip")
      fi
    done < <(grep -E '^\s*host\s+all\s+all\s+' "$hba_file" 2>/dev/null | awk '{print $4}' | sed 's#/32##' | sort -u)
  fi

  if systemctl is-active --quiet firewalld 2>/dev/null; then
    while read -r rip; do
      if [[ -n "$rip" && "$rip" != 127.* ]]; then
        ips+=("$rip")
      fi
    done < <(firewall-cmd --list-rich-rules 2>/dev/null | sed -n 's/.*source address="\([^"]*\)".*/\1/p' | sort -u)
  fi

  if [[ ${#ips[@]} -gt 0 ]]; then
    printf '%s\n' "${ips[@]}" | sort -u | tr '\n' ',' | sed 's/,$//' | sed 's/,/, /g'
  else
    echo ""
  fi
}

ask_user_preferences() {
  step "Installation Options"
  local srv_ip
  srv_ip=$(get_server_ip)

  # Check if AIV is already installed
  if rpm -q aiv >/dev/null 2>&1 && [[ "$ASSUME_YES" == "no" ]]; then
    local installed_pkg
    installed_pkg=$(rpm -q --queryformat '%{NAME}-%{VERSION}-%{RELEASE}' aiv 2>/dev/null || true)
    local desired_rpm url_no_query
    url_no_query="${AIV_DOWNLOAD_URL%%\?*}"
    desired_rpm="${url_no_query##*/}"
    local local_candidate
    local_candidate=$(find_newest_local_rpm)
    local local_cand_name=""
    [[ -n "$local_candidate" ]] && local_cand_name=$(basename "$local_candidate")

    echo
    printf '==================================================================\n'
    info "Existing AIV installation detected: ${installed_pkg}"
    info "Configured URL version            : ${desired_rpm%.el*.rpm}"
    [[ -n "$local_cand_name" ]] && info "Newest Local RPM in /opt          : ${local_cand_name}"
    printf '==================================================================\n'
    echo
    local do_quick_upgrade
    do_quick_upgrade=$(prompt_yes_no "Do you want to perform a quick in-place upgrade of AIV only (preserves DB & configs)?" "no")
    if [[ "$do_quick_upgrade" == "yes" ]]; then
      UPGRADE_ONLY="yes"
      return 0
    fi
  fi

  local existing_ips=""
  if [[ "$ASSUME_YES" == "no" ]]; then
    DOWNLOAD_AIV=$(prompt_yes_no "Do you want to download AIV RPM package from GitHub?" "$DOWNLOAD_AIV")

    INSTALL_PG=$(prompt_yes_no "Do you want to install local PostgreSQL server?" "$INSTALL_PG")
    if [[ "$INSTALL_PG" == "yes" ]]; then
      existing_ips=$(detect_existing_whitelisted_ips)
      echo
      if [[ -n "$existing_ips" ]]; then
        info "Currently whitelisted remote IP(s): ${existing_ips}"
        info "Currently allowed cluster ports   : ${ALLOW_PORTS}"
        local allow_remote
        allow_remote=$(prompt_yes_no "Do you want to whitelist additional remote worker / client IP(s)?" "yes")
        if [[ "$allow_remote" == "yes" ]]; then
          read -r -p "Enter new IP(s) to add to whitelist (comma-separated, e.g. local PC IP): " input_allow_ips
          ALLOW_IPS="${input_allow_ips:-}"
          read -r -p "Enter ports to allow for these new IP(s) [Default: ${ALLOW_PORTS}]: " input_ports
          ALLOW_PORTS="${input_ports:-$ALLOW_PORTS}"
        fi
      else
        info "Currently whitelisted remote IP(s): None"
        local allow_remote
        allow_remote=$(prompt_yes_no "Do you want to whitelist remote worker / client IP(s) in PostgreSQL & Firewall?" "yes")
        if [[ "$allow_remote" == "yes" ]]; then
          read -r -p "Enter remote IP(s) to whitelist (comma-separated, e.g. 13.140.201.207): " input_allow_ips
          ALLOW_IPS="${input_allow_ips:-$ALLOW_IPS}"
          read -r -p "Enter ports to allow for these IP(s) [Default: ${ALLOW_PORTS}]: " input_ports
          ALLOW_PORTS="${input_ports:-$ALLOW_PORTS}"
        fi
      fi
    else
      local default_db_host="${SERVER1_IP:-$DB_HOST}"
      [[ "$default_db_host" == "127.0.0.1" ]] && default_db_host="169.58.216.153"
      read -r -p "Enter remote PostgreSQL Server IP / Host [Default: ${default_db_host}]: " input_db_host
      DB_HOST="${input_db_host:-$default_db_host}"
    fi

    INSTALL_KAFKA=$(prompt_yes_no "Do you want to install local Apache Kafka broker (KRaft mode)?" "$INSTALL_KAFKA")
    if [[ "$INSTALL_KAFKA" != "yes" ]]; then
      local default_kafka_host="${KAFKA_REMOTE_HOST:-$DB_HOST}"
      read -r -p "Enter remote Kafka Broker IP [Default: ${default_kafka_host}]: " input_kafka_host
      KAFKA_REMOTE_HOST="${input_kafka_host:-$default_kafka_host}"
    fi

    INSTALL_S3=$(prompt_yes_no "Do you want to mount Contabo/AWS S3 Object Storage?" "$INSTALL_S3")
    INSTALL_NGINX=$(prompt_yes_no "Do you want to install and configure Nginx Load Balancer?" "$INSTALL_NGINX")
    if [[ "$INSTALL_NGINX" == "yes" ]]; then
      LB_TYPE="nginx"
    else
      LB_TYPE="none"
    fi

    # [FUTURE F5 INTEGRATION - UNCOMMENT WHEN READY]
    # echo
    # echo "Load Balancer Selection for Cluster Routing:"
    # echo "  1) Nginx (Local software reverse proxy on this server)"
    # echo "  2) F5 BIG-IP (External Enterprise Application Delivery Controller)"
    # echo "  3) None / Direct Node Access"
    # read -r -p "Select Load Balancer type [1-3, Default: 1]: " lb_choice
    # case "${lb_choice:-1}" in
    #   2|f5|F5)
    #     LB_TYPE="f5"
    #     INSTALL_NGINX="no"
    #     read -r -p "Enter F5 Virtual IP (VIP) or Domain [Default: ${srv_ip}]: " input_f5_vip
    #     F5_VIP="${input_f5_vip:-$srv_ip}"
    #     read -r -p "Enter F5 Virtual Server Port [Default: ${F5_PORT}]: " input_f5_port
    #     F5_PORT="${input_f5_port:-$F5_PORT}"
    #     read -r -p "Enter F5 Protocol (http / https) [Default: ${F5_PROTO}]: " input_f5_proto
    #     F5_PROTO="${input_f5_proto:-$F5_PROTO}"
    #     read -r -p "Enter F5 Self/Floating IP(s) for firewall whitelist (optional, comma-separated): " input_f5_self
    #     F5_SELF_IPS="${input_f5_self:-}"
    #     ;;
    #   3|none|None)
    #     LB_TYPE="none"
    #     INSTALL_NGINX="no"
    #     ;;
    #   *)
    #     LB_TYPE="nginx"
    #     INSTALL_NGINX="yes"
    #     ;;
    # esac

    IS_PRIMARY=$(prompt_yes_no "Is this server the Primary Leader (hosts UniversalAuth & runs DB migrations)?" "$IS_PRIMARY")
    if [[ "$IS_PRIMARY" != "yes" ]]; then
      ROLE="server2"
      local default_auth_host="${SERVER1_IP:-$DB_HOST}"
      read -r -p "Enter UniversalAuth Server IP [Default: ${default_auth_host}]: " input_auth_host
      SERVER1_IP="${input_auth_host:-$default_auth_host}"
    else
      ROLE="server1"
    fi

    if [[ "$ROLE" == "server1" && "$INSTALL_PG" == "yes" ]]; then
      ask_backup_preferences
    fi
  else
    info "Non-interactive mode (-y) detected. Using standard deployment defaults for role: ${ROLE}."
  fi
  normalize_backup_settings

  cat <<EOF

--- Selected Configuration Plan ---
Deployment Role           : $([[ "$IS_PRIMARY" == "yes" ]] && echo "Primary Server (Server 1)" || echo "Worker Server (Server 2)")
Current Server IP         : ${srv_ip}
$([[ "$IS_PRIMARY" != "yes" ]] && echo "Primary Server IP         : ${SERVER1_IP:-$DB_HOST}")
$([[ "$INSTALL_PG" == "yes" && -n "$existing_ips" ]] && echo "Existing Whitelisted IPs  : ${existing_ips}")
$([[ "$INSTALL_PG" == "yes" && -n "$ALLOW_IPS" ]] && echo "New IPs to Add to Whitelist : ${ALLOW_IPS} (Ports: ${ALLOW_PORTS})")
Java Runtime              : Project-isolated (${AIV_JAVA_DIR})
Download AIV RPM from Web : ${DOWNLOAD_AIV}
Install PostgreSQL Server : ${INSTALL_PG} $([[ "$INSTALL_PG" == "yes" ]] && echo "(Local Port: ${PG_PORT})" || echo "(Remote: ${DB_HOST}:${PG_PORT})")
Install Apache Kafka      : ${INSTALL_KAFKA} $([[ "$INSTALL_KAFKA" == "yes" ]] && echo "(Local Port: ${KAFKA_PORT})" || echo "(Remote: ${KAFKA_REMOTE_HOST:-$DB_HOST}:${KAFKA_PORT})")
Mount S3 Shared Storage   : ${INSTALL_S3} (Path: ${S3_MOUNT_POINT})
Load Balancer Strategy    : $([[ "$INSTALL_NGINX" == "yes" ]] && echo "Nginx (Port: ${AIV_LB_PORT})" || echo "Disabled / Direct Node (:8082)")
# [FUTURE F5 INTEGRATION - UNCOMMENT WHEN READY]
# Load Balancer Strategy    : $([[ "$LB_TYPE" == "f5" ]] && echo "F5 BIG-IP (${F5_PROTO}://${F5_VIP:-$srv_ip}:${F5_PORT})" || { [[ "$INSTALL_NGINX" == "yes" ]] && echo "Nginx (Local Port: ${AIV_LB_PORT})" || echo "Disabled / Direct Node"; })
Run DB Migrations         : $([[ "$IS_PRIMARY" == "yes" ]] && echo "Yes (Node 1)" || echo "No (Worker mode)")
Host UniversalAuth        : $([[ "$IS_PRIMARY" == "yes" ]] && echo "Yes (Local Port: ${AUTH_PORT})" || echo "No (Hosted on ${SERVER1_IP:-$DB_HOST}:${AUTH_PORT})")
Cluster Nodes Planned     : $([[ "$IS_PRIMARY" != "yes" || "$INSTALL_KAFKA" == "yes" ]] && echo "2 Local Nodes (:8082, :8083)" || echo "1 Node only (:8082)")
Scheduled Backups         : $([[ "$INSTALL_BACKUP" == "yes" ]] && echo "Yes (every ${BACKUP_INTERVAL_DAYS} day(s), keep ${BACKUP_KEEP_DAYS} day(s))" || echo "No")
-----------------------------------
EOF

  if [[ "$ASSUME_YES" == "no" ]]; then
    read -r -p "Proceed with this deployment? [Y/n]: " proceed
    [[ "${proceed:-y}" =~ ^[Yy]$ ]] || die "Deployment cancelled by user."
  fi
}

ensure_pkg() {
  local pkg="$1" cmd="${2:-}"
  if [[ -n "$cmd" ]] && command -v "$cmd" >/dev/null 2>&1; then
    ok "Package present: $pkg ($cmd)"; return 0
  elif rpm -q "$pkg" >/dev/null 2>&1; then
    ok "Package present: $pkg"; return 0
  fi
  info "Installing $pkg ..."
  dnf install -y "$pkg"
}

step_prereqs() {
  step "Base Packages & System Repositories"
  if ! rpm -q epel-release >/dev/null 2>&1; then
    dnf install -y epel-release || true
  fi
  ensure_pkg curl curl
  ensure_pkg wget wget
  ensure_pkg tar tar
  ensure_pkg gzip gzip
  ensure_pkg unzip unzip
  ensure_pkg openssl openssl
  ensure_pkg policycoreutils-python-utils semanage
  ensure_pkg chrony chronyd
  ensure_pkg nftables nft

  if [[ "$INSTALL_S3" == "yes" ]]; then
    ensure_pkg s3fs-fuse s3fs
    ensure_pkg s3cmd s3cmd
  fi

  if [[ "$INSTALL_NGINX" == "yes" ]]; then
    ensure_pkg nginx nginx
  fi
  systemctl enable --now chronyd >/dev/null 2>&1 || true
  chronyc makestep >/dev/null 2>&1 || true
}

step_download_aiv() {
  step "AIV RPM Package Selection & Download"

  # Case 0: User explicitly specified an RPM path via --rpm
  if [[ -n "$EXPLICIT_RPM" ]]; then
    [[ -f "$EXPLICIT_RPM" ]] || die "Specified RPM file not found: ${EXPLICIT_RPM}"
    TARGET_RPM_FILE="$EXPLICIT_RPM"
    ok "Using explicitly specified local RPM: ${TARGET_RPM_FILE}"
    record "AIV Package" "OK" "Explicit RPM: $(basename "$TARGET_RPM_FILE")"
    return 0
  fi

  local installed_ver=""
  if rpm -q aiv >/dev/null 2>&1; then
    installed_ver=$(rpm -q --queryformat '%{VERSION}-%{RELEASE}' aiv 2>/dev/null || true)
  fi

  local url_no_query desired_rpm url_ver
  url_no_query="${AIV_DOWNLOAD_URL%%\?*}"
  desired_rpm="${url_no_query##*/}"
  [[ -n "$desired_rpm" && "$desired_rpm" == *.rpm ]] || die "AIV_DOWNLOAD_URL does not point to an RPM file: ${AIV_DOWNLOAD_URL}"
  url_ver=$(echo "$desired_rpm" | sed -E 's/^aiv-([0-9]+\.[0-9]+\.[0-9]+-[0-9]+).*/\1/' || true)

  local local_rpm local_ver=""
  local_rpm=$(find_newest_local_rpm)
  if [[ -n "$local_rpm" && -f "$local_rpm" ]]; then
    local_ver=$(rpm -qp --queryformat '%{VERSION}-%{RELEASE}' "$local_rpm" 2>/dev/null || true)
    if [[ -z "$local_ver" ]]; then
      local_ver=$(basename "$local_rpm" | sed -E 's/^aiv-([0-9]+\.[0-9]+\.[0-9]+-[0-9]+).*/\1/' || true)
    fi
  fi

  # Priority 1: User explicitly changed or passed a custom URL (--url or AIV_DOWNLOAD_URL != DEFAULT)
  if [[ "$EXPLICIT_URL" == "yes" || "$AIV_DOWNLOAD_URL" != "$DEFAULT_AIV_DOWNLOAD_URL" ]]; then
    info "Priority 1: Explicit URL change detected (${AIV_DOWNLOAD_URL})"
    local desired_path="/opt/${desired_rpm}"
    if [[ -f "$desired_path" ]]; then
      ok "Target RPM already present at ${desired_path}. Skipping download."
      TARGET_RPM_FILE="$desired_path"
      record "AIV Package" "OK" "Local match: ${desired_rpm}"
      return 0
    fi
    info "Downloading AIV RPM from URL: ${AIV_DOWNLOAD_URL} ..."
    cd /opt
    if wget -q --show-progress -c -O "${desired_rpm}" "${AIV_DOWNLOAD_URL}"; then
      ok "AIV RPM downloaded successfully into /opt/${desired_rpm}."
      TARGET_RPM_FILE="${desired_path}"
      record "AIV Package" "OK" "Downloaded ${desired_rpm}"
      return 0
    else
      rm -f "/opt/${desired_rpm}" 2>/dev/null || true
      die "Failed to download AIV RPM from ${AIV_DOWNLOAD_URL}."
    fi
  fi

  # Priority 2: Check if a newer local RPM is present in /opt
  if [[ -n "$local_rpm" && -n "$local_ver" ]]; then
    local cmp_inst cmp_url
    cmp_inst=$(ver_compare "$local_ver" "$installed_ver")
    cmp_url=$(ver_compare "$local_ver" "$url_ver")

    # If local RPM in /opt is newer than what is currently installed
    if [[ "$cmp_inst" -eq 1 ]]; then
      info "Priority 2: Found newer locally downloaded RPM in /opt: $(basename "$local_rpm") (v${local_ver} vs installed v${installed_ver:-none})"
      ok "Using local RPM from /opt. Skipping remote download."
      TARGET_RPM_FILE="$local_rpm"
      record "AIV Package" "OK" "Local RPM: $(basename "$local_rpm")"
      return 0
    # Or if local RPM matches or is higher than default URL version
    elif [[ "$cmp_url" -ge 0 && -f "$local_rpm" ]]; then
      TARGET_RPM_FILE="$local_rpm"
      ok "Local RPM in /opt ($(basename "$local_rpm")) satisfies target version (v${local_ver}). Skipping download."
      record "AIV Package" "OK" "Local RPM: $(basename "$local_rpm")"
      return 0
    fi
  fi

  # Fallback: Download from URL if not already present in /opt
  local desired_path="/opt/${desired_rpm}"
  if [[ -f "$desired_path" ]]; then
    ok "AIV RPM already present at ${desired_path}. Skipping download."
    TARGET_RPM_FILE="$desired_path"
    record "AIV Package" "OK" "Already present: ${desired_rpm}"
    return 0
  fi

  if [[ "$DOWNLOAD_AIV" != "yes" ]]; then
    info "Automatic download skipped by user preference."
    if [[ -n "$local_rpm" ]]; then
      TARGET_RPM_FILE="$local_rpm"
      return 0
    fi
    die "No aiv-*.rpm found in /opt and download was disabled!"
  fi

  info "Downloading AIV RPM from: ${AIV_DOWNLOAD_URL} ..."
  cd /opt
  if wget -q --show-progress -c -O "${desired_rpm}" "${AIV_DOWNLOAD_URL}"; then
    ok "AIV RPM package downloaded successfully into /opt/${desired_rpm}."
    TARGET_RPM_FILE="${desired_path}"
    record "AIV Package" "OK" "Downloaded ${desired_rpm}"
  else
    rm -f "/opt/${desired_rpm}" 2>/dev/null || true
    die "Failed to download AIV RPM. Please check network connectivity or place RPM into /opt manually."
  fi
}

find_java() {
  if [[ -x "${AIV_JAVA_DIR}/bin/java" ]]; then
    echo "${AIV_JAVA_DIR}/bin/java"
    return 0
  fi
  return 1
}

step_java() {
  step "Project-Level Java OpenJDK ${JAVA_MAJOR} (${AIV_JAVA_DIR})"

  # Ensure user aiv exists so ownership can be set
  id aiv >/dev/null 2>&1 || useradd -r -d /var/lib/aiv -s /sbin/nologin aiv

  # Remove any system-wide profile hook so we never alter system PATH for other projects
  rm -f /etc/profile.d/aiv-java.sh

  local j=""
  if j="$(find_java)"; then
    ok "Project-level Java already present at ${AIV_JAVA_DIR}"
  else
    info "Setting up isolated project-level OpenJDK ${JAVA_MAJOR} in ${AIV_JAVA_DIR} ..."
    mkdir -p "${AIV_JAVA_DIR}"

    local arch
    arch=$(uname -m)
    local jdk_url="${TEMURIN_21_URL}"
    if [[ "$arch" == "aarch64" ]]; then
      jdk_url="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.6%2B7/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.6_7.tar.gz"
    fi

    # Check if a pre-downloaded tarball exists locally in /opt or current directory
    local local_tgz
    local_tgz=$(find /opt . -maxdepth 1 -name "OpenJDK21U-jdk_${arch}_*.tar.gz" -o -name "OpenJDK21U-jdk_*.tar.gz" -o -name "openjdk-21*.tar.gz" 2>/dev/null | head -n 1)

    local tmp_dir=""
    if [[ -n "$local_tgz" && -f "$local_tgz" ]]; then
      info "Found local OpenJDK tarball: ${local_tgz}. Extracting..."
      tar -xzf "$local_tgz" -C "${AIV_JAVA_DIR}" --strip-components=1
    else
      info "Downloading standalone OpenJDK ${JAVA_MAJOR} from: ${jdk_url} ..."
      tmp_dir="$(mktemp -d)"
      local tgz_file="${tmp_dir}/openjdk21.tar.gz"
      if curl -fsSL --retry 3 --connect-timeout 20 -o "${tgz_file}" "${jdk_url}" || wget -q --show-progress -c -O "${tgz_file}" "${jdk_url}"; then
        info "Extracting OpenJDK into ${AIV_JAVA_DIR} ..."
        tar -xzf "${tgz_file}" -C "${AIV_JAVA_DIR}" --strip-components=1
        rm -rf "${tmp_dir}"
      else
        rm -rf "${tmp_dir}"
        die "Failed to download OpenJDK 21. Please place an OpenJDK 21 tarball in /opt and re-run."
      fi
    fi

    chown -R aiv:aiv "${AIV_JAVA_DIR}"
    j="$(find_java)" || die "Java extracted to ${AIV_JAVA_DIR} but ${AIV_JAVA_DIR}/bin/java not executable."
    ok "Isolated project-level Java ready: $j"
  fi

  JAVA_BIN="$j"
  JAVA_HOME_DIR="${AIV_JAVA_DIR}"
  record "Java" "OK" "Project-isolated (${AIV_JAVA_DIR})"
}

step_postgres() {
  step "PostgreSQL Configuration (Port: ${PG_PORT})"
  if [[ "$INSTALL_PG" != "yes" ]]; then
    info "Skipping local PostgreSQL server setup (Role: ${ROLE})."
    ensure_pkg postgresql psql

    if [[ "$ROLE" == "server2" ]]; then
      info "Verifying remote PostgreSQL connectivity to ${DB_HOST}:${PG_PORT}..."
      local pg_home
      pg_home=$(getent passwd postgres 2>/dev/null | cut -d: -f6 || echo "/var/lib/pgsql")
      [[ -z "$pg_home" ]] && pg_home="/var/lib/pgsql"

      for passfile in "/root/.pgpass" "${pg_home}/.pgpass"; do
        mkdir -p "$(dirname "$passfile")"
        echo "${DB_HOST}:${PG_PORT}:*:postgres:${DB_PASSWORD}" >> "$passfile"
        chmod 600 "$passfile"
        [[ "$passfile" == *pgsql* ]] && chown postgres:postgres "$passfile" 2>/dev/null || true
      done

      if PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${PG_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT 1;" >/dev/null 2>&1; then
        ok "Successfully verified remote connection to PostgreSQL on ${DB_HOST}:${PG_PORT}!"
        record "PostgreSQL" "OK" "Remote DB on ${DB_HOST}"
      else
        warn "Could not connect to PostgreSQL on ${DB_HOST}:${PG_PORT} yet. Ensure Server 1 firewall and pg_hba.conf are updated."
        record "PostgreSQL" "WARN" "Remote DB at ${DB_HOST} pending reachability"
      fi
    else
      record "PostgreSQL" "SKIPPED" "Using remote/existing DB"
    fi
    return 0
  fi

  if rpm -q postgresql-server >/dev/null 2>&1 && systemctl is-active --quiet postgresql; then
    ok "PostgreSQL server already installed and running. Skipping package install."
  else
    dnf install -y postgresql-server postgresql-contrib
    postgresql-setup --initdb || true
    systemctl enable --now postgresql
  fi

  info "Configuring PostgreSQL credentials, .pgpass, and database '${DB_NAME}'..."

  # 1. Store .pgpass with wildcards for both root and postgres BEFORE any psql calls
  local pg_home
  pg_home=$(getent passwd postgres 2>/dev/null | cut -d: -f6 || echo "/var/lib/pgsql")
  [[ -z "$pg_home" ]] && pg_home="/var/lib/pgsql"

  for passfile in "/root/.pgpass" "${pg_home}/.pgpass"; do
    mkdir -p "$(dirname "$passfile")"
    cat << EOF > "$passfile"
*:*:*:postgres:${DB_PASSWORD}
127.0.0.1:*:*:postgres:${DB_PASSWORD}
localhost:*:*:postgres:${DB_PASSWORD}
EOF
    chmod 600 "$passfile"
    [[ "$passfile" == *pgsql* ]] && chown postgres:postgres "$passfile" 2>/dev/null || true
  done
  export PGPASSWORD="${DB_PASSWORD}"

  # 2. Set password and create database (pass PGPASSWORD explicitly to subshell)
  sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -c "ALTER USER postgres WITH PASSWORD '${DB_PASSWORD}';" 2>/dev/null || \
  PGPASSWORD="${DB_PASSWORD}" psql -h 127.0.0.1 -p "${PG_PORT}" -U postgres -c "ALTER USER postgres WITH PASSWORD '${DB_PASSWORD}';" 2>/dev/null || true

  if ! sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" 2>/dev/null | grep -q 1; then
    sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -c "CREATE DATABASE ${DB_NAME};"
    ok "Database '${DB_NAME}' created successfully."
  else
    ok "Database '${DB_NAME}' already exists."
  fi

  # Pre-create schema 'Default' for AIV reporting
  sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -d "${DB_NAME}" -c 'CREATE SCHEMA IF NOT EXISTS "Default";' 2>/dev/null || true

  # Pre-create extension 'intarray'
  sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -d "${DB_NAME}" -c 'CREATE EXTENSION IF NOT EXISTS intarray;' 2>/dev/null || true

  # Pre-create / compile and register 'pgvector' for AIV AI capabilities
  info "Checking pgvector extension in database '${DB_NAME}'..."
  if sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -d "${DB_NAME}" -tAc "SELECT 1 FROM pg_extension WHERE extname='vector';" 2>/dev/null | grep -q 1; then
    ok "pgvector extension already installed and active in database '${DB_NAME}'."
  else
    info "Installing pgvector build prerequisites (git, gcc, make, postgresql-server-devel, redhat-rpm-config)..."
    dnf install -y git gcc make postgresql-server-devel redhat-rpm-config >/dev/null 2>&1 || \
      dnf --enablerepo=crb install -y postgresql-server-devel redhat-rpm-config >/dev/null 2>&1 || true

    if command -v pg_config >/dev/null 2>&1; then
      info "Compiling and installing pgvector (v0.7.4) from source..."
      local pgv_tmp
      pgv_tmp=$(mktemp -d /tmp/pgvector-build.XXXXXX)
      if git clone --depth 1 --branch v0.7.4 https://github.com/pgvector/pgvector.git "$pgv_tmp" >/dev/null 2>&1; then
        (cd "$pgv_tmp" && make clean >/dev/null 2>&1 && make >/dev/null 2>&1 && make install >/dev/null 2>&1) || true
        rm -rf "$pgv_tmp"
      fi
    else
      warn "pg_config not found. Could not compile pgvector automatically."
    fi

    if sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -d "${DB_NAME}" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1; then
      ok "pgvector extension successfully enabled in database '${DB_NAME}'."
    else
      warn "Failed to enable pgvector in '${DB_NAME}'. Verify manual build if AIV AI Server is used."
    fi
  fi

  # 3. Configure pg_hba.conf: Keep peer auth for local postgres, enforce scram-sha-256 for all others
  local pg_hba_file
  pg_hba_file=$(sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -t -A -c "SHOW hba_file;" 2>/dev/null || echo "/var/lib/pgsql/data/pg_hba.conf")
  [[ -f "$pg_hba_file" ]] || pg_hba_file="/var/lib/pgsql/data/pg_hba.conf"

  # Ensure local postgres user has peer auth so internal maintenance commands never prompt
  if ! grep -qE '^local\s+all\s+postgres\s+peer' "$pg_hba_file"; then
    sed -i -E 's/^(local\s+all\s+all\s+.*)/local   all             postgres                                peer\n\1/' "$pg_hba_file" 2>/dev/null || true
  fi

  sed -i -E 's/^(local\s+all\s+all\s+)peer/\1scram-sha-256/' "$pg_hba_file"
  sed -i -E 's/^(host\s+all\s+all\s+127\.0\.0\.1\/32\s+)(ident|md5)/\1scram-sha-256/' "$pg_hba_file"
  sed -i -E 's/^(host\s+all\s+all\s+::1\/128\s+)(ident|md5)/\1scram-sha-256/' "$pg_hba_file"

  # 4. If remote worker IPs are specified, open listener and configure whitelisted access
  if [[ -n "$ALLOW_IPS" ]]; then
    local pg_conf
    pg_conf=$(sudo -u postgres env PGPASSWORD="${DB_PASSWORD}" psql -p "${PG_PORT}" -t -A -c "SHOW config_file;" 2>/dev/null || echo "/var/lib/pgsql/data/postgresql.conf")
    [[ -f "$pg_conf" ]] || pg_conf="/var/lib/pgsql/data/postgresql.conf"
    if [[ -f "$pg_conf" ]]; then
      info "Enabling remote listener (listen_addresses = '*') in postgresql.conf..."
      sed -i -E "s/^[# ]*listen_addresses\s*=.*/listen_addresses = '*'/" "$pg_conf"
    fi

    info "Whitelisting remote IP(s): ${ALLOW_IPS} in pg_hba.conf and firewalld..."
    IFS=',' read -ra IP_ARRAY <<< "$ALLOW_IPS"
    for rip in "${IP_ARRAY[@]}"; do
      rip=$(echo "$rip" | xargs)
      [[ -z "$rip" ]] && continue
      if ! grep -q "$rip" "$pg_hba_file"; then
        echo "host    all             all             ${rip}/32               scram-sha-256" >> "$pg_hba_file"
        ok "Added ${rip}/32 to pg_hba.conf"
      fi

      # Configure firewalld rich rules specifically for whitelisted IP
      if systemctl is-active --quiet firewalld; then
        IFS=',' read -ra PORT_ARRAY <<< "$ALLOW_PORTS"
        for rport in "${PORT_ARRAY[@]}"; do
          rport=$(echo "$rport" | xargs)
          [[ -z "$rport" ]] && continue
          firewall-cmd --permanent --add-rich-rule="rule family=\"ipv4\" source address=\"${rip}\" port port=\"${rport}\" protocol=\"tcp\" accept" 2>/dev/null || true
        done
        ok "Configured firewalld rules for ${rip} on ports ${ALLOW_PORTS}"
      fi
    done
    systemctl is-active --quiet firewalld && firewall-cmd --reload 2>/dev/null || true
  fi

  systemctl restart postgresql
  ok "PostgreSQL server and database '${DB_NAME}' ready (zero prompts confirmed)."
  record "PostgreSQL" "OK" "Running on port ${PG_PORT}"
}

download_and_install_kafka() {
  local tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  local dest="/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
  local opt_tgz="/opt/${tgz}"

  # Check if a valid Kafka archive is already cached in /opt
  if [[ -f "$opt_tgz" && -s "$opt_tgz" ]] && tar -tzf "$opt_tgz" >/dev/null 2>&1; then
    ok "Found valid cached Kafka archive at ${opt_tgz}. Skipping download."
  else
    info "Downloading Apache Kafka ${KAFKA_VERSION} (~120 MB)..."
    local downloaded="no"
    # Fast global CDN mirror (dlcdn) first, then downloads and archive fallback
    for base in https://dlcdn.apache.org/kafka https://downloads.apache.org/kafka https://archive.apache.org/dist/kafka; do
      local url="${base}/${KAFKA_VERSION}/${tgz}"
      info "Fetching from: ${url}"
      if command -v wget >/dev/null 2>&1; then
        if wget -q --show-progress -c -O "$opt_tgz" "$url"; then
          downloaded="yes"
          break
        fi
      elif curl -fL --progress-bar --retry 2 --connect-timeout 15 -o "$opt_tgz" "$url"; then
        downloaded="yes"
        break
      fi
    done
    [[ "$downloaded" == "yes" && -s "$opt_tgz" ]] || die "Could not download Kafka ${KAFKA_VERSION}."
  fi

  info "Extracting Kafka into /opt..."
  tar -xzf "$opt_tgz" -C /opt
  chown -R root:root "$dest"
  ln -sfn "$dest" "$KAFKA_LINK"
  ok "Kafka ${KAFKA_VERSION} deployed to ${KAFKA_LINK}"
}

step_kafka() {
  step "Apache Kafka (Port: ${KAFKA_PORT})"
  if [[ "$INSTALL_KAFKA" != "yes" ]]; then
    local remote_khost="${KAFKA_REMOTE_HOST:-$DB_HOST}"
    if [[ -n "$remote_khost" && "$remote_khost" != "127.0.0.1" ]]; then
      info "Kafka broker installation skipped locally (Connecting to cluster broker on ${remote_khost}:${KAFKA_PORT})."
      if python3 -c "import socket; s=socket.socket(); s.settimeout(3); s.connect(('${remote_khost}', int('${KAFKA_PORT}'))); s.close()" 2>/dev/null || \
         timeout 3 bash -c "</dev/tcp/${remote_khost}/${KAFKA_PORT}" 2>/dev/null; then
        ok "Successfully verified remote connection to Kafka broker on ${remote_khost}:${KAFKA_PORT}!"
        record "Kafka" "OK" "Remote broker on ${remote_khost}:${KAFKA_PORT}"
      else
        warn "Could not connect to Kafka broker on ${remote_khost}:${KAFKA_PORT} yet. Ensure Server 1 firewall allows this server."
        record "Kafka" "WARN" "Remote broker at ${remote_khost} pending reachability"
      fi
    else
      info "Kafka installation skipped by user choice. System will run in Single-Node mode."
      record "Kafka" "SKIPPED" "Single-node mode active"
    fi
    return 0
  fi

  local srv_ip
  srv_ip=$(curl -s --connect-timeout 3 https://api.ipify.org 2>/dev/null || ip -4 route get 1.1.1.1 2>/dev/null | awk '{print $7}' || hostname -I | awk '{print $1}')

  if [[ -x "${KAFKA_LINK}/bin/kafka-server-start.sh" ]]; then
    ok "Kafka binary already present at ${KAFKA_LINK}"
  else
    download_and_install_kafka
  fi

  id kafka >/dev/null 2>&1 || useradd --system --home-dir /var/lib/kafka --shell /sbin/nologin kafka
  install -d -o kafka -g kafka -m 0750 /var/lib/kafka /var/lib/kafka/data /var/log/kafka
  install -d -o root  -g kafka -m 0750 /etc/kafka

  cat << EOF > /etc/kafka/server.properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://0.0.0.0:${KAFKA_PORT},CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://${srv_ip}:${KAFKA_PORT}
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/var/lib/kafka/data
num.partitions=8
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
  chown root:kafka /etc/kafka/server.properties
  chmod 0640 /etc/kafka/server.properties

  if [[ ! -f /var/lib/kafka/data/meta.properties ]]; then
    local cid
    cid=$(env JAVA_HOME="${JAVA_HOME_DIR}" PATH="${JAVA_HOME_DIR}/bin:$PATH" /opt/kafka/bin/kafka-storage.sh random-uuid)
    sudo -u kafka env JAVA_HOME="${JAVA_HOME_DIR}" PATH="${JAVA_HOME_DIR}/bin:$PATH" /opt/kafka/bin/kafka-storage.sh format -t "$cid" -c /etc/kafka/server.properties
  fi

  cat << EOF > /etc/systemd/system/kafka.service
[Unit]
Description=Apache Kafka (KRaft mode)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="JAVA_HOME=${JAVA_HOME_DIR}"
Environment="PATH=${JAVA_HOME_DIR}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
Environment="LOG_DIR=/var/log/kafka"
Environment="KAFKA_HEAP_OPTS=-Xms512m -Xmx1g"
ExecStart=${KAFKA_LINK}/bin/kafka-server-start.sh /etc/kafka/server.properties
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable --now kafka

  # Automatically create/expand task-topic to 8 partitions for cluster concurrency
  local attempts=0
  while ! ss -tlpn | grep -q -E "(:|\])${KAFKA_PORT}\b" && [[ $attempts -lt 15 ]]; do
    sleep 2
    attempts=$((attempts + 1))
  done
  env JAVA_HOME="${JAVA_HOME_DIR}" PATH="${JAVA_HOME_DIR}/bin:$PATH" /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:${KAFKA_PORT} --create --if-not-exists --topic task-topic --partitions 8 --replication-factor 1 >/dev/null 2>&1 || true
  env JAVA_HOME="${JAVA_HOME_DIR}" PATH="${JAVA_HOME_DIR}/bin:$PATH" /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:${KAFKA_PORT} --alter --topic task-topic --partitions 8 >/dev/null 2>&1 || true

  # Secure Kafka broker via Linux firewall isolation (nftables or firewalld)
  local all_worker_ips="${ALLOW_IPS:-}"
  local existing_allowed
  existing_allowed=$(detect_existing_whitelisted_ips)
  [[ -n "$existing_allowed" ]] && all_worker_ips="${all_worker_ips},${existing_allowed}"

  if command -v nft >/dev/null 2>&1; then
    info "Applying Linux nftables kernel firewall isolation for Kafka (Port: ${KAFKA_PORT})..."
    nft add table inet filter 2>/dev/null || true
    nft 'add chain inet filter input { type filter hook input priority 0; policy accept; }' 2>/dev/null || true
    nft flush chain inet filter input 2>/dev/null || true
    nft add rule inet filter input ip saddr 127.0.0.1 tcp dport "${KAFKA_PORT}" accept 2>/dev/null || true
    [[ -n "$srv_ip" ]] && nft add rule inet filter input ip saddr "${srv_ip}" tcp dport "${KAFKA_PORT}" accept 2>/dev/null || true
    if [[ -n "$all_worker_ips" ]]; then
      IFS=',' read -ra W_ARR <<< "$all_worker_ips"
      for wip in "${W_ARR[@]}"; do
        wip=$(echo "$wip" | xargs)
        [[ -z "$wip" || "$wip" == "127.0.0.1" || "$wip" == "$srv_ip" ]] && continue
        nft add rule inet filter input ip saddr "${wip}" tcp dport "${KAFKA_PORT}" accept 2>/dev/null || true
      done
    fi
    nft add rule inet filter input tcp dport "${KAFKA_PORT}" drop 2>/dev/null || true
    systemctl enable --now nftables 2>/dev/null || true
    mkdir -p /etc/sysconfig
    nft list ruleset > /etc/sysconfig/nftables.conf 2>/dev/null || true
    ok "Kafka port ${KAFKA_PORT} isolated via nftables: restricted to localhost, Server 1, and cluster worker nodes."
  elif systemctl is-active --quiet firewalld; then
    info "Applying Linux firewalld isolation for Kafka (Port: ${KAFKA_PORT})..."
    firewall-cmd --permanent --zone=public --remove-port="${KAFKA_PORT}/tcp" 2>/dev/null || true
    firewall-cmd --permanent --add-rich-rule="rule family=\"ipv4\" source address=\"127.0.0.1\" port port=\"${KAFKA_PORT}\" protocol=\"tcp\" accept" 2>/dev/null || true
    [[ -n "$srv_ip" ]] && firewall-cmd --permanent --add-rich-rule="rule family=\"ipv4\" source address=\"${srv_ip}\" port port=\"${KAFKA_PORT}\" protocol=\"tcp\" accept" 2>/dev/null || true
    if [[ -n "$all_worker_ips" ]]; then
      IFS=',' read -ra W_ARR <<< "$all_worker_ips"
      for wip in "${W_ARR[@]}"; do
        wip=$(echo "$wip" | xargs)
        [[ -z "$wip" || "$wip" == "127.0.0.1" || "$wip" == "$srv_ip" ]] && continue
        firewall-cmd --permanent --add-rich-rule="rule family=\"ipv4\" source address=\"${wip}\" port port=\"${KAFKA_PORT}\" protocol=\"tcp\" accept" 2>/dev/null || true
      done
    fi
    firewall-cmd --reload 2>/dev/null || true
    ok "Kafka port ${KAFKA_PORT} isolated via firewalld: restricted to localhost, Server 1, and cluster worker nodes."
  fi

  ok "Kafka service active on port ${KAFKA_PORT} (configured with 8 cluster partitions)."
  record "Kafka" "OK" "Running on port ${KAFKA_PORT} (8 partitions)"
}

step_s3_storage() {
  step "Shared Storage Mount"
  if [[ "$INSTALL_S3" != "yes" ]]; then
    info "S3 Storage skipped. AIV will use local repository at /var/lib/aiv/repository."
    S3_MOUNT_POINT="/var/lib/aiv"
    record "Storage" "LOCAL" "Using local storage /var/lib/aiv"
    return 0
  fi

  mkdir -p "$S3_MOUNT_POINT"
  echo "${S3_ACCESS_KEY}:${S3_SECRET_KEY}" > /etc/passwd-s3fs
  chmod 600 /etc/passwd-s3fs

  id aiv >/dev/null 2>&1 || useradd -r -d /var/lib/aiv -s /sbin/nologin aiv
  local AIV_UID AIV_GID
  AIV_UID=$(id -u aiv)
  AIV_GID=$(id -g aiv)

  cat << EOF > /etc/systemd/system/aiv-s3fs.service
[Unit]
Description=Contabo S3 Shared Storage Mount for AIV
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
ExecStartPre=/usr/bin/mkdir -p ${S3_MOUNT_POINT}
ExecStart=/usr/bin/s3fs ${S3_BUCKET} ${S3_MOUNT_POINT} -f -o passwd_file=/etc/passwd-s3fs -o url=${S3_URL} -o use_path_request_style -o allow_other -o umask=0000 -o uid=${AIV_UID} -o gid=${AIV_GID} -o stat_cache_expire=300 -o max_stat_cache_size=100000 -o enable_noobj_cache -o parallel_count=10
ExecStop=/usr/bin/umount -l ${S3_MOUNT_POINT}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable --now aiv-s3fs.service
  sleep 2

  if mountpoint -q "$S3_MOUNT_POINT"; then
    ok "S3 Shared Storage successfully mounted at $S3_MOUNT_POINT"
    record "Storage" "OK" "S3 mounted at $S3_MOUNT_POINT"
  else
    warn "Mount check delayed; continuing."
  fi
}

step_install_aiv_rpm() {
  step "Install AIV RPM Package"
  local rpm_file=""
  if [[ -n "${TARGET_RPM_FILE:-}" && -f "$TARGET_RPM_FILE" ]]; then
    rpm_file="$TARGET_RPM_FILE"
  else
    rpm_file=$(find_newest_local_rpm)
    if [[ -z "$rpm_file" || ! -f "$rpm_file" ]]; then
      local desired_rpm url_no_query
      url_no_query="${AIV_DOWNLOAD_URL%%\?*}"
      desired_rpm="${url_no_query##*/}"
      [[ -n "$desired_rpm" && -f "/opt/${desired_rpm}" ]] && rpm_file="/opt/${desired_rpm}"
    fi
  fi
  [[ -n "$rpm_file" && -f "$rpm_file" ]] || die "No valid aiv-*.rpm found in /opt! Place RPM in /opt or specify valid --url."

  local rpm_nevra installed_nevra=""
  rpm_nevra=$(rpm -qp --queryformat '%{NEVRA}' "$rpm_file" 2>/dev/null || basename "$rpm_file")
  if rpm -q aiv >/dev/null 2>&1; then
    installed_nevra=$(rpm -q --queryformat '%{NEVRA}' aiv 2>/dev/null || true)
  fi

  if [[ -n "$installed_nevra" ]]; then
    if [[ "$installed_nevra" == "$rpm_nevra" ]]; then
      ok "AIV RPM already at desired version: ${installed_nevra}"
      record "AIV RPM" "OK" "Already at ${installed_nevra}"
    else
      info "Upgrading AIV RPM: ${installed_nevra} -> ${rpm_nevra}"
      # Stop app services before package replace so files are not busy
      systemctl stop aiv1.service aiv2.service aiv_universalauth.service aiv.service 2>/dev/null || true

      # Temporarily unmount any shared NFS/network mounts under /var/lib/aiv
      # so RPM cpio unpack/chown succeeds without NFS ownership squashing errors
      local unmounted_shares=()
      for m_point in /var/lib/aiv/repository /var/lib/aiv/logs; do
        if mountpoint -q "$m_point" 2>/dev/null; then
          info "Temporarily unmounting shared mount at ${m_point} for clean RPM unpack..."
          umount -l "$m_point" 2>/dev/null || true
          unmounted_shares+=("$m_point")
        fi
      done

      dnf upgrade -y "$rpm_file" || dnf reinstall -y "$rpm_file" || rpm -Uvh --force "$rpm_file"

      # Restore shared network mounts immediately after RPM unpack
      if [[ ${#unmounted_shares[@]} -gt 0 ]]; then
        info "Restoring shared storage mounts..."
        mount -a 2>/dev/null || true
      fi
      ok "AIV RPM upgraded to ${rpm_nevra}"
      record "AIV RPM" "OK" "Upgraded to ${rpm_nevra}"
    fi
  else
    info "Installing $rpm_file ..."
    dnf install -y "$rpm_file"
    ok "AIV RPM installed: ${rpm_nevra}"
    record "AIV RPM" "OK" "Installed ${rpm_nevra}"
  fi

  # Ensure launcher scripts use project-level Java
  for launcher in /usr/bin/aiv /usr/bin/universalauth /usr/bin/aiv_universalauth; do
    if [[ -f "$launcher" ]]; then
      sed -i "s#/usr/bin/java#${AIV_JAVA_DIR}/bin/java#g" "$launcher"
    fi
  done

  # Inject dynamic per-instance port resolution (AIV1_PORT / AIV2_PORT from /var/lib/aiv/enviroment)
  cat << 'EOF' > /var/lib/aiv/resolve_port.sh
#!/bin/bash
# Automatically load AIV1_PORT or AIV2_PORT from /var/lib/aiv/enviroment
if [[ -f /var/lib/aiv/enviroment ]]; then
  . /var/lib/aiv/enviroment
fi
if [[ "$AIV_NODE_INSTANCE" == "1" ]]; then
  export aiv_port="${AIV1_PORT:-8082}"
elif [[ "$AIV_NODE_INSTANCE" == "2" ]]; then
  export aiv_port="${AIV2_PORT:-8083}"
fi
EOF
  chmod 755 /var/lib/aiv/resolve_port.sh
  chown aiv:aiv /var/lib/aiv/resolve_port.sh 2>/dev/null || true

  if [[ -f /usr/bin/aiv ]]; then
    if ! grep -q "resolve_port.sh" /usr/bin/aiv 2>/dev/null; then
      sed -i '2i [ -f /var/lib/aiv/resolve_port.sh ] && . /var/lib/aiv/resolve_port.sh' /usr/bin/aiv
    fi
    if ! grep -q "app.leader.election.enabled" /usr/bin/aiv 2>/dev/null; then
      sed -i 's#-Dspark.io.mode.default=NIO#-Dspark.io.mode.default=NIO -Dapp.leader.election.enabled=${APP_LEADER_ELECTION_ENABLED:-true}#g' /usr/bin/aiv
    fi
  fi

  # Also update aiv_universalauth.service if it hardcodes /usr/bin/java and configure log path
  if [[ -f /etc/systemd/system/aiv_universalauth.service ]]; then
    sed -i "s#/usr/bin/java#${AIV_JAVA_DIR}/bin/java#g" /etc/systemd/system/aiv_universalauth.service
    if ! grep -q "LOG_DIR=" /etc/systemd/system/aiv_universalauth.service 2>/dev/null; then
      sed -i '/\[Service\]/a Environment="LOG_DIR=/var/lib/aiv/logs/universalauth"' /etc/systemd/system/aiv_universalauth.service
    fi
    systemctl daemon-reload
  fi

  # Configure UniversalAuth log directory in /var/lib/aiv/logs/universalauth
  if [[ -f /var/lib/aiv/universalauth/application.yml ]]; then
    sed -i 's#logDir:.*#logDir: ${LOG_DIR:/var/lib/aiv/logs/universalauth}#g' /var/lib/aiv/universalauth/application.yml
  fi
  mkdir -p /var/lib/aiv/logs/universalauth
  chown -R aiv:aiv /var/lib/aiv/logs 2>/dev/null || true

  systemctl stop aiv.service 2>/dev/null || true
  systemctl disable aiv.service 2>/dev/null || true
  systemctl mask aiv.service 2>/dev/null || true

  if [[ "$INSTALL_S3" == "yes" ]] && command -v s3cmd >/dev/null 2>&1; then
    if [[ -f /var/lib/aiv/.s3_synced ]]; then
      ok "Repository files already synced to S3 previously."
    else
      info "Seeding repository files to S3 bucket ($S3_BUCKET) ..."
      s3cmd --access_key="$S3_ACCESS_KEY" \
            --secret_key="$S3_SECRET_KEY" \
            --host="$S3_URL" \
            --host-bucket="${S3_URL}/%(bucket)" \
            sync /var/lib/aiv/repository s3://${S3_BUCKET}/ 2>/dev/null || true
      touch /var/lib/aiv/.s3_synced
      ok "Repository synced to S3."
    fi
  fi
}

step_configure_aiv_cluster() {
  step "Configure Applications, Environment & Routing (Role: ${ROLE})"
  local srv_ip
  srv_ip=$(get_server_ip)

  if [[ "$INSTALL_S3" != "yes" ]]; then
    [[ -L /var/log/aiv ]] && rm -f /var/log/aiv
    [[ -L /var/lib/aiv/logs ]] && rm -f /var/lib/aiv/logs
  fi
  mkdir -p /var/lib/aiv/logs /var/log/aiv
  chown -R aiv:aiv /var/lib/aiv/logs /var/log/aiv 2>/dev/null || true

  local auth_host="${srv_ip}"
  local kafka_bootstrap="${DB_HOST}:${KAFKA_PORT}"
  local node1_liquibase="true"
  local node2_liquibase="false"
  local task_mode="multi"
  local leader_election="true"
  local public_aiv_url="http://${srv_ip}:${AIV_LB_PORT}/aiv"

  # [FUTURE F5 INTEGRATION - UNCOMMENT WHEN READY]
  # if [[ "$LB_TYPE" == "f5" ]]; then
  #   local vip_host="${F5_VIP:-$srv_ip}"
  #   public_aiv_url="${F5_PROTO}://${vip_host}:${F5_PORT}/aiv"
  #   if [[ "$ROLE" == "server2" ]]; then
  #     auth_host="${SERVER1_IP}"
  #     kafka_bootstrap="${SERVER1_IP}:${KAFKA_PORT}"
  #     node1_liquibase="false"
  #     node2_liquibase="false"
  #     task_mode="multi"
  #     leader_election="true"
  #   fi
  # el
  if [[ "$ROLE" == "server2" ]]; then
    auth_host="${SERVER1_IP}"
    kafka_bootstrap="${SERVER1_IP}:${KAFKA_PORT}"
    node1_liquibase="false" # CRITICAL: Worker server nodes must NEVER run database migrations
    node2_liquibase="false"
    task_mode="multi"
    leader_election="true" # All cluster nodes participate in election so only lock holder bootstraps
    public_aiv_url="http://${SERVER1_IP}:${AIV_LB_PORT}/aiv"
  elif [[ "$INSTALL_KAFKA" != "yes" ]]; then
    task_mode="single"
    leader_election="false"
    if [[ "$INSTALL_NGINX" != "yes" ]]; then
      public_aiv_url="http://${srv_ip}:${AIV1_PORT}/aiv"
    fi
  elif [[ "$INSTALL_NGINX" != "yes" ]]; then
    public_aiv_url="http://${srv_ip}:${AIV1_PORT}/aiv"
  fi

  local sys_timezone
  sys_timezone=$(timedatectl show --property=Timezone --value 2>/dev/null || true)
  if [[ -z "$sys_timezone" && -L /etc/localtime ]]; then
    sys_timezone=$(readlink /etc/localtime | sed 's#.*/zoneinfo/##')
  fi
  sys_timezone="${sys_timezone:-UTC}"

  # 0. Safety net: save the current config files before they are rewritten below.
  local cfg_backup_dir old_env_copy=""
  cfg_backup_dir="/var/lib/aiv/config-backups/$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$cfg_backup_dir"
  chmod 700 /var/lib/aiv/config-backups "$cfg_backup_dir" 2>/dev/null || true
  if [[ -f /var/lib/aiv/enviroment ]]; then
    cp -p /var/lib/aiv/enviroment "$cfg_backup_dir/enviroment" && old_env_copy="$cfg_backup_dir/enviroment"
  fi
  if [[ -f /var/lib/aiv/repository/econfig/application.yml ]]; then
    cp -p /var/lib/aiv/repository/econfig/application.yml "$cfg_backup_dir/application.yml" || true
  fi
  if rmdir "$cfg_backup_dir" 2>/dev/null; then
    :   # nothing existed yet (fresh install) - nothing to save
  else
    info "Previous config saved to ${cfg_backup_dir} (enviroment, application.yml)"
  fi

  # 1. Update /var/lib/aiv/enviroment
  cat << EOF > /var/lib/aiv/enviroment
export JAVA_HOME=${JAVA_HOME_DIR}
export PATH=${JAVA_HOME_DIR}/bin:\$PATH
export APP_TIMEZONE=${sys_timezone}
export aiv_base=${S3_MOUNT_POINT}

export AUTH_PORT=${AUTH_PORT}
export AIV1_PORT=${AIV1_PORT}
export AIV2_PORT=${AIV2_PORT}
export DATABASE_USERNAME=${DB_USER}
export DATABASE_PASSWORD=${DB_PASSWORD}
export DATABASE_URL=jdbc:postgresql://${DB_HOST}:${PG_PORT}/${DB_NAME}
export DATABASE_POOL_MAX=30
export DATABASE_POOL_MIN=5

export AUTH_UI_BASE_URL=http://${auth_host}:${AUTH_PORT}
export AUTH_CORS_AIV_ORIGIN_PATTERN=http://*:[*]
export AUTH_SSO_CALLBACK=${public_aiv_url}/sso/callback

export APP_ROLE=all
export APP_LEADER_ELECTION_ENABLED=${leader_election}
export APP_TASK_MANAGER_MODE=${task_mode}
export APP_EXECUTOR_CONCURRENCY=2
export SPRING_KAFKA_BOOTSTRAP_SERVERS=${kafka_bootstrap}

export aiv_db_user=${DB_USER}
export aiv_db_password=${DB_PASSWORD}
export aiv_db_url=jdbc:postgresql://${DB_HOST}:${PG_PORT}/${DB_NAME}

export security_db_user=${DB_USER}
export security_db_password=${DB_PASSWORD}
export security_db_url=jdbc:postgresql://${DB_HOST}:${PG_PORT}/${DB_NAME}?currentSchema=security

export AIV_SUPERSECURITY=http://${auth_host}:${AUTH_PORT}
export AIV_SSO_PUBLIC_BASE_URL=${public_aiv_url}
export AIV_SSO_CLIENT_ID=aiv

export LB_TYPE=${LB_TYPE}
# [FUTURE F5 INTEGRATION - UNCOMMENT WHEN READY]
# export F5_VIP=${F5_VIP:-}
# export F5_PORT=${F5_PORT:-8086}
# export F5_PROTO=${F5_PROTO:-http}
EOF

  # Keep your own additions: copy every "export NAME=..." line from the old file
  # whose NAME the new file does not define.
  # LOG_DIR is NOT carried over (same log behaviour as the pre-backup _7 script):
  # AIV logs go to /var/lib/aiv/logs after every re-run. Add LOG_DIR again by hand if needed.
  if [[ -n "$old_env_copy" ]]; then
    local kept=0 old_line old_var
    while IFS= read -r old_line; do
      [[ "$old_line" =~ ^export[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)= ]] || continue
      old_var="${BASH_REMATCH[1]}"
      [[ "$old_var" == "LOG_DIR" ]] && continue
      if ! grep -qE "^[[:space:]]*export[[:space:]]+${old_var}=" /var/lib/aiv/enviroment; then
        if [[ "$kept" -eq 0 ]]; then
          printf '\n# Preserved from previous configuration\n' >> /var/lib/aiv/enviroment
        fi
        printf '%s\n' "$old_line" >> /var/lib/aiv/enviroment
        kept=$((kept + 1))
      fi
    done < "$old_env_copy"
    if [[ "$kept" -gt 0 ]]; then
      info "Kept ${kept} custom setting(s) from your previous enviroment file."
    fi
  fi

  # 2. Update application.yml (the previous copy is saved in ${cfg_backup_dir})
  cat << 'EOF' > /var/lib/aiv/repository/econfig/application.yml
server:
  compression:
    enabled: true
    mime-types: application/json, text/html, text/xml, text/plain,text/css, text/javascript, application/javascript, application/octet-stream
    min-response-size: 1024
  servlet:
    context-path: /aiv
  port: ${aiv_port:8080}
  forward-headers-strategy: framework

spring:
  autoconfigure:
    exclude:
      - org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration
      - org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration
      - org.springframework.boot.actuate.autoconfigure.metrics.mongo.MongoMetricsAutoConfiguration
      - org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration
  resources:
    static-locations: classpath:/static/,file:///${aiv_base}/repository/images/
  jackson:
    serialization:
      WRITE_DATES_AS_TIMESTAMPS: false
    time-zone: UTC
  datasource:
    url: ${aiv_db_url}
    username: ${aiv_db_user}
    password: ${aiv_db_password}
    driverClassName: org.postgresql.Driver
  datasource1:
    url: ${security_db_url}
    username: ${security_db_user}
    password: ${security_db_password}
    driverClassName: org.postgresql.Driver
  mvc:
    pathmatch:
      matching-strategy: ANT_PATH_MATCHER
  jpa:
    hibernate:
      ddl-auto: ${JPA_DDL_AUTO:validate}
  liquibase:
    aiv:
      enabled: ${LIQUIBASE_AIV_ENABLED:true}
      change-log: classpath:db/changelog/db.changelog-aiv.sql
    security:
      enabled: ${LIQUIBASE_SECURITY_ENABLED:false}
      change-log: classpath:db/changelog/db.changelog-security.sql
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: aiv-task-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: com.aiv.cluster.MapDeserializer
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: com.aiv.cluster.MapSerializer

datasources:
  dslist[0]: '{"jndi-name":"jdbc/ActiveIDB","driver-class-name":"org.postgresql.Driver","url":"${aiv_db_url}","username":"${aiv_db_user}","password":"${aiv_db_password}"}'

app:
  slatKey: 0123456789abcdef
  ivspec: fedcba9876543210
  ltoken: aiv_jwt_secret_f83Kf92Jd8sLq7WzYxP4TnR6bH1M5c
  ekey: 0123456789abcdef0123456789abcdef
  eurl: https://aivlic.app.oneaiv.com
  enckey: 0fc79bc2a4f6411ca1a9cb325ee8a3a2
  masterkey: AiVecretKey91345
  imgLocation: ${aiv_base}/repository/images/
  appLocation: ${aiv_base}/repository/APP/
  repositoryLocation: ${aiv_base}/repository
  logDir: /var/lib/aiv/logs
  deliveryLocation: ${aiv_base}/repository/delivery
  database: postgresql
  securityClass: unused.UniversalAuthHttp
  isJira: false
  noofreports: 150
  ssoredirect: false
  marketplaceurl: https://marketplace.aivhub.com/api
  WEBHOOK_SECRET: B4O6XlaeBLAZVSo5g+GrVDz9BM/LcD5EMn6OFoaCIY1pL4qDk9pqdqlnR7Tv7zJ4
  role: ${APP_ROLE:all}
  leader:
    election:
      enabled: ${APP_LEADER_ELECTION_ENABLED:false}
      heartbeat.ms: 10000
      lock-at-most-for: 30s
      lock-at-least-for: 0s
  schedule:
    jitter.ms: ${APP_SCHEDULE_JITTER_MS:0}
  executor:
    concurrency: ${APP_EXECUTOR_CONCURRENCY:2}
  universalauth:
    api-token: ${UNIVERSALAUTH_API_TOKEN:}
    local-verify:
      rotation-interval-seconds: ${JWT_ACCESS_REUSE_SECONDS:900}
  sso:
    state-ttl-seconds: 600
    allowed-return-path-prefixes: /aiv/,/
    browser-session:
      enabled: ${AIV_BROWSER_SESSION_ENABLED:true}
      cookie-name: AIV_BROWSER_SESSION
      max-age-seconds: ${AIV_BROWSER_SESSION_MAX_AGE:1800}
      secure: ${AIV_BROWSER_SESSION_SECURE:false}
      same-site: Lax
  logs:
    jsonlogs: true
    showdept: true
    showtraceid: true
  task:
    kafka:
      retention.ms: 604800000
      retention.bytes: 10737418240
      min.insync.replicas: 1
      topic:
        topicName: task-topic
        partitions: 8
        replication-factor: 1
      producer:
        acks: all
        retries: 2147483647
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
        max.in.flight.requests.per.connection: 5
      consumer:
        concurrency: 2
        retry.interval.ms: 2000
        retry.max.attempts: 3
    manager:
      mode: ${APP_TASK_MANAGER_MODE:multi}

embed:
  ekey: ActiveInteigence
  tokenKey: H0WWWrNDCCoVKVPXMSei9/+rDJcLbgkEOXhayw790lY=
  iscustomtoken: false

logging:
  level:
    root: ${LOG_LEVEL_ROOT:ERROR}
    com.aiv: ${LOG_LEVEL_COM_AIV:INFO}
    liquibase: OFF

aiv-internalToken: ActiveIntelligence
general:
  location: ${aiv_base}/repository/econfig

universalauth:
  base-url: ${AIV_SUPERSECURITY}
  client-id: ${AIV_SSO_CLIENT_ID:aiv}
  public-base-url: ${AIV_SSO_PUBLIC_BASE_URL}
  callback-path: /sso/callback
  sso-enabled: ${AIV_SSO_REDIRECT_ENABLED:true}
  security:
    filter-enabled: false
EOF

  # 3. Create aiv1 systemd service
  cat << EOF > /etc/systemd/system/aiv1.service
[Unit]
Description=AIV Application Node 1
After=network.target aiv_universalauth.service
RequiresMountsFor=/var/lib/aiv/repository /var/lib/aiv/logs

[Service]
Type=simple
User=aiv
Group=aiv
Environment="JAVA_HOME=${JAVA_HOME_DIR}"
Environment="PATH=${JAVA_HOME_DIR}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
Environment="AIV_NODE_INSTANCE=1"
Environment="aiv_port=${AIV1_PORT}"
Environment="NODE_ID=$(hostname -f):${AIV1_PORT}"
Environment="APP_LEADER_ELECTION_ENABLED=${leader_election}"
Environment="LIQUIBASE_AIV_ENABLED=${node1_liquibase}"
Environment="JAVA_TOOL_OPTIONS=-Xms512m -Xmx8g -Dapp.leader.election.enabled=${leader_election}"
ExecStart=/usr/bin/aiv
WorkingDirectory=/var/lib/aiv
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

  # 4. Conditionally create aiv2 service (Active in multi-node mode or on Server 2)
  if [[ "$ROLE" == "server2" || "$INSTALL_KAFKA" == "yes" ]]; then
    cat << EOF > /etc/systemd/system/aiv2.service
[Unit]
Description=AIV Application Node 2
After=network.target aiv_universalauth.service aiv1.service
RequiresMountsFor=/var/lib/aiv/repository /var/lib/aiv/logs

[Service]
Type=simple
User=aiv
Group=aiv
Environment="JAVA_HOME=${JAVA_HOME_DIR}"
Environment="PATH=${JAVA_HOME_DIR}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
Environment="AIV_NODE_INSTANCE=2"
Environment="aiv_port=${AIV2_PORT}"
Environment="NODE_ID=$(hostname -f):${AIV2_PORT}"
Environment="APP_LEADER_ELECTION_ENABLED=${leader_election}"
Environment="LIQUIBASE_AIV_ENABLED=${node2_liquibase}"
Environment="JAVA_TOOL_OPTIONS=-Xms512m -Xmx8g -Dapp.leader.election.enabled=${leader_election}"
ExecStart=/usr/bin/aiv
WorkingDirectory=/var/lib/aiv
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
  else
    systemctl stop aiv2.service 2>/dev/null || true
    systemctl disable aiv2.service 2>/dev/null || true
    rm -f /etc/systemd/system/aiv2.service
  fi

  # 5. On Server 2, disable UniversalAuth and open local node ports for Server 1
  if [[ "$ROLE" == "server2" ]]; then
    systemctl stop aiv_universalauth.service 2>/dev/null || true
    systemctl disable aiv_universalauth.service 2>/dev/null || true

    if systemctl is-active --quiet firewalld; then
      firewall-cmd --permanent --add-rich-rule="rule family=\"ipv4\" source address=\"${SERVER1_IP}\" port port=\"${AIV1_PORT}\" protocol=\"tcp\" accept" 2>/dev/null || true
      firewall-cmd --permanent --add-rich-rule="rule family=\"ipv4\" source address=\"${SERVER1_IP}\" port port=\"${AIV2_PORT}\" protocol=\"tcp\" accept" 2>/dev/null || true
      firewall-cmd --reload 2>/dev/null || true
      ok "Firewalld configured to allow Server 1 (${SERVER1_IP}) access to ports ${AIV1_PORT} and ${AIV2_PORT}."
    fi
  fi

  # 5. Configure Nginx Load Balancer (Only if Nginx requested and Kafka enabled)
  if [[ "$INSTALL_NGINX" == "yes" && "$INSTALL_KAFKA" == "yes" ]]; then
    local upstream_servers="    server 127.0.0.1:${AIV1_PORT} max_fails=3 fail_timeout=10s;\n    server 127.0.0.1:${AIV2_PORT} max_fails=3 fail_timeout=10s;"
    local all_worker_ips="${ALLOW_IPS:-}"
    local existing_allowed
    existing_allowed=$(detect_existing_whitelisted_ips)
    [[ -n "$existing_allowed" ]] && all_worker_ips="${all_worker_ips},${existing_allowed}"

    if [[ -n "$all_worker_ips" ]]; then
      IFS=',' read -ra W_ARR <<< "$all_worker_ips"
      for wip in "${W_ARR[@]}"; do
        wip=$(echo "$wip" | xargs)
        [[ -z "$wip" || "$wip" == "127.0.0.1" || "$wip" == "$srv_ip" ]] && continue
        if timeout 2 bash -c "</dev/tcp/${wip}/${AIV1_PORT}" 2>/dev/null; then
          upstream_servers="${upstream_servers}\n    server ${wip}:${AIV1_PORT} max_fails=3 fail_timeout=10s;\n    server ${wip}:${AIV2_PORT} max_fails=3 fail_timeout=10s;"
        else
          info "Worker node ${wip} is not yet reachable on port ${AIV1_PORT}. Adding as commented upstream."
          upstream_servers="${upstream_servers}\n    # server ${wip}:${AIV1_PORT} max_fails=3 fail_timeout=10s; # Uncomment after Server 2 installation\n    # server ${wip}:${AIV2_PORT} max_fails=3 fail_timeout=10s; # Uncomment after Server 2 installation"
        fi
      done
    fi

    cat << EOF > /etc/nginx/conf.d/aiv-lb.conf
log_format upstream_log '\$remote_addr -> \$upstream_addr [\$time_local] "\$request" \$status';

upstream aiv_cluster {
    least_conn;
$(echo -e "${upstream_servers}")
}

server {
    listen ${AIV_LB_PORT};
    port_in_redirect on;
    access_log /var/log/nginx/aiv_upstream.log upstream_log;

    # REQUIRED: Prevents Nginx from dropping headers with underscores (logs_category, etc.)
    underscores_in_headers on;

    location = /aiv {
        return 301 http://\$http_host/aiv/;
    }

    location / {
        proxy_pass http://aiv_cluster;
        proxy_http_version 1.1;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header X-Forwarded-Host \$http_host;
        proxy_set_header X-Forwarded-Port ${AIV_LB_PORT};
        proxy_redirect ~^http://([^:]+)/(.*)\$ http://\$1:${AIV_LB_PORT}/\$2;
        client_max_body_size 500M;

        # Automatic failover if any backend node is stopped or fails
        proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
        proxy_next_upstream_tries 3;
        proxy_next_upstream_timeout 60s;

        # Upstream buffer tuning (prevents 502 Bad Gateway on large SSO tokens & cookies)
        proxy_buffer_size 128k;
        proxy_buffers 4 256k;
        proxy_busy_buffers_size 256k;

        # Fast connection timeout for dead node detection (failover within 3s)
        proxy_connect_timeout 3s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }
}
EOF
    semanage port -a -t http_port_t -p tcp "${AIV_LB_PORT}" 2>/dev/null || semanage port -m -t http_port_t -p tcp "${AIV_LB_PORT}" 2>/dev/null || true
    setsebool -P httpd_can_network_connect 1 2>/dev/null || true
    systemctl daemon-reload
    systemctl enable --now nginx
    systemctl restart nginx
  fi

  # [FUTURE F5 INTEGRATION - UNCOMMENT WHEN READY]
  # elif [[ "$LB_TYPE" == "f5" ]]; then
  #   if systemctl is-active --quiet nginx 2>/dev/null; then
  #     info "Stopping and disabling local Nginx service (F5 load balancer selected)..."
  #     systemctl disable --now nginx 2>/dev/null || true
  #   fi
  #
  #   # Create F5 configuration directory
  #   mkdir -p /var/lib/aiv/f5
  #   chown -R aiv:aiv /var/lib/aiv/f5 2>/dev/null || true
  #
  #   # Collect all cluster node IPs for F5 pool members
  #   local all_nodes=()
  #   all_nodes+=("${srv_ip}")
  #   local all_worker_ips="${ALLOW_IPS:-}"
  #   local existing_allowed
  #   existing_allowed=$(detect_existing_whitelisted_ips)
  #   [[ -n "$existing_allowed" ]] && all_worker_ips="${all_worker_ips},${existing_allowed}"
  #
  #   if [[ -n "$all_worker_ips" ]]; then
  #     IFS=',' read -ra W_ARR <<< "$all_worker_ips"
  #     for wip in "${W_ARR[@]}"; do
  #       wip=$(echo "$wip" | xargs)
  #       [[ -z "$wip" || "$wip" == "127.0.0.1" || "$wip" == "$srv_ip" ]] && continue
  #       all_nodes+=("${wip}")
  #     done
  #   fi
  #
  #   local f5_vip_target="${F5_VIP:-$srv_ip}"
  #
  #   # 1. Generate F5 TMSH CLI Setup Script
  #   local tmsh_nodes=""
  #   local tmsh_members=""
  #   local idx=1
  #   for nip in "${all_nodes[@]}"; do
  #     tmsh_nodes="${tmsh_nodes}create ltm node node_aiv_s${idx} address ${nip} description \"AIV Cluster Node Host ${idx}\"\n"
  #     tmsh_members="${tmsh_members}    node_aiv_s${idx}:${AIV1_PORT} { priority-group 1 }\n"
  #     tmsh_members="${tmsh_members}    node_aiv_s${idx}:${AIV2_PORT} { priority-group 1 }\n"
  #     idx=$((idx + 1))
  #   done
  #
  #   cat << 'EOF' > /var/lib/aiv/f5/f5_tmsh_setup.sh
  # ...
  #   chmod +x /var/lib/aiv/f5/f5_tmsh_setup.sh
  #   ok "F5 BIG-IP configuration files generated in /var/lib/aiv/f5/."

  # 6. Configure Centralized Shared Logs in S3 Storage
  if [[ "$INSTALL_S3" == "yes" && -d "${S3_MOUNT_POINT}" ]]; then
    local server_tag
    server_tag=$(hostname -f 2>/dev/null || hostname -s 2>/dev/null || uname -n)

    info "Setting up partitioned shared logs in ${S3_MOUNT_POINT}/logs/${server_tag}..."
    mkdir -p "${S3_MOUNT_POINT}/logs/${server_tag}"
    chown -R aiv:aiv "${S3_MOUNT_POINT}/logs" 2>/dev/null || true

    # Migrate and symlink /var/log/aiv (primary active log destination)
    if [[ -d /var/log/aiv && ! -L /var/log/aiv ]]; then
      cp -r /var/log/aiv/* "${S3_MOUNT_POINT}/logs/${server_tag}/" 2>/dev/null || true
      rm -rf /var/log/aiv
    fi
    ln -sfn "${S3_MOUNT_POINT}/logs/${server_tag}" /var/log/aiv
    chown -h aiv:aiv /var/log/aiv

    # Also ensure /var/lib/aiv/logs symlinks to the same shared directory
    if [[ -d /var/lib/aiv/logs && ! -L /var/lib/aiv/logs ]]; then
      cp -r /var/lib/aiv/logs/* "${S3_MOUNT_POINT}/logs/${server_tag}/" 2>/dev/null || true
      rm -rf /var/lib/aiv/logs
    fi
    ln -sfn "${S3_MOUNT_POINT}/logs/${server_tag}" /var/lib/aiv/logs
    chown -h aiv:aiv /var/lib/aiv/logs
    ok "Shared logs configured: /var/log/aiv & /var/lib/aiv/logs -> ${S3_MOUNT_POINT}/logs/${server_tag}"
  fi

  mkdir -p /var/lib/aiv/logs/universalauth
  chown -R aiv:aiv /var/lib/aiv/logs 2>/dev/null || true

  systemctl daemon-reload
  ok "Applications and environment configured."
  record "Cluster Config" "OK" "Node configurations updated"
}

wait_for_service() {
  local svc="$1" timeout="${2:-90}" port="${3:-}"
  info "Waiting for service '$svc' (timeout: ${timeout}s)..."
  local elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    if systemctl is-active --quiet "$svc"; then
      if [[ -n "$port" ]]; then
        if ss -tlpn | grep -q -E "(:|\])${port}\b"; then
          ok "Service '$svc' is active and listening on port $port!"
          return 0
        fi
      else
        ok "Service '$svc' is active!"
        return 0
      fi
    fi
    sleep 3
    elapsed=$((elapsed + 3))
  done

  warn "WARNING: Service '$svc' did not bind within ${timeout}s (Process may still be initializing)."
  journalctl -u "$svc" -n 25 --no-pager >&2 || true
  return 1
}

step_start_stack() {
  step "Starting Services in Sequence (Role: ${ROLE})"

  if [[ "$ROLE" == "server1" ]]; then
    info "1. Starting UniversalAuth (port ${AUTH_PORT})..."
    systemctl restart aiv_universalauth.service
    wait_for_service "aiv_universalauth.service" 120 "${AUTH_PORT}" || true
  else
    info "1. UniversalAuth skipped on Server 2 (Auth service hosted on Server 1: ${SERVER1_IP:-remote}:${AUTH_PORT})."
    systemctl stop aiv_universalauth.service 2>/dev/null || true
    systemctl disable aiv_universalauth.service 2>/dev/null || true
    systemctl mask aiv_universalauth.service 2>/dev/null || true
  fi

  info "2. Starting AIV Node 1 (port ${AIV1_PORT})..."
  systemctl restart aiv1.service
  wait_for_service "aiv1.service" 500 "${AIV1_PORT}" || true

  if [[ "$ROLE" == "server2" || "$INSTALL_KAFKA" == "yes" ]]; then
    info "3. Starting AIV Node 2 (port ${AIV2_PORT})..."
    systemctl restart aiv2.service
    wait_for_service "aiv2.service" 300 "${AIV2_PORT}" || true
    ok "All planned AIV node services started!"
    record "Service Stack" "OK" "2 Local Nodes Started"
  else
    ok "UniversalAuth and AIV Node 1 started (Single Node mode)."
    record "Service Stack" "OK" "1 Node Started (Single-node)"
  fi
}

# ==============================================================================
# SCHEDULED BACKUPS (optional): PostgreSQL dump + repository snapshot, point-in-time restore
# ==============================================================================
ask_backup_preferences() {
  local ans d k
  echo
  ans=$(prompt_yes_no "Do you want scheduled backups of the database and repository?" "$INSTALL_BACKUP")
  if [[ "$ans" != "yes" ]]; then
    INSTALL_BACKUP="no"
    return 0
  fi
  INSTALL_BACKUP="yes"
  while true; do
    read -r -p "Take a backup every how many days? [Default: ${BACKUP_INTERVAL_DAYS}]: " d
    d="${d:-$BACKUP_INTERVAL_DAYS}"
    [[ "$d" =~ ^[1-9][0-9]*$ ]] && break
    echo "Please enter a whole number of days (1 or more)." >&2
  done
  BACKUP_INTERVAL_DAYS="$d"
  [[ "$BACKUP_KEEP_DAYS" -lt "$BACKUP_INTERVAL_DAYS" ]] && BACKUP_KEEP_DAYS="$BACKUP_INTERVAL_DAYS"
  while true; do
    read -r -p "How many days of backups to keep? [Default: ${BACKUP_KEEP_DAYS}]: " k
    k="${k:-$BACKUP_KEEP_DAYS}"
    if [[ "$k" =~ ^[1-9][0-9]*$ ]] && [[ "$k" -ge "$BACKUP_INTERVAL_DAYS" ]]; then break; fi
    echo "Please enter a whole number of days, at least ${BACKUP_INTERVAL_DAYS}." >&2
  done
  BACKUP_KEEP_DAYS="$k"
}

normalize_backup_settings() {
  if [[ "$INSTALL_BACKUP" == "yes" && ( "$ROLE" != "server1" || "$INSTALL_PG" != "yes" ) ]]; then
    warn "Scheduled backups are only installed on the primary server that hosts PostgreSQL - skipping."
    INSTALL_BACKUP="no"
  fi
  [[ "$BACKUP_INTERVAL_DAYS" =~ ^[1-9][0-9]*$ ]] || die "--backup-days must be a whole number (1 or more)."
  [[ "$BACKUP_KEEP_DAYS" =~ ^[1-9][0-9]*$ ]]     || die "--backup-keep must be a whole number (1 or more)."
  [[ "$BACKUP_KEEP_DAYS" -lt "$BACKUP_INTERVAL_DAYS" ]] && BACKUP_KEEP_DAYS="$BACKUP_INTERVAL_DAYS"
  return 0
}

# Install packages with whichever package manager this Linux has.
backup_pkg_install() {
  if   command -v dnf     >/dev/null 2>&1; then dnf install -y "$@"
  elif command -v yum     >/dev/null 2>&1; then yum install -y "$@"
  elif command -v apt-get >/dev/null 2>&1; then DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
  elif command -v zypper  >/dev/null 2>&1; then zypper --non-interactive install "$@"
  elif command -v pacman  >/dev/null 2>&1; then pacman -S --noconfirm --needed "$@"
  elif command -v apk     >/dev/null 2>&1; then apk add "$@"
  else return 1; fi
}

# restic: distro package first, then the official static binary from GitHub.
backup_install_restic() {
  command -v restic >/dev/null 2>&1 && { ok "Package present: restic"; return 0; }
  info "Installing restic ..."
  backup_pkg_install restic >/dev/null 2>&1 || true
  command -v restic >/dev/null 2>&1 && { ok "restic installed from the distribution repositories."; return 0; }

  local arch ver url tmp
  case "$(uname -m)" in
    x86_64|amd64)  arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    armv7l|armv6l) arch="arm" ;;
    i386|i686)     arch="386" ;;
    *) warn "No restic build for $(uname -m)."; return 1 ;;
  esac
  command -v bzip2 >/dev/null 2>&1 || backup_pkg_install bzip2 >/dev/null 2>&1 || true
  command -v bzip2 >/dev/null 2>&1 || { warn "bzip2 is needed to unpack restic and could not be installed."; return 1; }
  ver=$(curl -fsSL --connect-timeout 10 https://api.github.com/repos/restic/restic/releases/latest 2>/dev/null \
        | grep -o '"tag_name": *"v[^"]*"' | head -n 1 | sed -E 's/.*"v([^"]+)".*/\1/' || true)
  ver="${ver:-0.17.3}"
  url="https://github.com/restic/restic/releases/download/v${ver}/restic_${ver}_linux_${arch}.bz2"
  tmp=$(mktemp)
  info "Downloading restic ${ver} (${arch}) from GitHub ..."
  if curl -fsSL --connect-timeout 15 -o "$tmp" "$url" && bzip2 -dc "$tmp" > "${tmp}.bin" && install -m 0755 "${tmp}.bin" /usr/local/bin/restic; then
    rm -f "$tmp" "${tmp}.bin"
    ok "restic ${ver} installed to /usr/local/bin/restic"
    return 0
  fi
  rm -f "$tmp" "${tmp}.bin"
  return 1
}

step_backup() {
  step "Scheduled Backups"
  if [[ "$INSTALL_BACKUP" != "yes" ]]; then
    info "Scheduled backups not requested - skipping."
    record "Backup" "SKIPPED" "Not requested"
    return 0
  fi

  if ! id aiv >/dev/null 2>&1; then
    warn "User 'aiv' does not exist (AIV not installed?). Backups NOT installed."
    record "Backup" "WARN" "aiv user missing"
    return 0
  fi

  if ! backup_install_restic; then
    warn "Could not install restic. Backups NOT installed (install restic, then re-run with --backup)."
    record "Backup" "WARN" "restic not installed"
    return 0
  fi
  if ! command -v flock >/dev/null 2>&1; then
    backup_pkg_install util-linux >/dev/null 2>&1 || true
    command -v flock >/dev/null 2>&1 || { warn "flock (util-linux) missing. Backups NOT installed."; record "Backup" "WARN" "flock missing"; return 0; }
  fi
  if ! command -v pg_dump >/dev/null 2>&1; then
    warn "pg_dump not found. Backups NOT installed (install the PostgreSQL client tools)."
    record "Backup" "WARN" "pg_dump missing"
    return 0
  fi

  local interval_secs max_backups agrp
  interval_secs=$(( BACKUP_INTERVAL_DAYS * 86400 ))
  max_backups=$(( (BACKUP_KEEP_DAYS + BACKUP_INTERVAL_DAYS - 1) / BACKUP_INTERVAL_DAYS ))
  agrp=$(id -gn aiv)
  local envf=/etc/aiv-backup/aiv-backup.env

  install -d -o aiv -g "$agrp" -m 0750 /var/backups/aiv
  install -d -o root -g "$agrp" -m 0750 /etc/aiv-backup

  cat > /usr/local/bin/aiv-backup <<'AIV_BACKUP_TOOL_EOF'
#!/bin/bash
# aiv-backup - scheduled PostgreSQL + repository backups and point-in-time restore for AIV.
# One self-contained file. Installed by aiv-base-setup.sh as /usr/local/bin/aiv-backup.
# Config: /etc/aiv-backup/aiv-backup.env  (DB + repository location are auto-read from /var/lib/aiv/enviroment)
set -uo pipefail

CONF="${AIV_BACKUP_CONF:-/etc/aiv-backup/aiv-backup.env}"
RUN_AS=aiv
SELF="$(readlink -f "$0")"

usage() {
cat <<'EOF'
Usage: aiv-backup <command>

  status                        Service state and newest restore points
  backup                        Take a backup now
  list                          List restore points
  restore [-y] <time|latest>    Restore repository files AND database to a restore point
  restore-db [-y] <time|latest> Restore the database only
  logs                          Follow the backup log

  <time> looks like 2026-05-20_110000 (see 'aiv-backup list'); the newest restore point at or before it is used.
  restore stops AIV (aiv1, aiv2, UniversalAuth) on this server while it runs, then starts it again.
  Config: /etc/aiv-backup/aiv-backup.env   (apply changes: systemctl restart aiv-backup)
EOF
}

log()  { printf '[%s] %s\n' "$(date '+%F %T')" "$*"; }
fail() { printf '[%s] ERROR: %s\n' "$(date '+%F %T')" "$*" >&2; return 1; }

# Run a command as another user on any distro (runuser -> setpriv -> su).
run_as_user() {   # $1 = user, rest = command
    local u="$1"; shift
    if command -v runuser >/dev/null 2>&1; then runuser -u "$u" -- "$@"
    elif command -v setpriv >/dev/null 2>&1; then setpriv --reuid="$u" --regid="$u" --init-groups -- "$@"
    else su -s /bin/bash "$u" -c 'exec "$@"' _ "$@"; fi
}

as_aiv() {
    if [ "$(id -u)" -eq 0 ] && id "$RUN_AS" >/dev/null 2>&1; then run_as_user "$RUN_AS" "$@"; else "$@"; fi
}

# ---------------------------------------------------------------- configuration
load_config() {
    if [ -r "$CONF" ]; then set -a; . "$CONF"; set +a; fi
    local f="${AIV_ENV_FILE:-/var/lib/aiv/enviroment}"
    if [ -r "$f" ]; then
        local vals url user pass base
        vals="$(bash -c 'set +eu; . "$1" >/dev/null 2>&1
            printf "%s\n%s\n%s\n%s\n" "${DATABASE_URL:-}" "${DATABASE_USERNAME:-}" "${DATABASE_PASSWORD:-}" "${aiv_base:-}"' _ "$f")"
        { read -r url; read -r user; read -r pass; read -r base; } <<<"$vals" || true
        if [[ "${url:-}" =~ ^jdbc:postgresql://([^:/]+)(:([0-9]+))?/([^?]+) ]]; then
            : "${PGHOST:=${BASH_REMATCH[1]}}"; : "${PGPORT:=${BASH_REMATCH[3]:-5432}}"; : "${PGDATABASE:=${BASH_REMATCH[4]}}"
        fi
        [ -n "${user:-}" ] && : "${PGUSER:=$user}"
        [ -n "${pass:-}" ] && : "${PGPASSWORD:=$pass}"
        [ -n "${base:-}" ] && : "${REPO_PATH:=$base/repository}"
    fi
    : "${BACKUP_ENABLED:=true}" "${BACKUP_ROOT:=/var/backups/aiv}" "${BACKUP_INTERVAL:=3600}"
    : "${STARTUP_DELAY:=1200}" "${MAX_BACKUPS:=24}" "${PGHOST:=127.0.0.1}" "${PGPORT:=5432}" "${PGUSER:=postgres}"
    : "${PGDATABASE:=aiv}" "${REPO_PATH:=/var/lib/aiv/repository}" "${RESTIC_PASSWORD:=}" "${PGPASSWORD:=}"
    export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD RESTIC_PASSWORD
    export RESTIC_REPOSITORY="$BACKUP_ROOT/restic_repo"
}

check_prereqs() {
    local ok=0
    command -v pg_dump >/dev/null 2>&1 || { fail "pg_dump not found (install the PostgreSQL client)."; ok=1; }
    command -v restic  >/dev/null 2>&1 || { fail "restic not found (install it with your package manager, or from https://restic.net)."; ok=1; }
    command -v flock   >/dev/null 2>&1 || { fail "flock not found (util-linux)."; ok=1; }
    [ -n "$RESTIC_PASSWORD" ] || { fail "Set RESTIC_PASSWORD in $CONF (and keep a copy of it)."; ok=1; }
    [ -n "$PGPASSWORD" ]      || { fail "No database password found (set PGPASSWORD in $CONF)."; ok=1; }
    [ -d "$REPO_PATH" ]       || { fail "Repository folder does not exist: $REPO_PATH"; ok=1; }
    return $ok
}

list_points() {   # newest first
    ls -1 "$BACKUP_ROOT" 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{6}$' | sort -r
}

# ---------------------------------------------------------------- backup
ensure_restic_repo() {
    # Decide by the config file itself: a failing 'restic cat' (stale lock, permissions) must never trigger a re-init.
    [ -f "$RESTIC_REPOSITORY/config" ] && return 0
    log "Initialising restic repository at $RESTIC_REPOSITORY"
    mkdir -p "$RESTIC_REPOSITORY" && restic init >/dev/null
}

prune_old() {
    local old ids="" p sid
    old="$(list_points | tail -n +$((MAX_BACKUPS + 1)))"
    [ -n "$old" ] || return 0
    for p in $old; do
        sid="$(cat "$BACKUP_ROOT/$p/restic_snapshot" 2>/dev/null || true)"
        [ -n "$sid" ] && ids="$ids $sid"
    done
    # shellcheck disable=SC2086
    [ -n "$ids" ] && { restic forget --prune --quiet $ids || log "WARNING: restic forget/prune failed (will retry next run)"; }
    for p in $old; do log "Removing old restore point $p"; rm -rf "${BACKUP_ROOT:?}/$p"; done
}

do_backup() {
    load_config; check_prereqs || return 1
    mkdir -p "$BACKUP_ROOT" || return 1
    exec 9>"$BACKUP_ROOT/.lock"
    flock -n 9 || { log "Another backup is already running - skipping."; return 0; }

    local ts tmp final sid rc
    ts="$(date +%Y-%m-%d_%H%M%S)"; tmp="$BACKUP_ROOT/.partial_$ts"; final="$BACKUP_ROOT/$ts"
    mkdir -p "$tmp"
    log "=== Backup $ts started ==="

    log "pg_dump of database '$PGDATABASE' ..."
    if ! pg_dump -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -Fc -f "$tmp/db.dump"; then
        rm -rf "$tmp"; fail "pg_dump failed."; return 1
    fi

    ensure_restic_repo || { rm -rf "$tmp"; fail "Cannot prepare restic repository."; return 1; }
    log "restic snapshot of $REPO_PATH ..."
    restic backup "$REPO_PATH" --tag "ts-$ts" --quiet; rc=$?
    if [ $rc -ne 0 ] && [ $rc -ne 3 ]; then rm -rf "$tmp"; fail "restic backup failed (exit $rc)."; return 1; fi
    [ $rc -eq 3 ] && log "WARNING: some repository files could not be read (snapshot still created)."

    sid="$(restic snapshots --tag "ts-$ts" --json 2>/dev/null | grep -o '"short_id":"[^"]*"' | head -1 | cut -d'"' -f4)"
    [ -n "$sid" ] || { rm -rf "$tmp"; fail "Could not find the new restic snapshot."; return 1; }
    echo "$sid" > "$tmp/restic_snapshot"
    mv "$tmp" "$final"
    log "=== Backup complete: $final (snapshot $sid) ==="
    prune_old
}

# ---------------------------------------------------------------- scheduler (used by systemd)
do_scheduler() {
    load_config
    if [ "$BACKUP_ENABLED" != "true" ]; then
        log "BACKUP_ENABLED is not true in $CONF - backup not started."; exit 0
    fi
    check_prereqs || exit 1
    mkdir -p "$BACKUP_ROOT" || exit 1
    trap 'log "Stopping."; exit 0' TERM INT
    if [ "$STARTUP_DELAY" -gt 0 ] 2>/dev/null; then
        log "First backup in ${STARTUP_DELAY}s, then every ${BACKUP_INTERVAL}s (keeping ${MAX_BACKUPS} restore points) -> $BACKUP_ROOT"
        sleep "$STARTUP_DELAY" & wait $!
    fi
    while true; do
        ( do_backup ) || log "Backup run failed - will retry at the next interval."
        sleep "$BACKUP_INTERVAL" & wait $!
    done
}

# ---------------------------------------------------------------- restore
resolve_point() {   # $1 = time or latest -> prints restore point name
    local when="$1"
    [ "$when" = "latest" ] && when="9999-12-31_235959"
    list_points | awk -v w="$when" '$0 <= w { print; exit }'
}

restore_inner() {   # runs as the aiv user: $1 = all|db, $2 = restore point
    load_config
    local what="$1" p="$2" dir="$BACKUP_ROOT/$2" sid rc=0
    [ -f "$dir/db.dump" ] || { fail "No db.dump in $dir"; return 1; }
    if [ "$what" = "all" ]; then
        sid="$(cat "$dir/restic_snapshot" 2>/dev/null || true)"
        [ -n "$sid" ] || { fail "No restic snapshot recorded for $p"; return 1; }
        log "Restoring repository files from snapshot $sid ..."
        restic restore "$sid" --target / || { fail "restic restore failed."; return 1; }
    fi
    log "Restoring database '$PGDATABASE' from $dir/db.dump ..."
    pg_restore -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" --clean --if-exists --no-owner --no-privileges "$dir/db.dump" || rc=$?
    [ $rc -ne 0 ] && log "WARNING: pg_restore finished with messages (exit $rc). Often harmless with --clean; verify AIV starts and data is correct."
    return 0
}

with_aiv_stopped() {
    local stop=(aiv-backup.service aiv2.service aiv1.service aiv_universalauth.service)
    local start=(aiv_universalauth.service aiv1.service aiv2.service aiv-backup.service)
    local was=() s a rc=0
    if [ -z "${AIV_BACKUP_NO_SERVICES:-}" ]; then
        for s in "${stop[@]}"; do systemctl is-active --quiet "$s" && was+=("$s"); done
        for s in "${stop[@]}"; do systemctl stop "$s" 2>/dev/null || true; done
    fi
    "$@" || rc=$?
    if [ -z "${AIV_BACKUP_NO_SERVICES:-}" ]; then
        for s in "${start[@]}"; do
            for a in "${was[@]:-}"; do [ "$a" = "$s" ] && { systemctl start "$s" || echo "WARNING: could not start $s"; }; done
        done
    fi
    return $rc
}

do_restore() {   # $1 = all|db ; rest = [-y] <time|latest>
    local what="$1"; shift
    local yes=0
    [ "${1:-}" = "-y" ] && { yes=1; shift; }
    local when="${1:-}"
    [ -n "$when" ] || { usage; return 1; }
    load_config
    local p; p="$(resolve_point "$when")"
    [ -n "$p" ] || { fail "No restore point at or before '$when'. Run: aiv-backup list"; return 1; }
    echo "Restore point selected: $p  ($([ "$what" = all ] && echo 'repository files + database' || echo 'database only'))"
    if [ $yes -ne 1 ]; then
        echo "This OVERWRITES current AIV data. AIV (aiv1, aiv2, UniversalAuth) is stopped on THIS server meanwhile."
        echo "Cluster: first stop AIV on every worker server (systemctl stop aiv1 aiv2), start them again afterwards."
        read -rp "Type 'yes' to continue: " a
        [ "$a" = "yes" ] || { echo "Cancelled."; return 1; }
    fi
    with_aiv_stopped as_aiv "$SELF" _restore_inner "$what" "$p"
}

# ---------------------------------------------------------------- dispatch
cmd="${1:-}"; [ $# -gt 0 ] && shift

case "$cmd" in   # run as the aiv user so backup files are never root-owned
    backup|list|_scheduler)
        if [ "$(id -u)" -eq 0 ] && id "$RUN_AS" >/dev/null 2>&1; then run_as_user "$RUN_AS" "$SELF" "$cmd" "$@"; exit $?; fi ;;
    restore|restore-db|status|logs)
        if [ "$(id -u)" -ne 0 ] && [ -z "${AIV_BACKUP_NO_SERVICES:-}" ]; then exec sudo "$SELF" "$cmd" "$@"; fi ;;
esac

case "$cmd" in
    backup)        ( do_backup ) ;;
    _scheduler)    do_scheduler ;;
    _restore_inner) restore_inner "$@" ;;
    list)          load_config; echo "Restore points in $BACKUP_ROOT (newest first):"; list_points | sed 's/^/  /' ;;
    restore)       do_restore all "$@" ;;
    restore-db)    do_restore db "$@" ;;
    status)        load_config; systemctl --no-pager status aiv-backup.service 2>/dev/null || true
                   echo; echo "Newest restore points:"; list_points | head -5 | sed 's/^/  /' ;;
    logs)          exec journalctl -u aiv-backup.service -f ;;
    *)             usage; [ -n "$cmd" ] && exit 1 || exit 0 ;;
esac
AIV_BACKUP_TOOL_EOF
  chmod 0755 /usr/local/bin/aiv-backup
  command -v restorecon >/dev/null 2>&1 && restorecon -R /usr/local/bin/aiv-backup /var/backups/aiv 2>/dev/null || true

  if [[ ! -f "$envf" ]]; then
    cat > "$envf" <<EOF
# aiv-backup settings. Apply changes with: systemctl restart aiv-backup
BACKUP_ENABLED=true
# seconds between backups (${BACKUP_INTERVAL_DAYS} day(s))
BACKUP_INTERVAL=${interval_secs}
# how many restore points to keep (about ${BACKUP_KEEP_DAYS} day(s))
MAX_BACKUPS=${max_backups}
# wait this long after start before the first backup
STARTUP_DELAY=1200
# where backups are stored (use a separate disk if possible)
BACKUP_ROOT=/var/backups/aiv
# encryption password for the repository snapshots - keep a copy, it is needed to restore
RESTIC_PASSWORD=
EOF
  else
    info "Keeping existing ${envf}; updating interval and retention only."
    grep -q '^BACKUP_ENABLED=' "$envf" && sed -i 's|^BACKUP_ENABLED=.*|BACKUP_ENABLED=true|' "$envf" || echo 'BACKUP_ENABLED=true' >> "$envf"
    grep -q '^BACKUP_INTERVAL=' "$envf" && sed -i "s|^BACKUP_INTERVAL=.*|BACKUP_INTERVAL=${interval_secs}|" "$envf" || echo "BACKUP_INTERVAL=${interval_secs}" >> "$envf"
    grep -q '^MAX_BACKUPS=' "$envf" && sed -i "s|^MAX_BACKUPS=.*|MAX_BACKUPS=${max_backups}|" "$envf" || echo "MAX_BACKUPS=${max_backups}" >> "$envf"
  fi
  chown root:"$agrp" "$envf"; chmod 0640 "$envf"

  # Generate the restic password once (never printed to the log).
  if grep -qE '^RESTIC_PASSWORD=$' "$envf"; then
    local rp
    rp=$(openssl rand -hex 24 2>/dev/null || head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
    sed -i "s|^RESTIC_PASSWORD=\$|RESTIC_PASSWORD=${rp}|" "$envf"
    warn "A backup encryption password was generated in ${envf} - COPY IT somewhere safe; it is required to restore."
  fi

  cat > /etc/systemd/system/aiv-backup.service <<'AIV_BACKUP_UNIT_EOF'
[Unit]
Description=AIV scheduled backup (PostgreSQL + repository)
After=network.target postgresql.service aiv_universalauth.service aiv1.service aiv2.service

[Service]
Type=simple
User=aiv
Group=aiv
ExecStart=/usr/local/bin/aiv-backup _scheduler
Restart=on-failure
RestartSec=30
TimeoutStopSec=30
KillMode=control-group
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target aiv1.service aiv2.service aiv_universalauth.service
AIV_BACKUP_UNIT_EOF
  sed -i "s|^Group=aiv\$|Group=${agrp}|" /etc/systemd/system/aiv-backup.service

  systemctl daemon-reload
  systemctl enable aiv-backup.service >/dev/null 2>&1 || true
  systemctl restart aiv-backup.service || warn "aiv-backup service failed to start - see: journalctl -u aiv-backup"
  ok "Backups enabled: every ${BACKUP_INTERVAL_DAYS} day(s), keeping ~${BACKUP_KEEP_DAYS} day(s) (${max_backups} restore points)."
  info "First backup ~20 min after start. Commands: aiv-backup status | list | restore latest"
  record "Backup" "OK" "every ${BACKUP_INTERVAL_DAYS}d, keep ${max_backups} points"
}

print_summary() {
  step "Summary"
  local line name status detail
  printf '%-22s %-10s %s\n' "ITEM" "STATUS" "DETAIL"
  for line in "${RESULTS[@]}"; do
    IFS='|' read -r name status detail <<<"$line"
    printf '%-22s %-10s %s\n' "$name" "$status" "$detail"
  done
  echo

  local end_time elapsed mins secs
  end_time=$(date +%s)
  elapsed=$(( end_time - ${SCRIPT_START_TIME:-end_time} ))
  mins=$(( elapsed / 60 ))
  secs=$(( elapsed % 60 ))

  local srv_ip
  srv_ip=$(get_server_ip)

  echo "Deployment Role      : ${ROLE}"
  echo "Current Server IP    : ${srv_ip}"
  [[ "$ROLE" == "server2" ]] && echo "Primary Server 1 IP  : ${SERVER1_IP}"
  if [[ "$ROLE" == "server1" && "$INSTALL_PG" == "yes" ]]; then
    local active_whitelisted
    active_whitelisted=$(detect_existing_whitelisted_ips)
    echo "Whitelisted Client IPs   : ${active_whitelisted:-None} (Ports: ${ALLOW_PORTS})"
  fi

  local access_url="http://${srv_ip}:${AIV1_PORT}/aiv"
  # [FUTURE F5 INTEGRATION - UNCOMMENT WHEN READY]
  # if [[ "$LB_TYPE" == "f5" ]]; then
  #   access_url="${F5_PROTO}://${F5_VIP:-$srv_ip}:${F5_PORT}/aiv"
  #   echo "Load Balancer        : F5 BIG-IP"
  #   echo "Cluster VIP Access   : ${access_url}"
  #   echo "F5 TMSH Setup Script : /var/lib/aiv/f5/f5_tmsh_setup.sh"
  #   echo "F5 AS3 JSON Template : /var/lib/aiv/f5/f5_as3_declaration.json"
  # el
  if [[ "$ROLE" == "server1" && "$INSTALL_NGINX" == "yes" && "$INSTALL_KAFKA" == "yes" ]]; then
    access_url="http://${srv_ip}:${AIV_LB_PORT}/aiv"
    echo "Cluster Load Balancer: ${access_url}"
  elif [[ "$ROLE" == "server2" ]]; then
    echo "Server 2 Node 1 URL  : http://${srv_ip}:${AIV1_PORT}/aiv"
    echo "Server 2 Node 2 URL  : http://${srv_ip}:${AIV2_PORT}/aiv"
    echo "Cluster Entrypoint   : http://${SERVER1_IP}:${AIV_LB_PORT}/aiv"
  else
    echo "AIV Direct URL       : ${access_url}"
  fi

  [[ "$ROLE" == "server1" ]] && echo "UniversalAuth URL    : http://${srv_ip}:${AUTH_PORT}" || echo "UniversalAuth URL    : http://${SERVER1_IP}:${AUTH_PORT}"
  echo "Java Runtime         : ${AIV_JAVA_DIR} (Project-level isolated)"
  [[ "$INSTALL_KAFKA" == "yes" ]] && echo "Kafka Broker         : Local (${srv_ip}:${KAFKA_PORT})" || echo "Kafka Broker         : Remote (${SERVER1_IP:-$DB_HOST}:${KAFKA_PORT})"
  [[ "$INSTALL_PG" == "yes" ]]    && echo "PostgreSQL           : Local (${PG_PORT})" || echo "PostgreSQL           : External (${DB_HOST}:${PG_PORT})"
  [[ "$INSTALL_S3" == "yes" ]]    && echo "Shared Storage       : ${S3_MOUNT_POINT}"   || echo "Shared Storage       : Local (/var/lib/aiv)"
  [[ "$INSTALL_BACKUP" == "yes" ]] && echo "Scheduled Backups    : every ${BACKUP_INTERVAL_DAYS} day(s), keep ${BACKUP_KEEP_DAYS} day(s) (aiv-backup status | list | restore latest)"
  echo "Total Execution Time : ${mins}m ${secs}s"
  echo "Setup Complete!"

  if [[ "$ROLE" == "server2" ]]; then
    echo
    echo "=============================================================================="
    echo " [CLUSTER ACTION REQUIRED ON SERVER 1]"
    echo " To distribute traffic across this worker server, run this on Server 1: ${SERVER1_IP}"
    echo
    printf '   sudo sed -i "/server 127.0.0.1:%s;/a \\    server %s:%s;\\n    server %s:%s;" /etc/nginx/conf.d/aiv-lb.conf\n' \
      "${AIV2_PORT}" "${srv_ip}" "${AIV1_PORT}" "${srv_ip}" "${AIV2_PORT}"
    echo "   sudo nginx -t && sudo systemctl reload nginx"
    echo
    echo " Or simply re-run: sudo /opt/aiv-base-setup.sh --role server1"
    echo "=============================================================================="
  fi
}

# ==============================================================================
# 3. MAIN RUNNER
# ==============================================================================
main() {
  SCRIPT_START_TIME=$(date +%s)
  parse_args "$@"
  [[ $EUID -eq 0 ]] || die "Please run as root: sudo $0 $*"
  setup_logging
  detect_os

  # Auto-detect Server 2 role if UniversalAuth is disabled locally
  if [[ "$ROLE" == "server1" && -f /etc/systemd/system/aiv1.service ]]; then
    if ! systemctl is-enabled --quiet aiv_universalauth.service 2>/dev/null; then
      ROLE="server2"
      IS_PRIMARY="no"
    fi
  fi

  if [[ "$UPGRADE_ONLY" == "yes" ]]; then
    step "AIV Application Upgrade Only"
    step_download_aiv
    step_install_aiv_rpm
    step_start_stack
    print_summary
    return 0
  fi

  ask_user_preferences
  if [[ "$UPGRADE_ONLY" == "yes" ]]; then
    step "AIV Application Upgrade Only"
    step_download_aiv
    step_install_aiv_rpm
    step_start_stack
    print_summary
    return 0
  fi

  step_prereqs
  step_download_aiv
  step_java
  step_postgres
  step_kafka
  step_s3_storage
  step_install_aiv_rpm
  step_configure_aiv_cluster
  step_start_stack
  step_backup
  print_summary
}

main "$@"