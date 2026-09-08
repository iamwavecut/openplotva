#!/usr/bin/env bash
set -euo pipefail

if [[ $(id -u) != 0 || $(uname -s) != Linux ]]; then
  echo "Run the installer as root on the Linux worker host." >&2
  exit 1
fi
source_root=$(git rev-parse --show-toplevel)
if [[ $(git -C "$source_root" symbolic-ref --short HEAD) != main ]] ||
   [[ $(git -C "$source_root" rev-parse HEAD) != $(git -C "$source_root" rev-parse origin/main) ]] ||
   [[ -n $(git -C "$source_root" status --porcelain) ]]; then
  echo "Install only from a clean, verified checkout of origin/main." >&2
  exit 1
fi
for dependency in python3 gh docker iptables fallocate mkfs.ext4 mount umount findmnt; do
  command -v "$dependency" >/dev/null || { echo "Missing host dependency: $dependency" >&2; exit 1; }
done
python3 -c 'import sys; assert sys.version_info >= (3, 11), "Python 3.11+ is required"'
image_id=${1:?Pass the immutable image ID built from tools/maintenance/Dockerfile}
[[ $image_id =~ ^sha256:[a-f0-9]{64}$ ]] || { echo "Expected an immutable sha256 image ID." >&2; exit 1; }
[[ $(docker image inspect "$image_id" --format '{{index .Config.Labels "openplotva.maintenance.omp"}}') == 18.1.14 ]]
[[ $(docker image inspect "$image_id" --format '{{index .Config.Labels "openplotva.maintenance.rust"}}') == 1.95.0 ]]

install -d -m 0755 /opt/openplotva-maintenance
install -d -m 0700 /etc/openplotva-maintenance /var/lib/openplotva-maintenance
if ! id openplotva-omp >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/openplotva-omp --shell /usr/sbin/nologin openplotva-omp
fi
install -d -m 0700 -o openplotva-omp -g openplotva-omp /var/lib/openplotva-omp
if ! id openplotva-maintenance-dispatch >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/openplotva-maintenance-dispatch --shell /bin/sh openplotva-maintenance-dispatch
fi
install -d -m 0755 -o root -g root /var/lib/openplotva-maintenance-dispatch
install -d -m 0755 -o root -g root /var/lib/openplotva-maintenance-dispatch/.ssh

copy_if_changed() {
  if ! cmp -s "$1" "$2"; then install -m "$3" "$1" "$2"; fi
}
for file in "$source_root"/tools/maintenance/*.py; do
  copy_if_changed "$file" "/opt/openplotva-maintenance/$(basename "$file")" 0644
done
copy_if_changed "$source_root/deploy/maintenance/ingress" /usr/local/sbin/openplotva-maintenance-ingress 0755
for file in "$source_root"/deploy/maintenance/*.service; do
  copy_if_changed "$file" "/etc/systemd/system/$(basename "$file")" 0644
done
if [[ ! -e /etc/openplotva-maintenance/config.json ]]; then
  install -m 0600 "$source_root/deploy/maintenance/config.example.json" /etc/openplotva-maintenance/config.json
  python3 - "$image_id" <<'PY'
import json, pathlib, sys
path = pathlib.Path('/etc/openplotva-maintenance/config.json')
config = json.loads(path.read_text())
config['image'] = sys.argv[1]
path.write_text(json.dumps(config, indent=2) + '\n')
PY
fi
temporary_container=$(docker create --label openplotva.maintenance.install=true "$image_id")
trap 'docker rm -f "$temporary_container" >/dev/null 2>&1 || true' EXIT
docker cp "$temporary_container:/usr/local/bin/omp" /opt/openplotva-maintenance/omp
chmod 0755 /opt/openplotva-maintenance/omp
cat > /etc/sudoers.d/openplotva-maintenance <<'SUDOERS'
openplotva-maintenance-dispatch ALL=(root) NOPASSWD: /usr/local/sbin/openplotva-maintenance-ingress *
SUDOERS
chmod 0440 /etc/sudoers.d/openplotva-maintenance
visudo -cf /etc/sudoers.d/openplotva-maintenance
systemctl daemon-reload
echo "Installed. Services and new jobs remain disabled. Complete deploy/maintenance/README.md before activation."
