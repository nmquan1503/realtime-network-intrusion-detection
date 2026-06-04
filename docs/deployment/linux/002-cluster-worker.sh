#!/usr/bin/env bash
set -euo pipefail

echo "=============================="
echo "🚀 Kubernetes WORKER JOIN (SAFE)"
echo "=============================="

# --------------------------------------------------
# 0. SWAP OFF (REQUIRED)
# --------------------------------------------------
sudo swapoff -a
sudo sed -i '/ swap / s/^/#/' /etc/fstab || true

# --------------------------------------------------
# 1. KERNEL SETTINGS
# --------------------------------------------------
sudo modprobe overlay
sudo modprobe br_netfilter

cat <<EOF | sudo tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF

sudo sysctl --system

# --------------------------------------------------
# 2. CONTAINERD CHECK
# --------------------------------------------------
echo "==> Checking containerd..."
sudo systemctl enable --now containerd

if ! systemctl is-active --quiet containerd; then
  echo "❌ containerd not running!"
  exit 1
fi

# --------------------------------------------------
# 3. KUBELET CHECK
# --------------------------------------------------
echo "==> Enabling kubelet..."
sudo systemctl enable kubelet
sudo systemctl start kubelet

# --------------------------------------------------
# 4. JOIN COMMAND
# --------------------------------------------------
echo ""
echo "👉 Paste FULL kubeadm join command (single line):"
read -r JOIN_CMD

echo "==> Joining cluster..."
sudo $JOIN_CMD

# --------------------------------------------------
# 5. DONE
# --------------------------------------------------
echo "=============================="
echo "✅ Worker successfully joined"
echo "=============================="