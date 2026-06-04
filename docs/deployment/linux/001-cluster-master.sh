#!/usr/bin/env bash
set -euo pipefail

echo "=============================="
echo "🧨 FULL WIPE + REBUILD K8S CLUSTER"
echo "=============================="

# --------------------------------------------------
# 0. KILL EXISTING K8S STATE (IMPORTANT FIX)
# --------------------------------------------------
echo "==> Reset kubeadm..."
sudo kubeadm reset -f || true

echo "==> Stopping kubelet..."
sudo systemctl stop kubelet || true

echo "==> Removing ALL Kubernetes remnants..."
sudo rm -rf /etc/kubernetes/
sudo rm -rf /var/lib/etcd
sudo rm -rf /var/lib/kubelet/*
sudo rm -rf ~/.kube

# --------------------------------------------------
# 1. CLEAN NETWORK + IPTABLES (CRITICAL FIX FOR PORT ISSUES)
# --------------------------------------------------
echo "==> Cleaning iptables..."
sudo iptables -F || true
sudo iptables -t nat -F || true
sudo iptables -t mangle -F || true
sudo iptables -X || true

# --------------------------------------------------
# 2. LOAD KERNEL MODULES
# --------------------------------------------------
echo "==> Loading kernel modules..."
sudo modprobe br_netfilter || true
sudo modprobe overlay || true

cat <<EOF | sudo tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF

sudo sysctl --system

# --------------------------------------------------
# 3. ENSURE SWAP OFF (K8S REQUIREMENT)
# --------------------------------------------------
echo "==> Disabling swap..."
sudo swapoff -a || true
sudo sed -i '/ swap / s/^/#/' /etc/fstab || true

# --------------------------------------------------
# 4. ENSURE CONTAINERD RUNNING
# --------------------------------------------------
echo "==> Starting containerd..."
sudo systemctl enable --now containerd

# --------------------------------------------------
# 5. INSTALL CNI PLUGINS
# --------------------------------------------------
echo "==> Installing CNI plugins..."
sudo mkdir -p /opt/cni/bin
curl -L https://github.com/containernetworking/plugins/releases/download/v1.5.1/cni-plugins-linux-amd64-v1.5.1.tgz \
| sudo tar -C /opt/cni/bin -xz

# --------------------------------------------------
# 6. INIT KUBERNETES CLUSTER
# --------------------------------------------------
MASTER_IP=$(hostname -I | awk '{print $1}')

echo "==> Initializing Kubernetes on $MASTER_IP..."

sudo kubeadm init \
  --apiserver-advertise-address="$MASTER_IP" \
  --pod-network-cidr=10.244.0.0/16 \
  --ignore-preflight-errors=all

# --------------------------------------------------
# 7. CONFIGURE KUBECTL
# --------------------------------------------------
echo "==> Setting kubeconfig..."
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

# --------------------------------------------------
# 8. INSTALL FLANNEL CNI
# --------------------------------------------------
echo "==> Installing Flannel..."
kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml

# allow scheduling on master
kubectl taint nodes --all node-role.kubernetes.io/control-plane- || true

# --------------------------------------------------
# 9. CREATE BIGDATA NAMESPACE + STRIMZI
# --------------------------------------------------
echo "==> Creating namespace bigdata..."
kubectl create namespace bigdata || true

echo "==> Installing Strimzi Kafka Operator..."
kubectl apply -f "https://strimzi.io/install/latest?namespace=bigdata" -n bigdata

# --------------------------------------------------
# 10. LOCAL STORAGE CLASS (FOR MINIO / PV)
# --------------------------------------------------
echo "==> Installing local-path storage..."
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.34/deploy/local-path-storage.yaml

# --------------------------------------------------
# 11. CONFIGMAP FROM .ENV (SAFE VERSION)
# --------------------------------------------------
if [ -f ".env" ]; then
  echo "==> Creating ConfigMap from .env..."
  kubectl create configmap config \
    --from-env-file=.env \
    -n bigdata \
    --dry-run=client -o yaml | kubectl apply -f -
else
  echo "⚠️ No .env file found, skipping ConfigMap"
fi

# --------------------------------------------------
# DONE
# --------------------------------------------------
echo "=============================="
echo "✅ CLUSTER READY (CLEAN STATE)"
echo "=============================="
kubectl get nodes