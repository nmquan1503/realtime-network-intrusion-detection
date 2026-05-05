#!/bin/bash
set -e

# --------------------------------------------------
# Load required kernel modules
# --------------------------------------------------
sudo modprobe br_netfilter
sudo modprobe overlay
cat <<EOF | sudo tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF
sudo sysctl --system



# --------------------------------------------------
# Install CNI plugins
# --------------------------------------------------
sudo mkdir -p /opt/cni/bin
curl -L https://github.com/containernetworking/plugins/releases/download/v1.5.1/cni-plugins-linux-amd64-v1.5.1.tgz | sudo tar -C /opt/cni/bin -xz



# --------------------------------------------------
# Reset any existing kubeadm cluster
# --------------------------------------------------
echo "==> Reset kubeadm..."
sudo kubeadm reset -f
sudo iptables -F
rm -rf $HOME/.kube



# --------------------------------------------------
# Initialize Kubernetes cluster
# --------------------------------------------------
echo "==> Init Kubernetes cluster..."
sudo kubeadm init --pod-network-cidr=10.244.0.0/16
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config



# --------------------------------------------------
# Install Flannel CNI network
# --------------------------------------------------
echo "==> Apply Flannel network..."
kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml

echo "==> Allow pods on master node..."
kubectl taint nodes --all node-role.kubernetes.io/control-plane-



# --------------------------------------------------
# Setup namespaces and storage
# --------------------------------------------------
kubectl create namespace bigdata
kubectl apply -f 'https://strimzi.io/install/latest?namespace=bigdata' -n bigdata
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.34/deploy/local-path-storage.yaml



# --------------------------------------------------
# Create ConfigMap from .env
# --------------------------------------------------
echo "==> Create ConfigMap from .env..."
kubectl create configmap config --from-env-file=.env -n bigdata --dry-run=client -o yaml | kubectl apply -f -

