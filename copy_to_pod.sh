#!/bin/bash
# -------------------------------
# Copy Test Files to Kubernetes Pod
# -------------------------------
# Copies test files to a pod for manual execution.
#
# Usage:
#   ./copy_to_pod.sh <pod-name> [namespace] [container]
#
# Examples:
#   ./copy_to_pod.sh jupyter-pod
#   ./copy_to_pod.sh jupyter-pod hopsworks
#   ./copy_to_pod.sh jupyter-pod hopsworks jupyter
#
# After copying, run tests manually in the pod:
#   cd /hopsfs/Jupyter/deltars-test && python run_cluster.py

set -e

POD_NAME="${1}"
NAMESPACE="${2:-default}"
CONTAINER="${3:-jupyter}"
REMOTE_DIR="/hopsfs/Jupyter/deltars-test"

if [ -z "$POD_NAME" ]; then
    echo "Usage: $0 <pod-name> [namespace] [container]"
    echo ""
    echo "Examples:"
    echo "  $0 jupyter-pod"
    echo "  $0 jupyter-pod hopsworks"
    echo "  $0 jupyter-pod hopsworks jupyter"
    exit 1
fi

echo "=============================================="
echo "Copying Delta-rs Tests to Pod"
echo "=============================================="
echo "Pod:       $POD_NAME"
echo "Namespace: $NAMESPACE"
echo "Container: $CONTAINER"
echo "Remote:    $REMOTE_DIR"
echo "=============================================="

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo "[1/2] Creating remote directory..."
kubectl exec -n "$NAMESPACE" -c "$CONTAINER" "$POD_NAME" -- mkdir -p "$REMOTE_DIR/tests"

echo ""
echo "[2/2] Copying test files to pod..."

# Copy main runner
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/run_cluster.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/run_cluster.py"

# Copy test modules
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/tests/__init__.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/tests/__init__.py" 2>/dev/null || \
    kubectl exec -n "$NAMESPACE" -c "$CONTAINER" "$POD_NAME" -- touch "$REMOTE_DIR/tests/__init__.py"
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/tests/config.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/tests/config.py"
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/tests/config_cluster.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/tests/config_cluster.py"
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/tests/test_write_operations.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/tests/test_write_operations.py"
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/tests/test_read_operations.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/tests/test_read_operations.py"
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/tests/test_dml_operations.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/tests/test_dml_operations.py"
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/tests/test_maintenance.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/tests/test_maintenance.py"
kubectl cp -c "$CONTAINER" "$SCRIPT_DIR/tests/test_advanced.py" "$NAMESPACE/$POD_NAME:$REMOTE_DIR/tests/test_advanced.py"

echo ""
echo "=============================================="
echo "Files copied successfully!"
echo "=============================================="
echo ""
echo "To run tests, open a terminal in the pod and execute:"
echo ""
echo "  cd $REMOTE_DIR && python run_cluster.py"
echo ""
echo "To cleanup after testing:"
echo "  rm -rf $REMOTE_DIR"
echo ""
