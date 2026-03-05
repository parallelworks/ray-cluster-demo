#!/bin/bash
# run_benchmark.sh — Submit Ray benchmark and stream results to dashboard
#
# Environment variables:
#   DASHBOARD_URL - URL of the dashboard
#   RAY_HEAD_IP   - IP of the Ray head node (for site detection)
#   NUM_TASKS     - Number of benchmark tasks (default: 500)
#   MATRIX_SIZE   - Matrix size for CPU compute (default: 500)

set -e

echo "=========================================="
echo "Ray Benchmark Starting: $(date)"
echo "=========================================="
echo "Dashboard URL: ${DASHBOARD_URL}"
echo "Ray head IP:   ${RAY_HEAD_IP}"
echo "Num tasks:     ${NUM_TASKS:-500}"
echo "Matrix size:   ${MATRIX_SIZE:-500}"

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
SCRIPT_DIR="${JOB_DIR}/scripts"

# Activate venv if available
VENV_DIR="${JOB_DIR}/.venv"
if [ -f "${VENV_DIR}/bin/python" ]; then
    source "${VENV_DIR}/bin/activate"
    PYTHON_CMD="${VENV_DIR}/bin/python"
else
    PYTHON_CMD="python3"
fi

# Verify Ray is running
ray status

# Auto-detect cluster name for on-prem (site-1)
CLUSTER_NAME=""
SCHEDULER_TYPE="ssh"
if command -v pw &>/dev/null || [ -x "${HOME}/pw/pw" ]; then
    PW_CMD=$(command -v pw 2>/dev/null || echo "${HOME}/pw/pw")
    while IFS= read -r uri; do
        name="${uri##*/}"
        CLUSTER_NAME="${name}"
        break
    done < <(${PW_CMD} cluster list 2>/dev/null | awk '/^pw:\/\/'"${PW_USER}"'/ && /active/ && /existing/ {print $1}')
fi

echo "On-prem cluster name: ${CLUSTER_NAME:-unknown}"

# Run benchmark
${PYTHON_CMD} "${SCRIPT_DIR}/benchmark.py" \
    --dashboard-url "${DASHBOARD_URL}" \
    --ray-head-ip "${RAY_HEAD_IP}" \
    --num-tasks "${NUM_TASKS:-500}" \
    --matrix-size "${MATRIX_SIZE:-500}" \
    --onprem-cluster-name "${CLUSTER_NAME}"

echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
