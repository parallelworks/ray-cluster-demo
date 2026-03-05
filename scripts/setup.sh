#!/bin/bash
# setup.sh — Install Ray into a virtual environment
#
# Environment variables:
#   RAY_VERSION - Ray version to install (default: 2.40.0)

set -e

echo "=========================================="
echo "Ray Setup: $(date)"
echo "=========================================="

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
RAY_VERSION="${RAY_VERSION:-2.40.0}"
VENV_DIR="${JOB_DIR}/.venv"

# Find Python
PYTHON_CMD=""
for cmd in python3 python; do
    command -v $cmd &>/dev/null && { PYTHON_CMD=$cmd; break; }
done
if [ -z "${PYTHON_CMD}" ]; then
    echo "[ERROR] Python not found"
    exit 1
fi
echo "Python: ${PYTHON_CMD} ($(${PYTHON_CMD} --version 2>&1))"

# Check if Ray is already installed at correct version
INSTALLED_VERSION=$(${PYTHON_CMD} -c "import ray; print(ray.__version__)" 2>/dev/null || echo "")
if [ "${INSTALLED_VERSION}" == "${RAY_VERSION}" ]; then
    echo "Ray ${RAY_VERSION} already installed, skipping."
    touch "${JOB_DIR}/SETUP_COMPLETE"
    exit 0
fi

# Try uv first (fast), fall back to pip
install_uv() {
    if command -v uv &>/dev/null; then return 0; fi
    echo "Installing uv..."
    if command -v curl &>/dev/null; then
        curl -LsSf https://astral.sh/uv/install.sh | sh
    elif command -v wget &>/dev/null; then
        wget -qO- https://astral.sh/uv/install.sh | sh
    else
        return 1
    fi
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
    command -v uv &>/dev/null
}

echo "Installing Ray ${RAY_VERSION}..."

if install_uv; then
    echo "Using uv for fast installation..."
    # Use exact Python micro version if specified (ensures head/worker match)
    UV_PYTHON="${PYTHON_MICRO_VERSION:-3.12}"
    echo "Python version for venv: ${UV_PYTHON}"
    if [ ! -d "${VENV_DIR}" ]; then
        uv venv "${VENV_DIR}" --python "${UV_PYTHON}"
    fi
    uv pip install --python "${VENV_DIR}/bin/python" \
        "ray[default]==${RAY_VERSION}" numpy
else
    echo "Using pip..."
    if [ ! -d "${VENV_DIR}" ]; then
        ${PYTHON_CMD} -m venv "${VENV_DIR}"
    fi
    "${VENV_DIR}/bin/python" -m pip install --quiet --upgrade pip
    "${VENV_DIR}/bin/python" -m pip install --quiet \
        "ray[default]==${RAY_VERSION}" numpy
fi

# Verify
"${VENV_DIR}/bin/python" -c "import ray; print(f'Ray {ray.__version__} ready')"
"${VENV_DIR}/bin/python" -c "import numpy; print(f'NumPy {numpy.__version__} ready')"

touch "${JOB_DIR}/SETUP_COMPLETE"
echo "=========================================="
echo "Setup complete! Ray ${RAY_VERSION} installed."
echo "=========================================="
