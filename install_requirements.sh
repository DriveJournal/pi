# Ensure the script is executed in the directory where requirements.txt is located
cd "$(dirname "$0")"

# Run pip install with no cache and breaking system packages
pip install --no-cache-dir --break-system-packages -r requirements.txt