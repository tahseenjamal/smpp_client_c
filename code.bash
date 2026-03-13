find . \
  -type d \( -name target -o -name .git \) -prune -o \
  -type f \( -name "*.c" -o -name "*.h" -o -name "*.conf" \) -print | while read -r f; do
    echo "===== $f ====="
    cat "$f"
    echo
done
