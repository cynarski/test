#!/usr/bin/env bash
set -euo pipefail

# Hardcoded pattern:
PATTERN="cernel1.tar.gz"

# File with repo URLs (one per line). You can pass as first arg, otherwise repos.txt
REPO_LIST_FILE="${1:-repos.txt}"

if [[ ! -f "$REPO_LIST_FILE" ]]; then
  echo "Plik z listą repozytoriów nie istnieje: $REPO_LIST_FILE"
  echo "Utwórz plik z jednym URL git na linię lub podaj ścieżkę jako argument."
  echo "Przykład zawartości repos.txt:"
  echo "https://github.com/user/repo1.git"
  echo "git@github.com:org/repo2.git"
  exit 2
fi

# temp base dir for all clones
BASE_TMP="$(mktemp -d)"
cleanup_all() {
  rm -rf "$BASE_TMP"
}
trap cleanup_all EXIT

echo "Wzorzec wyszukiwania (hardcoded): '$PATTERN'"
echo "Plik z repo: $REPO_LIST_FILE"
echo "Tymczasowy katalog bazowy: $BASE_TMP"
echo

# Read repo list, skip empty lines and lines starting with #
while IFS= read -r raw || [[ -n "$raw" ]]; do
  repo="$(echo "$raw" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  # skip blank or comment lines
  if [[ -z "$repo" || "${repo:0:1}" == "#" ]]; then
    continue
  fi

  echo "=============================="
  echo "Repo: $repo"
  REPO_DIR="$BASE_TMP/$(echo "$repo" | sed -e 's/[^a-zA-Z0-9._-]/_/g')"

  # Clone as mirror (all refs, no working tree). Use --quiet to reduce noise
  echo "Klonowanie (mirror) do: $REPO_DIR ..."
  if ! git clone --mirror --quiet "$repo" "$REPO_DIR" 2> /dev/null; then
    echo "BŁĄD: Nie udało się sklonować repo: $repo"
    echo "Przechodzę do następnego..."
    echo
    continue
  fi

  pushd "$REPO_DIR" >/dev/null

  # ensure refs are up-to-date (try, but ignore failure)
  git remote update --prune --quiet 2>/dev/null || true

  # collect refs to search (heads i remotes)
  mapfile -t REFS < <(git for-each-ref --format='%(refname:short)' refs/heads refs/remotes 2>/dev/null || true)

  if [[ ${#REFS[@]} -eq 0 ]]; then
    echo "Brak refów do przeszukania w repo: $repo"
    popd >/dev/null
    echo
    continue
  fi

  FOUND_IN_REPO=0

  for ref in "${REFS[@]}"; do
    # skip empty
    [[ -z "$ref" ]] && continue

    # Use git grep on the ref. -I ignores binary files, -n shows line numbers, --no-color for clean output
    if git grep -I --no-color -n -e "$PATTERN" "$ref" >/dev/null 2>&1; then
      if [[ $FOUND_IN_REPO -eq 0 ]]; then
        echo "Znaleziono w repo: $repo"
      fi
      FOUND_IN_REPO=1
      # print results with repo and ref prefix
      # format each matching line to: repo :: ref :: path:linia:znaleziona linia
      git grep -I --no-color -n -e "$PATTERN" "$ref" | while IFS= read -r line; do
        # line format from git grep is "path:lineno:content"
        printf '%s :: %s :: %s\n' "$repo" "$ref" "$line"
      done
    fi
  done

  if [[ $FOUND_IN_REPO -eq 0 ]]; then
    echo "Brak wystąpień w repo: $repo"
  fi

  popd >/dev/null
  echo
done < "$REPO_LIST_FILE"

echo "Gotowe."
exit 0
