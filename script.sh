#!/usr/bin/env bash
set -euo pipefail

# Ustawienia domyślne (można nadpisać argumentami)
REPO_LIST_FILE="${1:-repos.txt}"
PATTERN_LIST_FILE="${2:-patterns.txt}"
OUTPUT_CSV="${3:-results.csv}"

# ---- Funkcje pomocnicze ----

# csv_escape: ucieka pola CSV i otacza w cudzysłowy (RFC 4180)
csv_escape() {
  local s="${1:-}"
  # Zamień " na ""
  s="${s//\"/\"\"}"
  # Otocz w cudzysłowy
  printf '"%s"' "$s"
}

# trim: usuwa wiodące/końcowe białe znaki
trim() {
  local s="${1:-}"
  # Użyj sed do trim
  printf '%s' "$s" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

# sanitize dirname (dla katalogu klona)
sanitize_dirname() {
  printf '%s' "$1" | sed -e 's/[^a-zA-Z0-9._-]/_/g' | cut -c1-200
}

# ---- Walidacja wejścia ----

if [[ ! -f "$REPO_LIST_FILE" ]]; then
  echo "Plik z listą repozytoriów nie istnieje: $REPO_LIST_FILE"
  echo "Utwórz plik z jednym URL git na linię lub podaj ścieżkę jako argument."
  echo "Przykład repos.txt:"
  echo "https://github.com/user/repo1.git"
  echo "git@github.com:org/repo2.git"
  exit 2
fi

if [[ ! -f "$PATTERN_LIST_FILE" ]]; then
  echo "Plik z listą patternów nie istnieje: $PATTERN_LIST_FILE"
  echo "Utwórz plik z jednym patternem na linię (puste i zaczynające się od # są ignorowane)."
  echo "Przykład patterns.txt:"
  echo "cernel1.tar.gz"
  echo "# .*\\.tar\\.gz  (możesz użyć też zwykłego tekstu; git grep traktuje -e jako wzorzec grep)"
  exit 2
fi

# Wczytaj patterny do tablicy, pomijając puste i komentarze
mapfile -t PATTERNS < <(awk 'NF && $0 !~ /^[[:space:]]*#/' "$PATTERN_LIST_FILE" | sed -e 's/[[:space:]]*$//')

if [[ ${#PATTERNS[@]} -eq 0 ]]; then
  echo "Brak patternów do przeszukania po odfiltrowaniu pustych linii/komentarzy w: $PATTERN_LIST_FILE"
  exit 0
fi

# temp base dir for all clones
BASE_TMP="$(mktemp -d)"
cleanup_all() {
  rm -rf "$BASE_TMP"
}
trap cleanup_all EXIT

echo "Plik z repo: $REPO_LIST_FILE"
echo "Plik z patternami: $PATTERN_LIST_FILE"
echo "Plik wynikowy CSV: $OUTPUT_CSV"
echo "Tymczasowy katalog bazowy: $BASE_TMP"
echo

# Zainicjuj plik CSV z nagłówkiem
{
  echo 'repo_url,ref,file_path,line_number,matched_line,pattern'
} > "$OUTPUT_CSV"

# ---- Główna pętla po repo ----

# Read repo list, skip empty lines and lines starting with #
while IFS= read -r raw || [[ -n "${raw-}" ]]; do
  repo="$(trim "${raw-}")"
  # skip blank or comment lines
  if [[ -z "$repo" || "${repo:0:1}" == "#" ]]; then
    continue
  fi

  echo "=============================="
  echo "Repo: $repo"
  REPO_DIR="$BASE_TMP/$(sanitize_dirname "$repo")"

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

  # Dla każdego refa i dla każdego patternu uruchom git grep,
  # aby w CSV móc zapisać konkretny pattern, który zadziałał.
  for ref in "${REFS[@]}"; do
    [[ -z "$ref" ]] && continue

    for pattern in "${PATTERNS[@]}"; do
      # Uwaga: 'pattern' jest traktowany jak wzorzec 'grep' (ERE jeśli Git skonfigurowany, zwykle BRE/ERE).
      # Jeśli chcesz dosłowny tekst, najlepiej uciekać regex lub użyć -F (fixed), ale wtedy regexy nie zadziałają.
      # Tu zakładamy, że patterns.txt zawiera to, co chcesz przekazać do -e.
      if git grep -I --no-color -n -e "$pattern" "$ref" >/dev/null 2>&1; then
        FOUND_IN_REPO=1
        # Wypisz szczegóły do CSV
        git grep -I --no-color -n -e "$pattern" "$ref" | while IFS= read -r line; do
          # format linii: path:lineno:content
          file_path="${line%%:*}"
          rest="${line#*:}"
          line_no="${rest%%:*}"
          matched_line="${rest#*:}"

          # CSV-escape pól
          esc_repo=$(csv_escape "$repo")
          esc_ref=$(csv_escape "$ref")
          esc_file=$(csv_escape "$file_path")
          esc_line_no=$(csv_escape "$line_no")
          esc_content=$(csv_escape "$matched_line")
          esc_pattern=$(csv_escape "$pattern")

          printf '%s,%s,%s,%s,%s,%s\n' \
            "$esc_repo" "$esc_ref" "$esc_file" "$esc_line_no" "$esc_content" "$esc_pattern" >> "$OUTPUT_CSV"
        done
      fi
    done
  done

  if [[ $FOUND_IN_REPO -eq 0 ]]; then
    echo "Brak wystąpień w repo: $repo"
  else
    echo "Zapisano dopasowania do: $OUTPUT_CSV"
  fi

  popd >/dev/null
  echo
done < "$REPO_LIST_FILE"

echo "Gotowe. Wyniki w: $OUTPUT_CSV"
exit 0
