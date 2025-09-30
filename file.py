#!/usr/bin/env python3
"""
scan_multiple_repos.py

Użycie:
    python3 scan_multiple_repos.py [repos.txt]

Opis:
- Plik z listą repozytoriów: jeden URL git na linię. Linia zaczynająca się od '#' lub pusta = ignorowana.
- Pattern jest hardcoded poniżej w zmiennej PATTERN.
- Wypisuje każde dopasowanie w formacie:
    repo_url :: ref :: path:linia:treść_linii
"""

from __future__ import annotations
import subprocess
import tempfile
import shutil
import sys
import os
from pathlib import Path
from typing import List

# ---- konfiguracja (hardcoded pattern) ----
PATTERN = "cernel1.tar.gz"

# ---- helpery ----
def run(cmd: List[str], cwd: Path | None = None, capture_output: bool = False) -> subprocess.CompletedProcess:
    """Uruchamia polecenie i zwraca CompletedProcess. Podnosi CalledProcessError jeśli returncode != 0, chyba że capture_output=True."""
    if capture_output:
        cp = subprocess.run(cmd, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        cp = subprocess.run(cmd, cwd=cwd, text=True)
    return cp

def clone_mirror(repo_url: str, dest: Path) -> bool:
    """Klonuje repo jako mirror. Zwraca True jeśli ok, False jeśli nie."""
    try:
        # --mirror tworzy pełny mirror refs (bez working tree)
        subprocess.run(["git", "clone", "--mirror", repo_url, str(dest)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False

def get_refs(repo_dir: Path) -> List[str]:
    """Zwraca listę refów (refs/heads i refs/remotes) w formie krótkiej (refname:short)."""
    try:
        cp = subprocess.run(
            ["git", "for-each-ref", "--format=%(refname:short)", "refs/heads", "refs/remotes"],
            cwd=repo_dir,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        refs = [r.strip() for r in cp.stdout.splitlines() if r.strip()]
        return refs
    except subprocess.CalledProcessError:
        return []

def grep_ref(repo_dir: Path, ref: str, pattern: str) -> List[str]:
    """
    Uruchamia git grep na podanym refie i zwraca listę linii wyników.
    Używa opcji:
     -I       ignoruje pliki binarne
     --no-color czysty output
     -n       numer linii
    """
    try:
        cp = subprocess.run(
            ["git", "grep", "-I", "--no-color", "-n", "-e", pattern, ref],
            cwd=repo_dir,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        # każda linia ma format: path:lineno:content
        lines = [ln for ln in cp.stdout.splitlines() if ln.strip()]
        return lines
    except subprocess.CalledProcessError as e:
        # git grep zwraca kod 1 jeśli nic nie znaleziono -> traktujemy jako brak wyników
        return []

def sanitize_dirname(repo_url: str) -> str:
    """Prosty sposób na konwersję URL do nazwy katalogu bez niebezpiecznych znaków."""
    safe = []
    for ch in repo_url:
        if ch.isalnum() or ch in "._-":
            safe.append(ch)
        else:
            safe.append("_")
    return "".join(safe)[:200]

# ---- główna logika ----
def process_repo(repo_url: str, base_tmp: Path) -> None:
    repo_dir = base_tmp / sanitize_dirname(repo_url)
    print(f"--- Repo: {repo_url}")
    # clone
    ok = clone_mirror(repo_url, repo_dir)
    if not ok:
        print(f"BŁĄD: Nie udało się sklonować repo: {repo_url}", file=sys.stderr)
        return

    # opcjonalnie: spróbuj zaktualizować refs (ignorujemy błędy)
    try:
        subprocess.run(["git", "remote", "update", "--prune"], cwd=repo_dir, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass

    refs = get_refs(repo_dir)
    if not refs:
        print(f"Brak refów do przeszukania w repo: {repo_url}")
        return

    found_any = False
    for ref in refs:
        lines = grep_ref(repo_dir, ref, PATTERN)
        if not lines:
            continue
        if not found_any:
            print(f"Znaleziono w repo: {repo_url}")
            found_any = True
        for ln in lines:
            # drukujemy: repo :: ref :: path:linia:treść
            print(f"{repo_url} :: {ref} :: {ln}")

def read_repos_file(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(path)
    repos = []
    with path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#"):
                continue
            repos.append(line)
    return repos

def main(argv: List[str]) -> int:
    repos_file = Path(argv[1]) if len(argv) > 1 else Path("repos.txt")
    try:
        repos = read_repos_file(repos_file)
    except FileNotFoundError:
        print(f"Plik z listą repozytoriów nie istnieje: {repos_file}", file=sys.stderr)
        return 2

    if not repos:
        print("Brak repozytoriów w pliku (poza komentarzami/pustymi liniami).")
        return 0

    base_tmp = Path(tempfile.mkdtemp(prefix="scan_repos_"))
    try:
        for repo in repos:
            try:
                process_repo(repo, base_tmp)
            except Exception as e:
                # nie przerywamy pętli dla jednego nieudanego repo
                print(f"BŁĄD podczas przetwarzania {repo}: {e}", file=sys.stderr)
    finally:
        # sprzątanie
        try:
            shutil.rmtree(base_tmp)
        except Exception:
            pass

    print("Gotowe.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
