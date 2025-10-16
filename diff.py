import csv
from packaging import version

def parse_pip_list(file_path):
    """Wczytuje plik z `pip list` i zwraca słownik {pakiet: wersja}."""
    packages = {}
    with open(file_path, encoding='utf-8') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        # pomijamy nagłówki np. "Package Version"
        if not line or line.lower().startswith("package") or line.startswith("-"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            pkg, ver = parts[0], parts[1]
            packages[pkg] = ver
    return packages


def compare_versions(main_ver, other_ver):
    """Zwraca 'NEWER', 'OLDER', 'THE SAME' lub '' jeśli brak danych."""
    if not other_ver:
        return ""
    if not main_ver:
        return "UNKNOWN"
    try:
        if version.parse(other_ver) > version.parse(main_ver):
            return "NEWER"
        elif version.parse(other_ver) < version.parse(main_ver):
            return "OLDER"
        else:
            return "THE SAME"
    except Exception:
        return "INVALID"


def generate_comparison(main_file, other_files, output_file="comparison.csv"):
    # Wczytaj dane
    main_packages = parse_pip_list(main_file)
    others = {name: parse_pip_list(path) for name, path in other_files.items()}

    # Zbierz wszystkie unikalne nazwy bibliotek
    all_packages = set(main_packages.keys())
    for data in others.values():
        all_packages.update(data.keys())

    # Przygotuj dane do CSV
    fieldnames = ["Package", "main"] + list(others.keys()) + [f"{k} vs main" for k in others.keys()]

    with open(output_file, "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for pkg in sorted(all_packages):
            row = {"Package": pkg, "main": main_packages.get(pkg, "")}
            # Dodaj wersje i porównania
            for kernel_name, data in others.items():
                ver = data.get(pkg, "")
                row[kernel_name] = ver
                row[f"{kernel_name} vs main"] = compare_versions(main_packages.get(pkg, ""), ver)
            writer.writerow(row)

    print(f"✅ Wynik zapisano do pliku: {output_file}")


if __name__ == "__main__":
    # Przykład użycia:
    generate_comparison(
        main_file="main.txt",
        other_files={
            "kernel1": "kernel1.txt",
            "kernel2": "kernel2.txt",
            "kernel3": "kernel3.txt",
        },
        output_file="comparison.csv"
    )
