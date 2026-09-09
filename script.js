document.addEventListener("DOMContentLoaded", function () {
    const original = document.querySelector(".rst-versions");
    const header = document.querySelector(".wy-side-nav-search");

    if (!original || !header) {
        return;
    }

    // Aktualna wersja
    const current = original.querySelector(".rst-current-version");

    let currentVersion = "version";

    if (current) {
        // Najczęściej sphinx-multiversion ma wersję w elemencie po prawej
        const text = current.textContent
            .replace("Versions", "")
            .trim();

        if (text) {
            currentVersion = text;
        }
    }

    // Pobierz linki z oryginalnego menu
    const links = Array.from(
        original.querySelectorAll(".rst-other-versions a")
    );

    // Główny kontener
    const wrapper = document.createElement("div");
    wrapper.className = "top-version-switcher";

    // Przycisk
    const button = document.createElement("button");
    button.className = "top-version-button";
    button.type = "button";
    button.setAttribute("aria-expanded", "false");

    button.innerHTML = `
        <span>${currentVersion}</span>
        <span class="top-version-arrow">▼</span>
    `;

    // Lista
    const menu = document.createElement("div");
    menu.className = "top-version-menu";

    links.forEach((link) => {
        const item = document.createElement("a");

        item.href = link.href;
        item.textContent = link.textContent.trim();

        if (item.textContent === currentVersion) {
            item.classList.add("active");
        }

        menu.appendChild(item);
    });

    wrapper.appendChild(button);
    wrapper.appendChild(menu);

    // Wstaw przed wyszukiwarką
    const search = header.querySelector("form");

    if (search) {
        header.insertBefore(wrapper, search);
    } else {
        header.appendChild(wrapper);
    }

    // Rozwijanie
    button.addEventListener("click", function (event) {
        event.stopPropagation();

        const open = wrapper.classList.toggle("open");

        button.setAttribute(
            "aria-expanded",
            open ? "true" : "false"
        );
    });

    // Kliknięcie poza menu zamyka
    document.addEventListener("click", function () {
        wrapper.classList.remove("open");
        button.setAttribute("aria-expanded", "false");
    });

    // Schowaj oryginalny dolny panel
    original.style.display = "none";
});
