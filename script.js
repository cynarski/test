document.addEventListener("DOMContentLoaded", function () {

    const original = document.querySelector(".rst-versions");
    const header = document.querySelector(".wy-side-nav-search");

    if (!original || !header) {
        return;
    }

    /*
     * Pobierz aktualną wersję.
     */
    const currentElement = original.querySelector(".rst-current-version");

    let currentVersion = "";

    if (currentElement) {
        currentVersion = currentElement.textContent
            .replace("Versions", "")
            .replace("Documentation version:", "")
            .trim();
    }

    /*
     * Pobierz wszystkie linki do wersji.
     */
    const links = original.querySelectorAll(".rst-other-versions a");

    const switcher = document.createElement("div");
    switcher.className = "custom-version-switcher";

    const button = document.createElement("button");
    button.className = "custom-version-current";
    button.type = "button";

    button.innerHTML = `
        <span>${currentVersion}</span>
        <span class="custom-version-arrow">▼</span>
    `;

    const menu = document.createElement("div");
    menu.className = "custom-version-menu";

    links.forEach(function (link) {

        const item = document.createElement("a");

        item.href = link.href;
        item.textContent = link.textContent.trim();

        if (item.textContent === currentVersion) {
            item.classList.add("active");
        }

        menu.appendChild(item);
    });

    switcher.appendChild(button);
    switcher.appendChild(menu);

    /*
     * Wstaw przed polem wyszukiwania.
     */
    const search = header.querySelector("form");

    if (search) {
        header.insertBefore(switcher, search);
    } else {
        header.appendChild(switcher);
    }

    /*
     * Obsługa rozwijania.
     */
    button.addEventListener("click", function (event) {

        event.stopPropagation();

        switcher.classList.toggle("open");
    });

    document.addEventListener("click", function () {
        switcher.classList.remove("open");
    });
});
