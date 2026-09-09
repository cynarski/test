document.addEventListener("DOMContentLoaded", function () {
    const versions = document.querySelector(".rst-versions");
    const header = document.querySelector(".wy-side-nav-search");

    if (!versions || !header) {
        return;
    }

    const searchForm = header.querySelector("form");

    if (searchForm) {
        header.insertBefore(versions, searchForm);
    } else {
        header.appendChild(versions);
    }
});
