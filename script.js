document.addEventListener("DOMContentLoaded", function () {
    const switcher = document.querySelector(".custom-version-switcher");

    if (!switcher) {
        return;
    }

    const button = switcher.querySelector(".custom-version-current");

    button.addEventListener("click", function (event) {
        event.stopPropagation();

        const isOpen = switcher.classList.toggle("open");

        button.setAttribute(
            "aria-expanded",
            isOpen ? "true" : "false"
        );
    });

    document.addEventListener("click", function () {
        switcher.classList.remove("open");
        button.setAttribute("aria-expanded", "false");
    });
});
