const helpButton = document.querySelector(".help-fab");
const helpPanel = document.querySelector("#help-panel");
const closeHelpButton = document.querySelector("[data-help-close]");
const modalOpenButtons = document.querySelectorAll("[data-modal-open]");
const modalCloseButtons = document.querySelectorAll("[data-modal-close]");
const flashMessages = document.querySelectorAll(".flash");
const flashCloseButtons = document.querySelectorAll(".flash__close");
const forms = document.querySelectorAll("form");
const validationFields = document.querySelectorAll("input, select, textarea");

if (helpButton && helpPanel) {
    const setHelpState = (open) => {
        helpPanel.hidden = !open;
        helpButton.setAttribute("aria-expanded", String(open));
    };

    helpButton.addEventListener("click", () => {
        setHelpState(helpPanel.hidden);
    });

    closeHelpButton?.addEventListener("click", () => {
        setHelpState(false);
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            setHelpState(false);
        }
    });
}

const setModalState = (modalId, open) => {
    const modal = document.getElementById(modalId);
    if (!modal) {
        return;
    }
    modal.hidden = !open;
};

modalOpenButtons.forEach((button) => {
    button.addEventListener("click", () => {
        setModalState(button.dataset.modalOpen, true);
    });
});

modalCloseButtons.forEach((button) => {
    button.addEventListener("click", () => {
        setModalState(button.dataset.modalClose, false);
    });
});

document.querySelectorAll(".modal-backdrop").forEach((modal) => {
    modal.addEventListener("click", (event) => {
        if (event.target === modal) {
            modal.hidden = true;
        }
    });
});

document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
        document.querySelectorAll(".modal-backdrop").forEach((modal) => {
            modal.hidden = true;
        });
    }
});

flashMessages.forEach((message) => {
    window.setTimeout(() => {
        message.style.opacity = "0";
        message.style.transform = "translateY(-8px)";
        message.style.transition = "opacity 0.25s ease, transform 0.25s ease";
        window.setTimeout(() => {
            message.remove();
        }, 250);
    }, 3200);
});

flashCloseButtons.forEach((button) => {
    button.addEventListener("click", () => {
        button.closest(".flash")?.remove();
    });
});

validationFields.forEach((field) => {
    field.addEventListener("blur", () => {
        field.classList.add("field-touched");
    });

    field.addEventListener("input", () => {
        if (field.checkValidity()) {
            field.classList.remove("field-touched");
        }
    });
});

forms.forEach((form) => {
    form.addEventListener("submit", () => {
        form.classList.add("was-submitted");
    });
});
