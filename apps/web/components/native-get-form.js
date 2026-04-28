"use client";

function buildSearchText(form) {
  const formData = new FormData(form);
  const searchParams = new URLSearchParams();

  for (const [name, value] of formData.entries()) {
    if (typeof value !== "string") {
      continue;
    }
    searchParams.append(name, value);
  }

  return searchParams.toString();
}

export function NativeGetForm({ action = "", className, children }) {
  const handleSubmit = (event) => {
    event.preventDefault();

    const form = event.currentTarget;
    const searchText = buildSearchText(form);
    const targetPath = action || window.location.pathname;

    window.location.assign(searchText ? `${targetPath}?${searchText}` : targetPath);
  };

  return (
    <form action={action} method="get" className={className} onSubmit={handleSubmit}>
      {children}
    </form>
  );
}
