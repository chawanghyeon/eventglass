import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import { ProjectCard } from "./ProjectCard";

afterEach(() => vi.restoreAllMocks());

function show() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  vi.spyOn(endpoints, "projectKeys").mockResolvedValue([
    {
      id: "key1",
      dsn: "https://public@example.invalid/1",
      public_key: "public",
    },
  ]);
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <ProjectCard
          admin
          busy={false}
          userId="1"
          project={{ id: "1", name: "Shop", slug: "shop", is_active: true }}
          onCreateKey={vi.fn()}
          onToggle={vi.fn()}
          onRevokeKey={vi.fn()}
        />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}
it("copies the actual Sentry configuration with masking and reports success", async () => {
  const user = userEvent.setup();
  const write = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
  show();
  await user.click(await screen.findByText("Sentry SDK 연결 방법"));
  await user.click(screen.getByRole("button", { name: "설정 복사" }));
  expect(write).toHaveBeenCalledWith(
    expect.stringContaining('dsn: "https://public@example.invalid/1"'),
  );
  expect(write).toHaveBeenCalledWith(
    expect.stringContaining("maskAllText: true"),
  );
  expect(screen.getByRole("status")).toHaveTextContent(
    "클립보드에 복사했습니다.",
  );
});
it("reports clipboard denial instead of claiming that copying succeeded", async () => {
  const user = userEvent.setup();
  vi.spyOn(navigator.clipboard, "writeText").mockRejectedValue(
    new Error("denied"),
  );
  show();
  await user.click(await screen.findByRole("button", { name: "DSN 복사" }));
  expect(screen.getByRole("alert")).toHaveTextContent("복사하지 못했습니다");
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
});
it("confirms DSN copy only after the clipboard write completes", async () => {
  const user = userEvent.setup();
  const write = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
  show();
  await user.click(await screen.findByRole("button", { name: "DSN 복사" }));
  expect(write).toHaveBeenCalledWith("https://public@example.invalid/1");
  expect(screen.getByRole("button", { name: "복사됨 ✓" })).toBeVisible();
});
