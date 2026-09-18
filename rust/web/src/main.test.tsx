import { afterEach, beforeEach, expect, it, vi } from "vitest";
const root = vi.hoisted(() => ({ render: vi.fn(), create: vi.fn() }));
vi.mock("react-dom/client", () => ({ createRoot: root.create }));
beforeEach(() => {
  vi.resetModules();
  root.render.mockReset();
  root.create.mockReset().mockReturnValue({ render: root.render });
});
afterEach(async () => {
  const { router } = await import("./app/router");
  router.dispose();
  document.getElementById("root")?.remove();
});
it("fails clearly when the hosting document omits the root element", async () => {
  await expect(import("./main")).rejects.toThrow("Missing #root element");
  expect(root.create).not.toHaveBeenCalled();
});
it("mounts the protected application tree in the provided root", async () => {
  const element = document.createElement("div");
  element.id = "root";
  document.body.append(element);
  await import("./main");
  expect(root.create).toHaveBeenCalledOnce();
  expect(root.create).toHaveBeenCalledWith(element);
  expect(root.render).toHaveBeenCalledOnce();
  const tree = root.render.mock.lastCall?.[0];
  expect(tree.type).toBe(Symbol.for("react.strict_mode"));
  expect(tree.props.children.type.name).toBe("ErrorBoundary");
  expect(tree.props.children.props.children.type.name).toBe("AppProviders");
});
