import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { CreateUserForm } from "./CreateUserForm";

describe("CreateUserForm", () => {
  it("submits a trimmed email, password, and selected role", async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn();
    render(<CreateUserForm disabled={false} onCreate={onCreate} />);

    await user.type(screen.getByLabelText("이메일"), "  admin@example.com  ");
    await user.type(screen.getByLabelText("임시 비밀번호"), "twelve-chars!");
    await user.selectOptions(screen.getByLabelText("역할"), "admin");
    await user.click(screen.getByRole("button", { name: "사용자 추가" }));

    expect(onCreate).toHaveBeenCalledWith({
      email: "admin@example.com",
      password: "twelve-chars!", // pragma: allowlist secret -- disposable form test password
      role: "admin",
    });
  });

  it("keeps the submit action disabled while creating a user", () => {
    render(<CreateUserForm disabled onCreate={vi.fn()} />);
    expect(screen.getByRole("button", { name: "추가 중…" })).toBeDisabled();
  });
});
