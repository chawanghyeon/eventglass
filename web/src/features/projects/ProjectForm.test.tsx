import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ProjectForm } from "./ProjectForm";

describe("ProjectForm", () => {
  it("submits a normalized slug and trimmed display name", async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn();
    render(<ProjectForm disabled={false} onCreate={onCreate} />);

    await user.type(screen.getByLabelText("표시 이름"), "  결제 API  ");
    await user.type(
      screen.getByLabelText("프로젝트 식별자 (영문)"),
      "Payments-API",
    );
    await user.click(screen.getByRole("button", { name: "프로젝트 추가" }));

    expect(onCreate).toHaveBeenCalledWith({
      name: "결제 API",
      slug: "payments-api",
    });
  });

  it("keeps the primary action disabled during creation", () => {
    render(<ProjectForm disabled onCreate={vi.fn()} />);
    expect(screen.getByRole("button", { name: "추가 중…" })).toBeDisabled();
  });
});
