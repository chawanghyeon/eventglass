import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it } from "vitest";
import { RecordFields } from "./RecordFields";

it("loads the selected record field rows when the panel is opened", async () => {
  render(<RecordFields raw={{ "http.status": 500 }} />);
  expect(screen.queryByRole("cell")).not.toBeInTheDocument();
  await userEvent.click(screen.getByText("선택한 기록의 필드"));
  expect(
    await screen.findByRole("cell", { name: '$["http.status"]' }),
  ).toBeInTheDocument();
  expect(screen.getByRole("cell", { name: "number" })).toBeInTheDocument();
});
