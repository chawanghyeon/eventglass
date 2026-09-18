import { expect, it } from "vitest";
import { feedbackMessage } from "./feedback";
import { displayPage, duration, userLabel } from "./presentation";

it("reads only a valid feedback context message", () => {
  for (const value of [
    {},
    { contexts: null },
    { contexts: "wrong" },
    { contexts: {} },
    { contexts: { feedback: null } },
    { contexts: { feedback: "wrong" } },
    { contexts: { feedback: { message: 7 } } },
  ])
    expect(feedbackMessage(value)).toBe("");
  expect(
    feedbackMessage({
      contexts: { feedback: { message: "Checkout is broken" } },
    }),
  ).toBe("Checkout is broken");
});
it("formats duration without negative playback time", () => {
  expect(duration(-1000)).toBe("0:00");
  expect(duration(61999)).toBe("1:01");
});
it("uses the first supported string identity or an anonymous label", () => {
  expect(userLabel({ id: "42", email: "user@example.test" })).toBe("42");
  expect(userLabel({ id: 42, email: "user@example.test" })).toBe(
    "user@example.test",
  );
  expect(userLabel({ username: "visitor" })).toBe("visitor");
  expect(userLabel({ name: "Customer" })).toBe("Customer");
  expect(userLabel(null)).toBe("익명 방문자");
  expect(userLabel({ id: 7 })).toBe("익명 방문자");
});
it("shows a page path without query secrets and retains non-URL route labels", () => {
  expect(displayPage("https://example.test/?token=private")).toBe(
    "example.test",
  );
  expect(
    displayPage("https://example.test/checkout?token=private#fragment"),
  ).toBe("/checkout");
  expect(displayPage("/route")).toBe("/route");
});
