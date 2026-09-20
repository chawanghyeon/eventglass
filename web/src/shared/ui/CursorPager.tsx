export function CursorPager({ paging, label }: {
  paging: { canFirst: boolean; canNext: boolean; first: () => void; next: () => void };
  label: string;
}) {
  if (!paging.canFirst && !paging.canNext) return null;
  return <nav className="button-row" aria-label={`${label} pagination`}>
    <button type="button" disabled={!paging.canFirst} onClick={paging.first}>First page</button>
    <button type="button" disabled={!paging.canNext} onClick={paging.next}>Next page</button>
  </nav>;
}
