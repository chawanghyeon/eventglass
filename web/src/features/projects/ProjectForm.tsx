import type { FormEvent } from "react";
import { useState } from "react";
import { Button } from "../../components/Button";

export function ProjectForm({
  disabled,
  onCreate,
}: {
  disabled: boolean;
  onCreate: (input: { slug: string; name: string }) => void;
}) {
  const [name, setName] = useState("");
  const [slug, setSlug] = useState("");

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onCreate({ name: name.trim(), slug: slug.trim() });
  }

  return (
    <form className="project-form" onSubmit={submit}>
      <div className="form-heading">
        <div>
          <p className="eyebrow">새 수집 경계</p>
          <h2>프로젝트 추가</h2>
        </div>
        <p>환경이나 제품 단위로 데이터를 분리합니다.</p>
      </div>
      <div className="project-form__fields">
        <label>
          표시 이름
          <input
            maxLength={200}
            onChange={(event) => setName(event.target.value)}
            placeholder="결제 API"
            required
            value={name}
          />
        </label>
        <label>
          Slug
          <input
            autoCapitalize="none"
            maxLength={64}
            onChange={(event) => setSlug(event.target.value.toLowerCase())}
            pattern={"[a-z0-9\\-]+"}
            placeholder="payments-api"
            required
            spellCheck={false}
            value={slug}
          />
        </label>
        <Button disabled={disabled} type="submit">
          {disabled ? "추가 중…" : "프로젝트 추가"}
        </Button>
      </div>
    </form>
  );
}
