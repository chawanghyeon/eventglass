import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button } from "../components/Button";

interface State {
  error?: Error;
}

export class ErrorBoundary extends Component<{ children: ReactNode }, State> {
  state: State = {};

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("Eventglass UI failed", error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <main className="fatal-state">
          <span className="brand-mark" aria-hidden="true">
            E
          </span>
          <h1>화면을 불러오지 못했습니다.</h1>
          <p>페이지를 새로 고쳐 다시 시도해 주세요.</p>
          <Button onClick={() => window.location.reload()} type="button">
            새로 고침
          </Button>
        </main>
      );
    }
    return this.props.children;
  }
}
