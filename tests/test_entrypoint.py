import sys

import entrypoint


def test_main_uses_port_from_environment(monkeypatch):
    monkeypatch.setenv("PORT", "9000")
    monkeypatch.setattr(sys, "argv", ["pytest"])
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        entrypoint.runpy,
        "run_module",
        lambda name, run_name: calls.append((name, run_name)),
    )

    entrypoint.main()

    assert sys.argv == ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9000"]
    assert calls == [("uvicorn", "__main__")]


def test_main_defaults_to_port_8000(monkeypatch):
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.setattr(sys, "argv", ["pytest"])
    monkeypatch.setattr(entrypoint.runpy, "run_module", lambda name, run_name: None)

    entrypoint.main()

    assert sys.argv[-2:] == ["--port", "8000"]
