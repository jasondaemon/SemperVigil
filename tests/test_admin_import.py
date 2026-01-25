from sempervigil import admin


def test_admin_module_imports() -> None:
    assert admin.app is not None
