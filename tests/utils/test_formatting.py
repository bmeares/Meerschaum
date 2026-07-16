def test_get_console_is_cached():
    from meerschaum.utils.formatting import get_console

    assert get_console() is get_console()
