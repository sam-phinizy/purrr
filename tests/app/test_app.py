# Lets write a pytest test that starts the app and makes sure it runs w/o returning a 1

import pytest

from purrr.tui import PrefectApp, CachingPrefectClient


@pytest.mark.asyncio
async def test_app_starts(db):
    app = PrefectApp(client=CachingPrefectClient(db_name=":memory:"))
    async with app.run_test() as pilot:
        await pilot.press("q")
