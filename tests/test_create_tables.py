import pytest
from sqlalchemy import text


class TestCreateTables:
    @pytest.mark.asyncio
    async def test_create_tables_success(self, session):
        """Проверка доступа к таблице логов"""
        res = await session.execute(text("SELECT * FROM web_logs"))
        assert res
