# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import pytest
from pydantic import ValidationError

from mo_delay_agent import delay_agent


@pytest.mark.asyncio
async def test_invalid_urls():
    with pytest.raises(ValidationError):
        await delay_agent.producer("test", "test", "test")
