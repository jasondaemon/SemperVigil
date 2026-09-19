from unittest.mock import Mock

import pytest

from sempervigil.event_publication_store import promote

pytestmark = pytest.mark.offline


@pytest.mark.parametrize("qualification,predecessor", [(None,None), (True,None), ("bad",None),
                                                       ("a"*64,True), ("a"*64,"bad")])
def test_bad_publication_ids_cannot_open_database(qualification, predecessor):
    factory = Mock(side_effect=AssertionError("must not connect"))
    with pytest.raises(ValueError, match="invalid_publication_identity"):
        promote(factory, {}, {}, qualification_id=qualification, expected_predecessor=predecessor)
    factory.assert_not_called()
