from unittest.mock import Mock

import pytest

from sempervigil.event_publication_store import promote, load_export

pytestmark = pytest.mark.offline


@pytest.mark.parametrize("qualification,predecessor", [(None,None), (True,None), ("bad",None),
                                                       ("a"*64,True), ("a"*64,"bad")])
def test_bad_publication_ids_cannot_open_database(qualification, predecessor):
    factory = Mock(side_effect=AssertionError("must not connect"))
    with pytest.raises(ValueError, match="invalid_publication_identity"):
        promote(factory, {}, {}, qualification_id=qualification, expected_predecessor=predecessor)
    factory.assert_not_called()


@pytest.mark.parametrize("identities", [None, "one", [True], ["../one"], ["one", "one"],
                                       [str(i) for i in range(21)]])
def test_export_bounds_before_database(identities):
    factory = Mock(side_effect=AssertionError("must not connect"))
    with pytest.raises(ValueError, match="invalid_publication_export_ids"):
        load_export(factory, identities)
    factory.assert_not_called()


def test_empty_export_reads_nothing():
    factory = Mock(side_effect=AssertionError("must not connect"))
    assert load_export(factory, []) == {"qualified_revisions": {}, "promoted_revision_ids": {},
                                       "managed_event_ids": [], "withheld": {}, "withdrawn": {}}
    factory.assert_not_called()
