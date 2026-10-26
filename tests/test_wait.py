from processor.wait import remaining


def test_remaining_counts_from_the_committed_offset_or_the_partition_start() -> None:
    bounds = {0: (0, 100), 1: (40, 90), 2: (0, 0)}

    assert remaining(bounds, {0: 100, 1: 90}) == 0
    assert remaining(bounds, {0: 70}) == 30 + 50
    assert remaining(bounds, {0: -1001, 1: 10}) == 100 + 50
