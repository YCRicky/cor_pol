import pytest

from aftertake.rounds import crypto_5m_bounds_from_slug


def test_crypto_5m_slug_epoch_is_the_round_start():
    assert crypto_5m_bounds_from_slug("btc-updown-5m-900") == (900, 1_200)


@pytest.mark.parametrize(
    "slug",
    (
        "btc-updown-15m-900",
        "btc-updown-5m-nope",
        "btc-updown-5m-901",
        "btc-updown-5m-0",
    ),
)
def test_crypto_5m_bounds_reject_invalid_or_unaligned_slugs(slug):
    with pytest.raises(ValueError):
        crypto_5m_bounds_from_slug(slug)
