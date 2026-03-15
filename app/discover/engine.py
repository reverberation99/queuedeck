def clamp01(v: float) -> float:
    if v < 0:
        return 0.0
    if v > 1:
        return 1.0
    return float(v)


def provider_presence_bonus(provider_hits: int, bonuses: dict[int, float] | None = None) -> float:
    bonuses = bonuses or {2: 0.08, 3: 0.18, 4: 0.28, 5: 0.34, 6: 0.40}
    if provider_hits >= 6:
        return float(bonuses.get(6, 0.40))
    if provider_hits == 5:
        return float(bonuses.get(5, 0.34))
    if provider_hits == 4:
        return float(bonuses.get(4, 0.28))
    if provider_hits == 3:
        return float(bonuses.get(3, 0.18))
    if provider_hits == 2:
        return float(bonuses.get(2, 0.08))
    return 0.0


def consensus_factor(provider_hits: int) -> float:
    if provider_hits <= 1:
        return 0.62
    if provider_hits == 2:
        return 1.00
    if provider_hits == 3:
        return 1.06
    if provider_hits == 4:
        return 1.10
    return 1.12


def build_discover_score(
    provider_scores: dict[str, float | None],
    weights: dict[str, float] | None = None,
    bonuses: dict[int, float] | None = None,
    hot_threshold: float = 0.82,
) -> dict:
    weights = weights or {}

    active_scores = [float(v) for v in provider_scores.values() if v is not None]
    provider_hits = len(active_scores)

    weighted_total = 0.0
    weight_sum = 0.0

    for key, val in provider_scores.items():
        if val is None:
            continue
        w = float(weights.get(key, 1.0))
        weighted_total += float(val) * w
        weight_sum += w

    base_score = (weighted_total / weight_sum) if weight_sum > 0 else 0.0
    bonus = provider_presence_bonus(provider_hits, bonuses=bonuses)
    pre_consensus_score = clamp01(base_score + bonus)

    c_factor = consensus_factor(provider_hits)
    composite_score = round(clamp01(pre_consensus_score * c_factor), 4)

    special_hot = composite_score >= float(hot_threshold)

    breakdown = {
        "provider_scores": {
            str(k): (round(float(v), 4) if v is not None else None)
            for k, v in provider_scores.items()
        },
        "base_score": round(float(base_score), 4),
        "source_bonus": round(float(bonus), 4),
        "pre_consensus_score": round(float(pre_consensus_score), 4),
        "consensus_factor": round(float(c_factor), 4),
        "final_score": composite_score,
        "provider_hits": provider_hits,
        "hot_threshold": round(float(hot_threshold), 4),
    }

    return {
        "provider_hits": provider_hits,
        "composite_score": composite_score,
        "special_hot": special_hot,
        "score_breakdown": breakdown,
    }


def normalize_and_score_items(
    items: list[dict],
    weights: dict[str, float] | None = None,
    bonuses: dict[int, float] | None = None,
    hot_threshold: float = 0.82,
) -> list[dict]:
    out = []
    for item in items:
        provider_scores = item.get("provider_scores") or {}
        score_bits = build_discover_score(
            provider_scores,
            weights=weights,
            bonuses=bonuses,
            hot_threshold=hot_threshold,
        )
        merged = dict(item)
        merged.update(score_bits)
        out.append(merged)

    out.sort(
        key=lambda x: (
            float(x.get("composite_score") or 0.0),
            float(x.get("popularity") or 0.0),
            float(x.get("vote_average") or 0.0),
        ),
        reverse=True,
    )
    return out
