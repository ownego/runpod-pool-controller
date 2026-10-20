"""How many pods a pool should run: spec.autoscaling.

Like a HorizontalPodAutoscaler on one metric. Each loop the pool's query
gives a value (e.g. average inference queue per pod); the recommendation is
ceil(ready * value / target), clamped to [replicas.min, replicas.max], and
unchanged within 10% of the target. Recommendations are kept for the
stabilization windows:

- scale up only when every recommendation in scaleUpStabilization is above
  the current size, to the lowest of them (a short spike does not add a pod
  that takes minutes to start anyway);
- scale down to the highest recommendation in scaleDownStabilization.

Without enough history for a window (just started, or the metric was
missing) the size stays as it is. The planner turns the result into creates
(surge) or ScaleIn unwires (drained, then terminated).
"""

import math
from dataclasses import dataclass

TOLERANCE = 0.1


@dataclass
class AutoscalingSpec:
    enabled: bool = False
    # PromQL returning one value for the pool ("$job" replaced), compared
    # with target per Ready pod, e.g. the average queue length per pod.
    query: str = ""
    target: float = 1.0
    up_window: float = 120  # scaleUpStabilization
    down_window: float = 1800  # scaleDownStabilization


def recommend(ready, value, target, lo, hi):
    """Pods wanted for this loop's metric value, or None when it cannot tell."""
    if value is None or ready <= 0 or target <= 0:
        return None
    ratio = value / target
    if abs(ratio - 1) <= TOLERANCE:
        wanted = ready
    else:
        wanted = math.ceil(ready * ratio)
    return max(lo, min(hi, wanted))


class Autoscaler:
    def __init__(self, up_window, down_window, recommendations=None):
        self.up_window = up_window
        self.down_window = down_window
        self.recs = [(float(t), int(r)) for t, r in (recommendations or [])]

    @classmethod
    def from_state(cls, state, up_window, down_window):
        return cls(up_window, down_window, (state or {}).get("recommendations"))

    def state(self):
        return {"recommendations": [[t, r] for t, r in self.recs]}

    def step(self, prev, rec, now):
        """Size to run now, given the current size and this loop's recommendation."""
        if rec is None:
            return prev
        self.recs.append((now, rec))
        # Keep the windows plus the newest older sample, which shows that
        # history reaches back far enough even when loops drift by seconds.
        start = now - max(self.up_window, self.down_window)
        older = [i for i, (t, _) in enumerate(self.recs) if t < start]
        self.recs = self.recs[older[-1]:] if older else self.recs
        if rec > prev:
            window = self._window(now, self.up_window)
            if window and all(r > prev for r in window):
                return min(window)
        elif rec < prev:
            window = self._window(now, self.down_window)
            if window:
                return min(prev, max(window))
        return prev

    def _window(self, now, length):
        """Recommendations of the last `length` seconds, if history covers it."""
        if not self.recs or self.recs[0][0] > now - length:
            return []
        return [r for t, r in self.recs if t >= now - length]
