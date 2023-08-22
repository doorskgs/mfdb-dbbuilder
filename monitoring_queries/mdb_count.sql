select count(*)
from "Metabolites"
where not (
    starts_with(mid, 'S:') or starts_with(mid, 'H:') or starts_with(mid, 'C:') or starts_with(mid, 'A:')
	or starts_with(mid, 'P:')or starts_with(mid, 'X:')or starts_with(mid, 'K:')or starts_with(mid, 'L:')
	or starts_with(mid, 'M:')
)
