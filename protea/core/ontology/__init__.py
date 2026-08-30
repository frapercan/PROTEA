"""An encoder over an ontology's own structure, independent of any corpus.

Built here rather than taken from a library because it has to work on
ontologies other than GO, and because what we need from it is narrow: a term
must land somewhere that says what it subsumes and what subsumes it, so that a
term with one example can borrow the geometry of terms with a thousand.

The motivating measurement, on GO snapshot 36038118 against bank cbb35a32:
40,214 non-obsolete terms, of which 8,804 have no carrier at all and 11,788
have ten or fewer. Half the ontology is in the few-shot regime. Of the 69,188
is_a/part_of edges, 51.4 per cent have ten or fewer carriers on the child side,
and the median edge cuts the carrier set to 7.8 per cent of the parent's. Each
step down is a sharp decision and half of them have almost nothing to learn it
from individually. What can be learned is the shape they share.
"""

from protea.core.ontology.dag import Dag, EdgeSplit
from protea.core.ontology.order_encoder import OrderEncoder, TrainConfig
from protea.core.ontology.training import fit

__all__ = ["Dag", "EdgeSplit", "OrderEncoder", "TrainConfig", "fit"]
