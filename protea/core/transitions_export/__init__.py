"""The D56 transitions dataset: ten tables a leak-free loader can read.

Separate from ``protea.core.training_dump``, which writes the D45 pair table
and keeps writing it. Two datasets, two packages: mixing them would make one
``dataset`` row unable to say which it is, which is why the names here carry a
``transitions-`` prefix.
"""
