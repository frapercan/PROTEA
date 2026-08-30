"""The full measurement: 53,680 pairs, reached and missed, one statistic.

Positive and negative arm of the same test, computed pair by pair rather than
inferred from medians. For every (query, term) whose term exists in the bank
under the donor policy, the rank of the nearest carrier in the query's
embedding space, and how that compares with chance given the term's rarity.

Chance, for c carriers in a pool of N, puts the nearest at about N/(c+1). The
ratio of that to the observed rank is the signal: near 1 means the space knows
nothing about the pair.
"""
import csv
from collections import defaultdict
import numpy as np

BASE = "/home/bioxaxi2/Thesis-laptop/PROTEA/data/ref_cache/"
STEM = "e2__ab430e07-5586-5bdc-9b7e-cc2a3ca18781__cbb35a32-44e4-4e39-b524-05b4b7433727__donor-fde1cbf3642d"
acc = np.load(BASE + STEM + "_accessions.npy", allow_pickle=True)
pool = np.load(BASE + STEM + "_embeddings.npy").astype(np.float32)
pool /= np.linalg.norm(pool, axis=1, keepdims=True) + 1e-12
idx = {a: i for i, a in enumerate(acc)}
N, D = pool.shape
print(f"  pool {N:,} x {D}")

carriers = defaultdict(list)
for t, p in csv.reader(open("/tmp/allcarriers.csv")):
    j = idx.get(p)
    if j is not None:
        carriers[t].append(j)
carriers = {t: np.array(v, dtype=np.int32) for t, v in carriers.items()}
print(f"  terminos con portador en el pool: {len(carriers):,}")

qvec, qname = [], []
for row in csv.reader(open("/tmp/allqemb.csv")):
    if len(row) < 2: continue
    v = np.fromstring(row[1].strip("[]"), sep=",", dtype=np.float32)
    if v.size == D:
        qvec.append(v / (np.linalg.norm(v) + 1e-12)); qname.append(row[0])
Q = np.array(qvec, dtype=np.float32)
qidx = {a: i for i, a in enumerate(qname)}
print(f"  consultas con embedding: {len(qname):,}")

by_query = defaultdict(list)
for p, g, reached in csv.reader(open("/tmp/allpairs.csv")):
    if p in qidx and g in carriers:
        by_query[p].append((g, int(reached)))

res = {0: [], 1: []}
names = [q for q in by_query if q in qidx]
CH = 256
for s in range(0, len(names), CH):
    block = names[s:s + CH]
    sims = Q[[qidx[q] for q in block]] @ pool.T          # (b, N)
    for bi, q in enumerate(block):
        row = sims[bi]
        for term, reached in by_query[q]:
            ci = carriers[term]
            rank = int((row > row[ci].max()).sum()) + 1
            chance = N / (len(ci) + 1.0)
            res[reached].append((rank, len(ci), chance / rank))
    if s % 2560 == 0:
        print(f"    {s + len(block):,} / {len(names):,} consultas")

print(f"\n  {'':13s} {'n':>7s} {'portadores':>11s} {'rango p50':>10s} {'azar p50':>9s} "
      f"{'mejor que azar':>15s} {'<=1x':>6s}")
for k, tag in ((1, "ALCANZADA"), (0, "NO ALCANZ.")):
    a = np.array(res[k], dtype=float)
    print(f"  {tag:13s} {len(a):7,d} {np.median(a[:,1]):11,.0f} {np.median(a[:,0]):10,.0f} "
          f"{np.median(a[:,2]*a[:,0]):9,.0f} {np.median(a[:,2]):14.2f}x {(a[:,2]<=1).mean()*100:5.0f}%")
