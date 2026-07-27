"""Next-best-product recommender for Prime Capital Bank - "customers who
hold product X also hold product Y", plus a per-customer "recommended
next" list, built the same way as the shop-assistant recommender on the
Pet Business Intelligence project: item-based collaborative filtering
over a sparse customer x product ownership matrix.

Ownership here means an open account of that product_name. Evaluated
honestly with a leave-one-product-out test on customers holding 2+
products: hide one product, build recommendations from the rest, check
whether the hidden product lands in the top-3 recommendations.
"""
import json
import os

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.metrics.pairwise import cosine_similarity

SEED = 42
rng = np.random.default_rng(SEED)
TOP_K = 5
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "data")
OUT = os.path.join(BASE, "docs", "ml_outputs")
os.makedirs(OUT, exist_ok=True)


def main():
    accounts = pd.read_csv(os.path.join(DATA, "accounts.csv"))
    accounts = accounts[accounts["status"] == "Active"]
    cust_lookup = pd.read_csv(os.path.join(DATA, "customers.csv"),
                               usecols=["customer_id", "customer_number", "customer_segment"]) \
        .set_index("customer_id")

    customers = sorted(accounts["customer_id"].unique())
    products = sorted(accounts["product_name"].unique())
    cust_idx = {c: i for i, c in enumerate(customers)}
    prod_idx = {p: i for i, p in enumerate(products)}

    rows = accounts["customer_id"].map(cust_idx)
    cols = accounts["product_name"].map(prod_idx)
    data = np.ones(len(accounts))
    mat = sparse.csr_matrix((data, (rows, cols)), shape=(len(customers), len(products)))
    mat.data[:] = 1  # ownership, not count - a second account of the same product isn't a stronger signal

    print(f"Ownership matrix: {len(customers)} customers x {len(products)} products, "
          f"{mat.nnz} holdings ({100 * mat.nnz / (mat.shape[0] * mat.shape[1]):.2f}% dense)")

    item_sim = cosine_similarity(mat.T)
    np.fill_diagonal(item_sim, 0)

    # ---- top-5 "customers who hold X also hold" per product ----
    sim_rows = []
    for p in products:
        pi = prod_idx[p]
        sims = item_sim[pi]
        top = np.argsort(-sims)[:TOP_K]
        for rank, j in enumerate(top, 1):
            if sims[j] <= 0:
                continue
            sim_rows.append({"product": p, "rank": rank, "related_product": products[j],
                              "similarity": round(float(sims[j]), 4)})
    sim_df = pd.DataFrame(sim_rows)
    sim_path = os.path.join(OUT, "product_similarity.csv")
    sim_df.to_csv(sim_path, index=False)
    print(f"product_similarity.csv: {len(sim_df)} rows ({len(products)} products)")

    # ---- leave-one-product-out evaluation ----
    # Important: item_sim must NOT be fit on the interaction being held out,
    # or the test leaks (the held-out product's similarity to the customer's
    # other products would already "know" this customer holds it). With only
    # 989 holdings across 22 products a single interaction measurably moves
    # a similarity column, so item_sim is recomputed per held-out test on a
    # matrix with that one cell masked - the same leak Pet Business's
    # temporal split avoided by construction; here it has to be explicit.
    multi_product = [c for c in customers if mat[cust_idx[c]].nnz >= 2]
    eval_rng = rng
    hits, hits_random = 0, 0
    n_eval = len(multi_product)
    dense_mat = mat.toarray().astype(float)
    for c in multi_product:
        ci = cust_idx[c]
        owned = np.nonzero(dense_mat[ci])[0]
        held_out = eval_rng.choice(owned)
        remaining = [o for o in owned if o != held_out]

        masked = dense_mat.copy()
        masked[ci, held_out] = 0.0
        sim_masked = cosine_similarity(masked.T)
        np.fill_diagonal(sim_masked, 0)

        scores = sim_masked[remaining].sum(axis=0)
        scores[remaining] = -1
        top = np.argsort(-scores)[:3]
        if held_out in top:
            hits += 1
        random_top = eval_rng.choice([i for i in range(len(products)) if i not in remaining],
                                      size=min(3, len(products) - len(remaining)), replace=False)
        if held_out in random_top:
            hits_random += 1

    hit_rate = hits / n_eval if n_eval else 0
    random_rate = hits_random / n_eval if n_eval else 0
    metrics = {
        "model": "Item-based collaborative filtering (cosine similarity on product ownership)",
        "customers_with_accounts": len(customers),
        "products": len(products),
        "holdings": int(mat.nnz),
        "customers_evaluated": n_eval,
        "hit_rate_at_3": round(hit_rate, 4),
        "random_baseline_hit_rate_at_3": round(random_rate, 4),
        "lift_over_random": round(hit_rate / random_rate, 2) if random_rate else None,
    }
    print(json.dumps(metrics, indent=2))

    # ---- per-customer demo recommendations (all customers with 1+ product) ----
    demo = []
    for c in customers:
        ci = cust_idx[c]
        owned = mat[ci].indices
        if len(owned) == 0:
            continue
        scores = item_sim[owned].sum(axis=0)
        scores[owned] = -1
        top = np.argsort(-scores)[:TOP_K]
        top = [t for t in top if scores[t] > 0]
        row = cust_lookup.loc[c] if c in cust_lookup.index else None
        demo.append({
            "customer_id": c,
            "customer_number": row["customer_number"] if row is not None else c[:8],
            "segment": row["customer_segment"] if row is not None else "Unknown",
            "current_products": [products[o] for o in owned],
            "recommended_next": [products[t] for t in top],
        })
    demo.sort(key=lambda d: -len(d["current_products"]))
    demo_path = os.path.join(OUT, "customer_recommendations.json")
    with open(demo_path, "w") as f:
        json.dump(demo, f, indent=2)
    print(f"customer_recommendations.json: {len(demo)} customers")

    metrics_path = os.path.join(OUT, "recommender_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)


if __name__ == "__main__":
    main()
