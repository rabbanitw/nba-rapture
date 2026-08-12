"""Generate the NAS write-up (LaTeX -> PDF) from the result JSONs.

Inputs (all in training/):
  RESULTS_nas_direct.json         direct-rating NAS winners + test metrics
  RESULTS_nas_pairwise.json       pairwise NAS round 1 (offense)
  RESULTS_nas_pairwise2.json      pairwise NAS round 2 (both targets, solo+ens)
  RESULTS_nas_report_data.json    system metrics on shared pools + leaderboards

Output: training/report_nas/nas_report.{tex,pdf}

Run:  python training/gen_nas_report.py
"""

import json
import re
import subprocess
from pathlib import Path

from db import REPO_ROOT

TD = REPO_ROOT / "training"
OUT = TD / "report_nas"
OUT.mkdir(exist_ok=True)

SYS_LABEL = {"gbm-direct": "GBM direct blend",
             "gbm-pairwise": "GBM pairwise (48k)",
             "nas-direct": "NAS direct net",
             "nas-pairwise": "NAS pairwise ens.",
             "paine-published": "Paine eRT (publ.)",
             "paine-recreated": "Paine eRT (recr.)"}
PAIR_SYS = {"gbm-pairwise", "nas-pairwise"}   # tournament scale: no MAE


def esc(s):
    s = str(s)
    for a, b in [("&", r"\&"), ("%", r"\%"), ("#", r"\#"), ("_", r"\_"),
                 ("$", r"\$")]:
        s = s.replace(a, b)
    return s


def short(name):
    parts = str(name).split()
    if len(parts) >= 2:
        return esc(parts[0][0] + ". " + " ".join(parts[1:]))
    return esc(name)


def cfg_rows(cfg):
    order = ["family", "width", "depth", "hidden", "res", "act", "norm",
             "dropout", "lr", "wd", "batch", "sched", "emb", "bneck"]
    rows = []
    for k in order:
        if k in cfg:
            v = cfg[k]
            if k == "lr":
                v = f"{v:.2e}"
            rows.append(f"{esc(k)} & {esc(v)} \\\\")
    return "\n".join(rows)


def metric_table(block, systems, caption, label, mae=True):
    cols = "lrrrrrr" + ("r" if mae else "")
    head = ("system & dev@10 & dev@20 & $\\tau$@10 & $\\tau$@20 & "
            "hits@10 & hits@20" + (" & MAE" if mae else "")) + " \\\\"
    lines = [f"\\begin{{table}}[ht]\\centering\\small",
             f"\\caption{{{caption}}}\\label{{{label}}}",
             f"\\begin{{tabular}}{{{cols}}}", "\\toprule", head, "\\midrule"]
    for s in systems:
        if s not in block:
            continue
        m = block[s]
        row = (f"{SYS_LABEL.get(s, s)} & {m['dev@10']:.2f} & {m['dev@20']:.2f}"
               f" & {m['tau@10']:+.3f} & {m['tau@20']:+.3f}"
               f" & {m['hits@10']}/20 & {m['hits@20']}/40")
        if mae:
            row += " & --" if s in PAIR_SYS else f" & {m['mae']:.3f}"
        lines.append(row + " \\\\")
    lines += ["\\bottomrule", "\\end{tabular}", "\\end{table}"]
    return "\n".join(lines)


def board_table(board, key, caption):
    systems = ["gbm-direct", "gbm-pairwise", "nas-direct", "nas-pairwise"]
    if "paine-published" in board:
        systems.append("paine-published")
    heads = " & ".join(SYS_LABEL[s].replace(" (48k)", "").replace(" (publ.)", "")
                       for s in systems)
    lines = ["\\begin{table}[ht]\\centering\\scriptsize",
             f"\\caption{{{caption}}}",
             "\\begin{tabular}{rl" + "l" * len(systems) + "}", "\\toprule",
             f"\\# & true (score) & {heads} \\\\", "\\midrule"]
    for k in range(15):
        t = board["true"][k]
        cells = []
        for s in systems:
            e = board[s][k]
            cells.append(f"{short(e['player'])} ({e['true_rank']})")
        lines.append(f"{t['pos']} & {short(t['player'])} ({t['score']:+.1f}) & "
                     + " & ".join(cells) + " \\\\")
    lines += ["\\bottomrule", "\\end{tabular}", "\\end{table}"]
    return "\n".join(lines)


def main():
    nd = json.loads((TD / "RESULTS_nas_direct.json").read_text())
    p1 = json.loads((TD / "RESULTS_nas_pairwise.json").read_text())
    p2 = json.loads((TD / "RESULTS_nas_pairwise2.json").read_text())
    rd = json.loads((TD / "RESULTS_nas_report_data.json").read_text())

    tex = []
    tex.append(r"""\documentclass[10pt]{article}
\usepackage[margin=1.9cm]{geometry}
\usepackage[T1]{fontenc}
\usepackage[utf8]{inputenc}
\usepackage{booktabs,amsmath}
\usepackage{lmodern}
\usepackage{hyperref}
\title{Neural Architecture Search for RAPTOR Re-estimation:\\
Direct-Rating and Player-vs-Player Ranking Models}
\author{nba-rapture}
\date{\today}
\begin{document}
\maketitle

\begin{abstract}
We run two neural-architecture-search campaigns against the strongest
gradient-boosted baselines in this project: (i) direct regression of
per-player-season offensive and defensive RAPTOR, and (ii) a player-vs-player
(pairwise) ranking formulation in which the model only predicts which of two
players has the higher rating. Search spaces cover four regression families
(plain MLP, residual MLP, self-normalizing SELU networks, bottleneck
compressors) and three pairwise families (Bradley--Terry scorer, difference
network, two-tower interaction encoder), with successive-halving over
depth, width, activation, normalization, dropout, learning rate, weight decay,
batch size, and schedule. Selection uses held-out whole-season validation
cells; the 2013-14 and 2014-15 test cells are scored exactly once per final
model. We report top-of-leaderboard metrics (dev@10, dev@20, Kendall
$\tau$@k, hits@k), side-by-side projected-vs-true leaderboards including an
in-sample training season, and comparisons against Neil Paine's Estimated
RAPTOR linear model.
\end{abstract}

\section{Task, data, and splits}
\textbf{Labels.} FiveThirtyEight full-history RAPTOR, separate offense
(\texttt{raptor\_offense}) and defense (\texttt{raptor\_defense}) per player
per season. \textbf{Features.} The project's scraped matrix: pbpstats
totals/on-off, WOWY and opponent-WOWY splits, NBA tracking tables, and
\texttt{leaguedashptdefend} nearest-defender data; after rate normalization,
on-minus-off differentials, cell-relative standardization (12 features), and
engineered opponent/defend blocks, offense rows carry 1{,}173 columns and
defense rows 1{,}161. Identifier columns are excluded.

\textbf{Splits} (identical for every system in this report):
\begin{itemize}\itemsep0pt
\item \emph{Train}: 13{,}021 regular-season rows ($\geq$50 minutes) from
  labeled cells spanning 2015-16 through 2022-23, in-season snapshots
  included, excluding the four validation cells below.
\item \emph{Validation}: four whole-season cells (2015-16, 2016-17, 2017-18,
  2019-20) used for NAS selection and early stopping only.
\item \emph{Test}: the 2013-14 and 2014-15 regular-season cells (494 rows),
  never touched during search; each final model is scored on them once.
\end{itemize}
\textbf{Metrics.} dev@$k$ = mean absolute rank error of the projected top-$k$
(how far the player projected at position $i\le k$ truly ranks from $i$);
$\tau$@$k$ = Kendall tau between projected and true order of the true
top-$k$; hits@$k$ = overlap of projected and true top-$k$ sets, summed over
both test cells; MAE in RAPTOR points (undefined for tournament-scale
pairwise systems).
""")

    # ---- search methodology ------------------------------------------------
    tex.append(r"""\section{Search methodology}
\textbf{Direct-rating NAS} samples 24 configurations from: family $\in$
\{mlp, resmlp, snn, bottleneck\}; width $\in$ \{128, 256, 512, 1024\}; depth
1--4 (2--6 for resmlp); activation \{ReLU, GELU, SiLU\}; normalization
\{batch, layer, none\}; dropout \{0, 0.1, 0.2, 0.35\}; lr
$\log\mathcal{U}(10^{-3.7},10^{-2.3})$; weight decay \{0, $10^{-5}$,
$10^{-4}$, $10^{-3}$\}; batch \{256, 512, 1024\}; cosine schedule on/off.
SNN ignores the sampled activation/norm and uses SELU with LeCun-normal
initialization and alpha-dropout. Loss is smooth-L1 weighted by
$\sqrt{\text{minutes}}$. Successive halving: $24 \times 30$ epochs
$\rightarrow$ top $8\times{+}60 \rightarrow$ top $3\times{+}120$; selection on
mean validation-cell Spearman; the winner is retrained from scratch with
three seeds and seed-averaged.

\textbf{Pairwise NAS (round 2)} samples 20 configurations from families
\{scorer $s(a)-s(b)$, antisymmetrized difference net $f(a-b)$, two-tower
$g([e_a-e_b,\,e_a e_b])$\}, each with optional residual trunk, plus the same
hyperparameter axes and an embedding size \{32, 64, 128\} for the two-tower.
Training streams 200k fresh within-cell ordered pairs per epoch (ties
$|\Delta y|<0.05$ dropped), BCE on the pair logit. Halving: $20\times 8$
epochs $\rightarrow 6\times{+}16 \rightarrow 3\times{+}24$; selection on
validation-cell pairwise accuracy. Inference is a full round-robin tournament
per cell with antisymmetrized win probabilities; we evaluate both the solo
winner and the ensemble of the three best architectures (round 1 fielded
only solo models while the GBM baseline enjoys seed averaging). Round 1
(20 configs, no residual trunks, no schedules, offense only) is included for
reference.
""")

    # ---- winners -----------------------------------------------------------
    tex.append(r"\section{Winning architectures}")
    tex.append(r"\begin{table}[ht]\centering\small"
               r"\caption{NAS winners and their full hyperparameters. The SNN "
               r"family overrides act/norm with SELU/none by construction.}"
               r"\begin{tabular}{lllll}\toprule")
    heads, bodies = [], {}
    combos = [("direct offense", nd["offense"]["winner"]),
              ("direct defense", nd["defense"]["winner"]),
              ("pairwise offense", p2["offense"]["winner"]),
              ("pairwise defense", p2["defense"]["winner"])]
    keys = ["family", "res", "width", "depth", "hidden", "act", "norm",
            "dropout", "lr", "wd", "batch", "sched", "emb", "bneck"]
    tex.append("param & " + " & ".join(esc(c) for c, _ in combos) + r" \\")
    tex.append(r"\midrule")
    for k in keys:
        if not any(k in cfg for _, cfg in combos):
            continue
        vals = []
        for _, cfg in combos:
            v = cfg.get(k, "--")
            if k == "lr" and v != "--":
                v = f"{v:.2e}"
            vals.append(esc(v))
        tex.append(f"{esc(k)} & " + " & ".join(vals) + r" \\")
    tex.append(f"val score & {nd['offense']['val_rho']:+.4f} & "
               f"{nd['defense']['val_rho']:+.4f} & "
               f"{p2['offense']['val_acc']:.4f} & "
               f"{p2['defense']['val_acc']:.4f} \\\\")
    tex.append(r"\bottomrule\end{tabular}\end{table}")
    tex.append(
        "The pairwise ensembles average the three best architectures per "
        "target: offense "
        + esc(", ".join(f"{t['cfg']['family']}({t['acc']:.4f})"
                        for t in p2["offense"]["top3"]))
        + "; defense "
        + esc(", ".join(f"{t['cfg']['family']}({t['acc']:.4f})"
                        for t in p2["defense"]["top3"]))
        + ". Pairwise round 1 winner (offense): scorer, hidden "
        + esc(tuple(p1["winner_cfg"]["hidden"]))
        + f", {p1['winner_cfg']['act']}, norm={p1['winner_cfg']['norm']}, "
        + f"dropout {p1['winner_cfg']['dropout']}, "
        + f"lr {p1['winner_cfg']['lr']:.2e}, wd {p1['winner_cfg']['wd']}, "
        + f"batch {p1['winner_cfg']['batch']}; "
        + f"val acc {p1['val_acc']:.4f}, test dev@10 "
        + f"{p1['test']['dev@10']:.2f}.")

    # ---- results -----------------------------------------------------------
    tex.append(r"\section{Results}")
    for target in ("offense", "defense"):
        tex.append(metric_table(
            rd["metrics_full"][target],
            ["gbm-direct", "gbm-pairwise", "nas-direct", "nas-pairwise"],
            f"{target.capitalize()}, full test cells (2013-14 + 2014-15 RS, "
            "494 rows). GBM systems are the project baselines; NAS systems "
            "are this report's subject.",
            f"tab:full-{target}"))
    tex.append(
        "For production context (different architecture, same test cells): "
        "the deployed offense stack (components + opponent features) verifies "
        "at dev@10 1.10, $\\tau$@10 +0.800; the deployed defense stack "
        "(matched features + defend) at dev@10 3.80, $\\tau$@10 +0.511. "
        "Ten-fold leave-one-season-out medians: offense 1.50, defense 5.00, "
        "pairwise GBM 1.60.")

    tex.append(r"\subsection{Against Neil Paine's Estimated RAPTOR}")
    tex.append(
        r"\textbf{Caveat favoring Paine:} his published weights were fit on "
        r"full RAPTOR 2014--2023, which \emph{contains both of our test "
        r"seasons} -- his numbers are in-sample where ours are strictly "
        r"out-of-sample. Pools below are the $\geq$1065-minute test rows "
        r"matched into his file by normalized name "
        f"(offense n={rd['pools']['offense']['paine_matched']}, "
        f"defense n={rd['pools']['defense']['paine_matched']}).")
    for target in ("offense", "defense"):
        tex.append(metric_table(
            rd["metrics_paine"][target],
            ["gbm-direct", "gbm-pairwise", "nas-direct", "nas-pairwise",
             "paine-published", "paine-recreated"],
            f"{target.capitalize()}, Paine-matched $\\geq$1065-minute pool.",
            f"tab:paine-{target}"))

    tex.append(r"\subsection{In-sample fit (training cell)}")
    tex.append(
        "The 2021-22 regular-season cell was part of every system's training "
        "data; this table shows memorization capacity, not skill, and is "
        "included because projected-vs-true boards on a training season "
        "(next section) are only interpretable alongside it.")
    for target in ("offense", "defense"):
        tex.append(metric_table(
            rd["metrics_train"][target],
            ["gbm-direct", "gbm-pairwise", "nas-direct", "nas-pairwise"],
            f"{target.capitalize()}, 2021-22 RS, in-sample.",
            f"tab:train-{target}"))

    # ---- boards ------------------------------------------------------------
    tex.append(r"\section{Leaderboards: projected vs.\ true}")
    tex.append(
        "Each cell shows the player a system projects at that position, with "
        "his true rank in parentheses; pools are the $\\geq$1065-minute "
        "players of the cell. 2013-14 and 2014-15 are out-of-sample for our "
        "systems (in-sample for Paine); 2021-22 is in-sample for our systems "
        "and shown to make training-fit visible.")
    for key, cap in [
            ("offense|2013-14|test", "Offense 2013-14 (test)"),
            ("offense|2014-15|test", "Offense 2014-15 (test)"),
            ("defense|2013-14|test", "Defense 2013-14 (test)"),
            ("defense|2014-15|test", "Defense 2014-15 (test)"),
            ("offense|2021-22|train-insample",
             "Offense 2021-22 (training season, in-sample)"),
            ("defense|2021-22|train-insample",
             "Defense 2021-22 (training season, in-sample)")]:
        tex.append(board_table(rd["boards"][key], key, cap))
        tex.append(r"\clearpage" if "2014-15" in key and "defense" in key
                   else "")

    # ---- discussion --------------------------------------------------------
    o_ens = p2["offense"]["test"]["top3-ens"]
    d_ens = p2["defense"]["test"]["top3-ens"]
    o_solo = p2["offense"]["test"]["solo"]
    d_solo = p2["defense"]["test"]["solo"]
    mt = rd["metrics_train"]
    tex.append(r"\section{Discussion}" + f"""
\\begin{{itemize}}\\itemsep2pt
\\item \\textbf{{Round 2 genuinely improved the neural pairwise model.}}
Adding difference-network depth, LayerNorm, and cosine schedules to the
search moved the best neural pairwise result from dev@10 3.60 (round 1) to
{o_solo['dev@10']:.2f} on offense, and from 18.1 (the original all-pairs
RankNet) to {d_solo['dev@10']:.2f} on defense. Notably, the same
configuration won both targets under independent selection, and its family
is the free-form difference net $f(x_a{{-}}x_b)$ -- the GBM's own
formulation -- over the Bradley--Terry scorer that round 1's halving kept.
\\item \\textbf{{But the GBM gap remains.}} The best neural systems reach
{o_solo['dev@10']:.2f}/{d_solo['dev@10']:.2f} (offense/defense) against
1.55/3.65 for GBMs consuming identical features and 1.10/3.80 for the
production stacks. Across 108 trained configurations in five campaigns, no
searched region closes it.
\\item \\textbf{{The gap is generalization, not capacity.}} In-sample on the
2021-22 training cell the nets essentially memorize -- direct net dev@10
{mt['offense']['nas-direct']['dev@10']:.2f} offense,
{mt['defense']['nas-direct']['dev@10']:.2f} defense (tau@10
{mt['defense']['nas-direct']['tau@10']:+.2f}) -- while the GBMs fit training
far more loosely ({mt['offense']['gbm-direct']['dev@10']:.2f}/{mt['defense']['gbm-direct']['dev@10']:.2f})
yet generalize better. On 13k rows $\\times$ 1.1k columns the trees'
inductive bias, not model capacity, is what transfers.
\\item \\textbf{{Ensembling did not help.}} Averaging the top-3 architectures
moved offense from {o_solo['dev@10']:.2f} to {o_ens['dev@10']:.2f} and
defense from {d_solo['dev@10']:.2f} to {d_ens['dev@10']:.2f}: diverse
architectures at a shared plateau average toward the plateau, unlike GBM
seed-averaging, which reduces the variance of one stronger learner.
\\item \\textbf{{Defense stays qualitatively harder for networks}} (best net
{d_solo['dev@10']:.2f} vs.\\ 3.65--3.80 GBM), consistent with the
elite-defender study: defensive elite ordering rides on a few engineered
nearest-defender columns inside a full profile -- a regime trees exploit
and MLPs dilute.
\\item \\textbf{{Against Paine's Estimated RAPTOR}} every system here --
including the neural ones -- clears his published defense numbers (13.40
dev@10), and all GBM systems clear his offense (4.35), despite his weights
being fit on data containing our test seasons.
\\item \\textbf{{Conclusion.}} Gradient boosting remains the production
choice for both formulations. Untested neural territory that plausibly
differs in kind: attention over feature tokens (FT-Transformer) and
prior-fitted models (TabPFN), both wanting GPU-scale compute.
\\end{{itemize}}

\\section*{{Reproduction}}
\\texttt{{python training/nas\\_direct.py}};
\\texttt{{python training/nas\\_pairwise2.py}};
\\texttt{{python training/build\\_nas\\_report\\_data.py}};
\\texttt{{python training/gen\\_nas\\_report.py}}.
""")
    tex.append(r"\end{document}")

    (OUT / "nas_report.tex").write_text("\n".join(tex))
    for _ in range(2):
        r = subprocess.run(["pdflatex", "-interaction=nonstopmode",
                            "nas_report.tex"], cwd=OUT, capture_output=True,
                           text=True)
    ok = (OUT / "nas_report.pdf").exists()
    print("PDF built" if ok else "PDF FAILED", flush=True)
    if not ok:
        print(r.stdout[-3000:])


if __name__ == "__main__":
    main()
