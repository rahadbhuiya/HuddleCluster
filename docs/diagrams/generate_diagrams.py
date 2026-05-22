"""
HuddleCluster — Paper Diagrams
================================
Generates 3 publication-quality diagrams:
  1. architecture_diagram.png  — dual-ring structure
  2. temperature_lifecycle.png — server state machine
  3. rotation_flowchart.png    — rotation algorithm flowchart

Run:
  python generate_diagrams.py
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patches as patches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Arc
import matplotlib.patheffects as pe
import numpy as np

plt.rcParams.update({
    "font.family":     "DejaVu Sans",
    "font.size":       10,
    "axes.titlesize":  12,
    "figure.dpi":      150,
})

BLUE   = "#2E75B6"
GREEN  = "#55A868"
ORANGE = "#DD8452"
RED    = "#C0392B"
GRAY   = "#7F8C8D"
LGRAY  = "#ECF0F1"
WHITE  = "#FFFFFF"
DARK   = "#1C2833"



# 1. Architecture Diagram


def draw_architecture():
    fig, ax = plt.subplots(figsize=(13, 7))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 7)
    ax.axis("off")
    ax.set_facecolor("#FAFBFC")
    fig.patch.set_facecolor("#FAFBFC")

    fig.suptitle(
        "HuddleCluster: Dual-Ring Architecture",
        fontsize=14, fontweight="bold", y=0.97, color=DARK
    )

    #  Outer ring background 
    outer_circle = plt.Circle((6.5, 3.5), 2.9, fill=True,
                               facecolor="#EBF5FB", edgecolor=BLUE,
                               linewidth=2, linestyle="--", zorder=1)
    ax.add_patch(outer_circle)
    ax.text(6.5, 6.55, "Outer Ring  (Resting / Cooling)",
            ha="center", va="center", fontsize=10, color=BLUE,
            fontweight="bold")

    #  Inner ring background 
    inner_circle = plt.Circle((6.5, 3.5), 1.7, fill=True,
                               facecolor="#EAFAF1", edgecolor=GREEN,
                               linewidth=2.5, zorder=2)
    ax.add_patch(inner_circle)
    ax.text(6.5, 5.35, "Inner Ring  (Active / Serving)",
            ha="center", va="center", fontsize=10, color=GREEN,
            fontweight="bold")

    #  Inner ring servers 
    inner_angles = [90, 162, 234, 306, 18]
    inner_ids    = ["S0", "S1", "S2", "S3", "S4"]
    inner_temps  = [0.12, 0.08, 0.31, 0.18, 0.09]
    inner_r      = 1.15

    for angle, sid, temp in zip(inner_angles, inner_ids, inner_temps):
        rad   = np.radians(angle)
        cx    = 6.5 + inner_r * np.cos(rad)
        cy    = 3.5 + inner_r * np.sin(rad)
        color = "#ABEBC6" if temp < 0.2 else "#F9E79F" if temp < 0.4 else "#F1948A"
        box   = FancyBboxPatch((cx - 0.38, cy - 0.28), 0.76, 0.56,
                                boxstyle="round,pad=0.05",
                                facecolor=color, edgecolor=GREEN,
                                linewidth=1.5, zorder=4)
        ax.add_patch(box)
        ax.text(cx, cy + 0.07, sid, ha="center", va="center",
                fontsize=10, fontweight="bold", color=DARK, zorder=5)
        ax.text(cx, cy - 0.13, f"T={temp:.2f}", ha="center", va="center",
                fontsize=7.5, color=GRAY, zorder=5)

    #  Round-robin arrow in inner ring 
    for i in range(len(inner_angles)):
        a1  = np.radians(inner_angles[i])
        a2  = np.radians(inner_angles[(i + 1) % len(inner_angles)])
        r   = 0.62
        x1  = 6.5 + r * np.cos(a1)
        y1  = 3.5 + r * np.sin(a1)
        x2  = 6.5 + r * np.cos(a2)
        y2  = 3.5 + r * np.sin(a2)
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(arrowstyle="-|>", color=GREEN,
                                   lw=1.2, connectionstyle="arc3,rad=0.3"),
                    zorder=3)
    ax.text(6.5, 3.5, "Round\nRobin", ha="center", va="center",
            fontsize=8, color=GREEN, fontweight="bold", zorder=5)

    #  Outer ring servers 
    outer_angles = [45, 315]
    outer_ids    = ["S5", "S5*"]
    outer_temps  = [0.03, 0.0]
    outer_labels = ["Resting\n(cool)", "Ready to\npromote"]
    outer_r      = 2.25

    for angle, sid, temp, lbl in zip(outer_angles, outer_ids, outer_temps, outer_labels):
        rad = np.radians(angle)
        cx  = 6.5 + outer_r * np.cos(rad)
        cy  = 3.5 + outer_r * np.sin(rad)
        box = FancyBboxPatch((cx - 0.45, cy - 0.33), 0.90, 0.66,
                              boxstyle="round,pad=0.05",
                              facecolor="#D6EAF8", edgecolor=BLUE,
                              linewidth=1.5, linestyle="--", zorder=4)
        ax.add_patch(box)
        ax.text(cx, cy + 0.10, sid, ha="center", va="center",
                fontsize=9, fontweight="bold", color=BLUE, zorder=5)
        ax.text(cx, cy - 0.10, f"T={temp:.2f}", ha="center", va="center",
                fontsize=7.5, color=GRAY, zorder=5)
        ax.text(cx, cy - 0.28, lbl, ha="center", va="center",
                fontsize=6.5, color=GRAY, zorder=5, style="italic")

    #  Eviction arrow (inner → outer) 
    ax.annotate("",
        xy=(6.5 + 2.05 * np.cos(np.radians(45)),
            3.5 + 2.05 * np.sin(np.radians(45))),
        xytext=(6.5 + 1.55 * np.cos(np.radians(18)),
                3.5 + 1.55 * np.sin(np.radians(18))),
        arrowprops=dict(arrowstyle="-|>", color=RED, lw=2.0,
                        connectionstyle="arc3,rad=-0.3"),
        zorder=6)
    ax.text(8.35, 5.35, "Eviction\n(overheated)", ha="center",
            fontsize=8, color=RED, fontweight="bold")

    #  Promotion arrow (outer → inner) 
    ax.annotate("",
        xy=(6.5 + 1.55 * np.cos(np.radians(306)),
            3.5 + 1.55 * np.sin(np.radians(306))),
        xytext=(6.5 + 2.05 * np.cos(np.radians(315)),
                3.5 + 2.05 * np.sin(np.radians(315))),
        arrowprops=dict(arrowstyle="-|>", color=GREEN, lw=2.0,
                        connectionstyle="arc3,rad=-0.3"),
        zorder=6)
    ax.text(8.35, 1.55, "Promotion\n(cooled)", ha="center",
            fontsize=8, color=GREEN, fontweight="bold")

    #  Load balancer box (left) 
    lb_box = FancyBboxPatch((0.3, 2.8), 1.8, 1.4,
                             boxstyle="round,pad=0.1",
                             facecolor="#F8F9FA", edgecolor=DARK,
                             linewidth=1.5, zorder=4)
    ax.add_patch(lb_box)
    ax.text(1.2, 3.65, "Load\nBalancer", ha="center", va="center",
            fontsize=9, fontweight="bold", color=DARK, zorder=5)
    ax.text(1.2, 3.25, "get_server()\nrecord_latency()", ha="center",
            va="center", fontsize=6.5, color=GRAY, zorder=5)

    ax.annotate("", xy=(3.55, 3.5), xytext=(2.1, 3.5),
                arrowprops=dict(arrowstyle="-|>", color=DARK, lw=1.5),
                zorder=6)
    ax.text(2.82, 3.65, "route", ha="center", fontsize=8, color=DARK)

    #  Client box 
    cl_box = FancyBboxPatch((0.3, 4.5), 1.8, 1.0,
                             boxstyle="round,pad=0.1",
                             facecolor="#F8F9FA", edgecolor=GRAY,
                             linewidth=1.2, zorder=4)
    ax.add_patch(cl_box)
    ax.text(1.2, 5.0, "Client\nRequests", ha="center", va="center",
            fontsize=9, color=DARK, zorder=5)
    ax.annotate("", xy=(1.2, 4.5), xytext=(1.2, 4.22),
                arrowprops=dict(arrowstyle="-|>", color=GRAY, lw=1.2),
                zorder=6)

    #  Rotation daemon box 
    rd_box = FancyBboxPatch((0.3, 0.5), 1.8, 1.6,
                             boxstyle="round,pad=0.1",
                             facecolor="#FEF9E7", edgecolor=ORANGE,
                             linewidth=1.5, zorder=4)
    ax.add_patch(rd_box)
    ax.text(1.2, 1.55, "Rotation\nDaemon", ha="center", va="center",
            fontsize=9, fontweight="bold", color=ORANGE, zorder=5)
    ax.text(1.2, 1.05, "every 0.3s\ncheck temps\nevict / promote", ha="center",
            va="center", fontsize=6.5, color=GRAY, zorder=5)
    ax.annotate("", xy=(3.55, 2.8), xytext=(2.1, 1.5),
                arrowprops=dict(arrowstyle="-|>", color=ORANGE, lw=1.5,
                                connectionstyle="arc3,rad=-0.2"),
                zorder=6)

    #  Legend 
    legend_items = [
        mpatches.Patch(facecolor="#ABEBC6", edgecolor=GREEN, label="Cool server (T < 0.20)"),
        mpatches.Patch(facecolor="#F9E79F", edgecolor=ORANGE, label="Warm server (0.20 ≤ T < 0.40)"),
        mpatches.Patch(facecolor="#F1948A", edgecolor=RED, label="Hot server (T ≥ 0.40)"),
        mpatches.Patch(facecolor="#D6EAF8", edgecolor=BLUE, label="Resting server (outer ring)"),
    ]
    ax.legend(handles=legend_items, loc="lower left",
              bbox_to_anchor=(0.0, 0.0), fontsize=8, framealpha=0.9)

    fig.tight_layout()
    fig.savefig("architecture_diagram.png", dpi=150, bbox_inches="tight",
                facecolor="#FAFBFC")
    print("  Saved: architecture_diagram.png")
    plt.close()



# 2. Temperature Lifecycle / State Machine


def draw_temperature_lifecycle():
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    fig.suptitle("HuddleCluster: Server Temperature Lifecycle",
                 fontsize=13, fontweight="bold", y=0.98)
    fig.patch.set_facecolor("#FAFBFC")

    #  Left: State Machine 
    ax = axes[0]
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 8)
    ax.axis("off")
    ax.set_facecolor("#FAFBFC")
    ax.set_title("Server State Machine", fontsize=11, fontweight="bold", pad=10)

    def state_box(ax, x, y, w, h, label, sublabel, color, edge):
        box = FancyBboxPatch((x - w/2, y - h/2), w, h,
                              boxstyle="round,pad=0.12",
                              facecolor=color, edgecolor=edge,
                              linewidth=2.0, zorder=3)
        ax.add_patch(box)
        ax.text(x, y + 0.18, label, ha="center", va="center",
                fontsize=10, fontweight="bold", color=DARK, zorder=4)
        ax.text(x, y - 0.22, sublabel, ha="center", va="center",
                fontsize=8, color=GRAY, zorder=4)

    state_box(ax, 5, 6.5, 3.2, 1.0, "INNER (Active)",
              "Serving requests · T rising", "#EAFAF1", GREEN)
    state_box(ax, 5, 3.5, 3.2, 1.0, "OUTER (Resting)",
              "No requests · T cooling via EMA", "#EBF5FB", BLUE)
    state_box(ax, 5, 0.8, 3.2, 0.9, "UNHEALTHY",
              "Immediate eviction", "#FDEDEC", RED)

    # INNER → OUTER
    ax.annotate("", xy=(6.9, 4.05), xytext=(6.9, 5.95),
                arrowprops=dict(arrowstyle="-|>", color=RED, lw=2.0), zorder=5)
    ax.text(8.2, 5.0, "T ≥ 0.55\n(overheated)", ha="center",
            fontsize=8.5, color=RED, fontweight="bold")

    # OUTER → INNER
    ax.annotate("", xy=(3.1, 5.95), xytext=(3.1, 4.05),
                arrowprops=dict(arrowstyle="-|>", color=GREEN, lw=2.0), zorder=5)
    ax.text(1.6, 5.0, "T ≤ 0.30\n+ dwell OK\n(cooled)", ha="center",
            fontsize=8.5, color=GREEN, fontweight="bold")

    # INNER → UNHEALTHY
    ax.annotate("", xy=(5, 1.28), xytext=(5, 2.98),
                arrowprops=dict(arrowstyle="-|>", color=RED, lw=2.0,
                                linestyle="dashed"), zorder=5)
    ax.text(6.9, 2.1, "is_healthy\n= False", ha="center",
            fontsize=8, color=RED)

    # UNHEALTHY → OUTER
    ax.annotate("", xy=(3.5, 3.25), xytext=(4.0, 1.28),
                arrowprops=dict(arrowstyle="-|>", color=BLUE, lw=1.5,
                                connectionstyle="arc3,rad=0.3"), zorder=5)
    ax.text(2.0, 2.3, "moved to\nouter", ha="center", fontsize=8, color=BLUE)

    # Self-loop INNER (EMA rising)
    ax.text(5, 7.4, "EMA: T ← α·raw + (1-α)·T", ha="center",
            fontsize=8, color=GRAY, style="italic")

    #  Right: Temperature Formula Breakdown 
    ax2 = axes[1]
    ax2.set_facecolor("#FAFBFC")
    ax2.set_title("Temperature Composition (α = 0.60)", fontsize=11,
                  fontweight="bold", pad=10)

    components = [
        ("Latency Anomaly\n(relative, median-based)", 0.70, "#55A868"),
        ("Active Connections\n(conn / 1000)", 0.10, "#4C72B0"),
        ("CPU Usage",                         0.10, "#DD8452"),
        ("Memory Usage",                      0.05, "#9B59B6"),
        ("Error Rate",                         0.05, "#E74C3C"),
    ]
    labels  = [c[0] for c in components]
    weights = [c[1] for c in components]
    colors2 = [c[2] for c in components]

    wedges, texts, autotexts = ax2.pie(
        weights, labels=None, colors=colors2,
        autopct=lambda p: f"{p:.0f}%",
        startangle=90, pctdistance=0.75,
        wedgeprops={"edgecolor": "white", "linewidth": 2},
    )
    for at in autotexts:
        at.set_fontsize(9)
        at.set_fontweight("bold")
        at.set_color("white")

    legend_patches = [
        mpatches.Patch(facecolor=c, label=f"{l}  (w={w:.2f})")
        for l, w, c in components
    ]
    ax2.legend(handles=legend_patches, loc="lower center",
               bbox_to_anchor=(0.5, -0.28), fontsize=8.5,
               framealpha=0.9, ncol=1)

    ax2.text(0, -0.08, "raw = Σ wᵢ · scoreᵢ",
             ha="center", va="center", fontsize=9,
             color=DARK, fontweight="bold",
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                       edgecolor=GRAY, alpha=0.8))

    fig.tight_layout(rect=[0, 0.0, 1, 0.96])
    fig.savefig("temperature_lifecycle.png", dpi=150, bbox_inches="tight",
                facecolor="#FAFBFC")
    print("  Saved: temperature_lifecycle.png")
    plt.close()



# 3. Rotation Flowchart


def draw_flowchart():
    fig, ax = plt.subplots(figsize=(9, 13))
    ax.set_xlim(0, 9)
    ax.set_ylim(0, 13)
    ax.axis("off")
    ax.set_facecolor("#FAFBFC")
    fig.patch.set_facecolor("#FAFBFC")
    fig.suptitle("HuddleCluster: Rotation Algorithm Flowchart",
                 fontsize=13, fontweight="bold", y=0.99)

    def box(x, y, w, h, text, color="#F8F9FA", edge=DARK, fontsize=9):
        b = FancyBboxPatch((x - w/2, y - h/2), w, h,
                            boxstyle="round,pad=0.1",
                            facecolor=color, edgecolor=edge,
                            linewidth=1.8, zorder=3)
        ax.add_patch(b)
        ax.text(x, y, text, ha="center", va="center",
                fontsize=fontsize, color=DARK, zorder=4,
                multialignment="center")

    def diamond(x, y, w, h, text, color="#FEF9E7", edge=ORANGE):
        diamond_path = plt.Polygon(
            [[x, y + h/2], [x + w/2, y], [x, y - h/2], [x - w/2, y]],
            closed=True, facecolor=color, edgecolor=edge,
            linewidth=1.8, zorder=3
        )
        ax.add_patch(diamond_path)
        ax.text(x, y, text, ha="center", va="center",
                fontsize=8.5, color=DARK, zorder=4, multialignment="center")

    def arr(x1, y1, x2, y2, label="", color=DARK, rad=0.0):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(
                        arrowstyle="-|>", color=color, lw=1.5,
                        connectionstyle=f"arc3,rad={rad}"),
                    zorder=5)
        if label:
            mx, my = (x1+x2)/2, (y1+y2)/2
            ax.text(mx + 0.15, my, label, fontsize=8, color=color,
                    fontweight="bold", zorder=6)

    #  Blocks 
    box(4.5, 12.3, 3.5, 0.7, "START: Rotation Daemon wakes\n(every rotation_interval sec)",
        color="#D5F5E3", edge=GREEN, fontsize=8.5)

    arr(4.5, 11.95, 4.5, 11.4)

    box(4.5, 11.1, 4.5, 0.55, "Update temperatures for all servers\n(EMA + latency anomaly)",
        color=LGRAY, fontsize=8.5)

    arr(4.5, 10.83, 4.5, 10.25)

    diamond(4.5, 9.85, 4.5, 0.75, "Any inner server\nT ≥ heat_threshold?")

    # YES path
    arr(6.75, 9.85, 7.5, 9.85, "YES", color=RED)
    box(8.1, 9.85, 1.5, 0.6, "Cap evictions\n≤ max(1,|I|/3)", "#FADBD8", RED, fontsize=7.5)
    arr(8.1, 9.55, 8.1, 8.75)
    diamond(8.1, 8.4, 1.5, 0.65, "|I| > min\ninner?", color="#FADBD8", edge=RED)
    arr(8.1, 8.07, 8.1, 7.35, "YES", color=RED)
    box(8.1, 7.05, 1.5, 0.55, "Move server\ninner→outer", "#FADBD8", RED, fontsize=7.5)
    arr(7.35, 7.05, 5.85, 7.05)

    # NO path from diamond
    arr(4.5, 9.47, 4.5, 8.85, "NO", color=GRAY)

    box(4.5, 8.55, 4.5, 0.55, "Log rotation event, update timestamps",
        color=LGRAY, fontsize=8.5)

    arr(4.5, 8.28, 4.5, 7.65)

    diamond(4.5, 7.28, 4.5, 0.7, "Outer server exists\nAND coolest T ≤ cool_threshold\nAND dwell OK?")

    # YES promote
    arr(4.5, 6.93, 4.5, 6.25, "YES", color=GREEN)
    box(4.5, 5.95, 4.2, 0.55, "Pop from outer heap\nMove server outer→inner", "#D5F5E3", GREEN, fontsize=8.5)

    arr(4.5, 5.67, 4.5, 5.05)

    diamond(4.5, 4.7, 4.5, 0.65, "Any inner server\nis_healthy = False?")

    # YES health evict
    arr(6.75, 4.7, 7.8, 4.7, "YES", color=RED)
    box(8.0, 4.7, 1.7, 0.55, "Health evict\n→ outer", "#FADBD8", RED, fontsize=7.5)
    arr(8.0, 4.42, 8.0, 3.75)
    ax.text(8.0, 3.55, "done", ha="center", fontsize=8, color=GRAY)

    # NO from promote
    arr(2.25, 7.28, 1.2, 7.28, "NO", color=GRAY, rad=0.0)
    arr(1.2, 7.28, 1.2, 4.7)
    arr(1.2, 4.7, 2.25, 4.7)

    # NO from health
    arr(4.5, 4.37, 4.5, 3.75)

    diamond(4.5, 3.4, 4.0, 0.65, "|I| < min_inner?")

    # YES emergency
    arr(6.5, 3.4, 7.5, 3.4, "YES", color=ORANGE)
    box(8.1, 3.4, 1.5, 0.55, "Emergency:\npull any server", "#FEF9E7", ORANGE, fontsize=7.5)
    arr(8.1, 3.12, 8.1, 2.45)

    # NO from min_inner
    arr(4.5, 3.07, 4.5, 2.45)

    box(4.5, 2.15, 4.5, 0.55, "Sleep rotation_interval seconds",
        color=LGRAY, fontsize=8.5)

    arr(4.5, 1.87, 4.5, 1.3)

    box(4.5, 1.0, 3.5, 0.55, "RETURN to START",
        color="#D5F5E3", edge=GREEN, fontsize=8.5)

    #  Annotations 
    ax.text(0.25, 9.85, "Fix: Thundering\nHerd Prevention",
            ha="left", fontsize=7.5, color=ORANGE, style="italic")
    ax.text(0.25, 7.28, "Fix: Flapping\nPrevention\n(dwell check)",
            ha="left", fontsize=7.5, color=ORANGE, style="italic")

    fig.tight_layout()
    fig.savefig("rotation_flowchart.png", dpi=150, bbox_inches="tight",
                facecolor="#FAFBFC")
    print(" Saved: rotation_flowchart.png")
    plt.close()



# Main


if __name__ == "__main__":
    print("  Generating paper diagrams...")
    draw_architecture()
    draw_temperature_lifecycle()
    draw_flowchart()
    print("\n  All diagrams saved:")
    print("     architecture_diagram.png")
    print("     temperature_lifecycle.png")
    print("     rotation_flowchart.png")
