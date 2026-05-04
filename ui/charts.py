"""Shared Plotly styling for a clean, professional light-mode chart design."""

from __future__ import annotations

import plotly.graph_objects as go

CHART_COLORS = {
    "primary":   "#1d4ed8",
    "secondary": "#60a5fa",
    "positive":  "#15803d",
    "warning":   "#d97706",
    "negative":  "#b91c1c",
    "muted":     "#64748b",
    "grid":      "rgba(100, 116, 139, 0.12)",
    "bg":        "rgba(0, 0, 0, 0)",
}


def apply_consistent_layout(fig: go.Figure, *, height: int, top_margin: int = 28, show_legend: bool = True) -> go.Figure:
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor=CHART_COLORS["bg"],
        plot_bgcolor="rgba(248,250,252,0)",
        margin=dict(l=8, r=8, t=top_margin, b=8),
        height=height,
        showlegend=show_legend,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10, color=CHART_COLORS["muted"]),
            bgcolor="rgba(255,255,255,0)",
            borderwidth=0,
            itemclick="toggle",
            itemdoubleclick="toggleothers",
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="#ffffff",
            bordercolor="rgba(100,116,139,0.30)",
            font=dict(color="#0d1626", size=11),
        ),
        xaxis=dict(
            gridcolor=CHART_COLORS["grid"],
            zeroline=False,
            tickfont=dict(size=10, color=CHART_COLORS["muted"]),
            title_font=dict(size=11, color=CHART_COLORS["muted"]),
            linecolor="rgba(100,116,139,0.20)",
        ),
        yaxis=dict(
            gridcolor=CHART_COLORS["grid"],
            zeroline=False,
            tickfont=dict(size=10, color=CHART_COLORS["muted"]),
            title_font=dict(size=11, color=CHART_COLORS["muted"]),
            linecolor="rgba(100,116,139,0.20)",
        ),
    )
    return fig
