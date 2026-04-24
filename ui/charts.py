"""Shared Plotly styling for a calm, consistent chart design."""

from __future__ import annotations

import plotly.graph_objects as go

CHART_COLORS = {
    "primary": "#2563eb",
    "secondary": "#60a5fa",
    "positive": "#22c55e",
    "warning": "#f59e0b",
    "negative": "#ef4444",
    "muted": "#94a3b8",
    "grid": "#1e293b",
    "bg": "#0f172a",
}


def apply_consistent_layout(fig: go.Figure, *, height: int, top_margin: int = 30, show_legend: bool = True) -> go.Figure:
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=CHART_COLORS["bg"],
        plot_bgcolor=CHART_COLORS["bg"],
        margin=dict(l=0, r=0, t=top_margin, b=0),
        height=height,
        showlegend=show_legend,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=1.12,
            font=dict(size=9, color=CHART_COLORS["muted"]),
        ),
        xaxis=dict(gridcolor=CHART_COLORS["grid"], tickfont=dict(size=9, color="#64748b")),
        yaxis=dict(gridcolor=CHART_COLORS["grid"], tickfont=dict(size=9, color="#64748b")),
        hovermode="x unified",
    )
    return fig
